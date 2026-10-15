"""Actual adapter/reader processes and durable incremental state; no graph calls."""

from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

import pytest

import scheduled_catalogue_adapter as adapter
from emitter import Emitter, connect
from incremental import ChangeRunner
from test_scheduled_projection import catalogue, projection as projection_fixture


@pytest.fixture
def setup(tmp_path):
    projection = projection_fixture.__wrapped__()
    report, counter = tmp_path / "catalogue.json", tmp_path / "reader-calls"
    report.write_text(json.dumps(catalogue(projection)))
    reader = tmp_path / "st-reader-fixture"
    reader.write_text(
        "#!"
        + sys.executable
        + "\n"
        + """import json,os,sys,time
from pathlib import Path
assert 'ADAPTER_TEST_SENTINEL' not in os.environ
assert sys.argv[1:]==['--root',os.environ['FIXTURE_ROOT'],'work','jobs','--graph',
 'urn:example:catalogue','--expect-job','urn:example:job','--json','list']
p=Path(os.environ['FIXTURE_COUNTER']);p.write_text(str(int(p.read_text())+1) if p.exists() else '1')
mode=os.environ.get('FIXTURE_MODE','ok')
if mode=='sleep':time.sleep(10)
if mode=='fail':print('SYNTHETIC SECRET ERROR',file=sys.stderr);sys.exit(2)
if mode=='large':os.write(1,b'x'*10000)
else:sys.stdout.buffer.write(Path(os.environ['FIXTURE_REPORT']).read_bytes())
"""
    )
    reader.chmod(0o700)
    config = dict(
        version=1,
        projection=asdict(projection),
        reader=dict(
            executable=str(reader),
            root=str(tmp_path),
            env=dict(FIXTURE_ROOT=str(tmp_path), FIXTURE_COUNTER=str(counter), FIXTURE_REPORT=str(report)),
            timeout_s=2,
            output_limit=1048576,
        ),
    )
    config_file = tmp_path / "adapter.json"

    def command():
        config_file.write_text(json.dumps(config))
        config_file.chmod(0o600)
        return [
            sys.executable,
            str(Path(adapter.__file__)),
            "--config",
            str(config_file),
            "--config-sha256",
            hashlib.sha256(config_file.read_bytes()).hexdigest(),
        ]

    def run(request=None):
        argv = command() + (["--describe"] if request is None else ["--changes"])
        raw = json.dumps(request).encode() if not isinstance(request, bytes) else request
        return subprocess.run(argv, input=raw if request is not None else None, capture_output=True, timeout=5)

    return projection, config, config_file, report, counter, command, run


def count(counter):
    return int(counter.read_text()) if counter.exists() else 0


def test_describe_and_static_route_do_not_invoke_reader(setup, monkeypatch):
    p, _, _, _, counter, _, run = setup
    monkeypatch.setenv("ADAPTER_TEST_SENTINEL", "synthetic-not-a-credential")
    r = run()
    assert r.returncode == 0
    assert json.loads(r.stdout)["graphs"] == [p.graph] and count(counter) == 0
    r = run(dict(now=1801, route=[dict(graph=p.graph, attribute=adapter.SCHEMA + "name", entity="urn:example:any")]))
    assert r.returncode == 0 and json.loads(r.stdout)["items"] == [p.scope.job] and count(counter) == 0
    r = run(dict(now=1801, discover=True))
    assert r.returncode == 0 and count(counter) == 1
    r = run(dict(now=1801, items=[p.scope.job], changes=[]))
    assert r.returncode == 0 and count(counter) == 2
    out = json.loads(r.stdout)
    assert out["records"][0]["verdict"] == "UNBLOCKED"
    assert out["next_checks"] == {p.scope.job: 2400}


@pytest.mark.parametrize(
    "payload",
    [
        {},
        dict(now=True, discover=True),
        dict(now=1801, discover=False),
        dict(now=1801, items=["urn:example:foreign"], changes=[]),
        dict(now=1801, items=["urn:example:job"], changes=[], command=["anything"]),
        dict(now=1801, route=[]),
        dict(now=1801, route=[dict(graph="urn:foreign", attribute="name", entity="x")]),
        b'{"now":1801,"now":1802,"discover":true}',
        b'{"now":NaN,"discover":true}',
        (
            b'{"now":1801,"route":[{"graph":"urn:example:catalogue",'
            b'"attribute":"https://schema.org/name","entity":"urn:example:any","value":1e999}]}'
        ),
        b"x" * 65537,
    ],
)
def test_malformed_or_foreign_request_refuses_without_reader(setup, payload):
    *_, counter, _, run = setup
    r = run(payload)
    assert r.returncode == 2 and not r.stdout and count(counter) == 0


@pytest.mark.parametrize("mode", ["fail", "sleep", "large"])
def test_reader_failure_timeout_and_output_bound_never_emit_catalogue(setup, mode):
    p, config, _, _, counter, _, run = setup
    config["reader"]["env"]["FIXTURE_MODE"] = mode
    if mode == "sleep":
        config["reader"]["timeout_s"] = 0.1
    if mode == "large":
        config["reader"]["output_limit"] = 1024
    started = time.monotonic()
    r = run(dict(now=1801, discover=True))
    assert r.returncode == 2 and not r.stdout and count(counter) == 1
    assert b"SYNTHETIC SECRET" not in r.stderr and time.monotonic() - started < 3


@pytest.mark.parametrize(
    "body", [b"{}", b"[]", b"not-json", b'{"schema_version":1,"schema_version":1}', b'{"schema_version":NaN}']
)
def test_malformed_reader_result_is_unknown(setup, body):
    _, _, _, report, counter, _, run = setup
    report.write_bytes(body)
    r = run(dict(now=1801, discover=True))
    assert r.returncode == 2 and not r.stdout and count(counter) == 1


@pytest.mark.parametrize("change", ["changed", "writable", "symlink", "fifo"])
def test_pinned_config_custody_refuses_before_reader(setup, tmp_path, change):
    import os

    _, _, config_file, _, counter, command, _ = setup
    argv = command() + ["--describe"]
    if change == "changed":
        config_file.write_text("{}")
    elif change == "writable":
        config_file.chmod(0o666)
    else:
        config_file.unlink()
        if change == "symlink":
            config_file.symlink_to(tmp_path / "missing")
        else:
            os.mkfifo(config_file)
    r = subprocess.run(argv, capture_output=True, timeout=3)
    assert r.returncode == 2 and not r.stdout and count(counter) == 0


@pytest.mark.parametrize("key,value", [("version", True), ("projection", {}), ("reader", {}), ("extra", 1)])
def test_unsupported_configuration_is_unknown(setup, key, value):
    _, config, _, _, counter, _, run = setup
    config[key] = value
    r = run()
    assert r.returncode == 2 and not r.stdout and count(counter) == 0


def test_actual_incremental_idle_deadline_and_missing_identity_retention(setup, tmp_path):
    p, _, _, report, counter, command, _ = setup
    definition = Emitter(
        "fixture", command(), 3600, p.owner, event="scheduled", baseline_emits=True, change_driven=True
    )
    conn = connect(tmp_path / "outbox.sqlite")

    class PendingSink:
        def delivery_scope(self):
            return {"fixture": "unacknowledged"}

        def deliver(self, *_):
            raise ConnectionError("synthetic pending receipt")

    runner = ChangeRunner(definition, conn, PendingSink())
    try:
        assert count(counter) == 0
        runner.tick(1801)
        assert count(counter) == 2 and conn.execute("select count(*) from outbox").fetchone() == (1,)
        runner.tick(1802)
        runner.tick(2399)
        assert count(counter) == 2
        runner.tick(2400)
        assert count(counter) == 3 and conn.execute("select count(*) from outbox").fetchone() == (2,)
        before = conn.execute("select * from verdicts").fetchall()
        broken = catalogue(p)
        broken["jobs"] = []
        report.write_text(json.dumps(broken))
        runner.tick(3000)
        assert count(counter) == 4 and conn.execute("select count(*) from outbox").fetchone() == (2,)
        assert conn.execute("select * from verdicts").fetchall() == before
        assert conn.execute("select due from adapter_deadlines").fetchone() == (3000,)
        runner.tick(3001)
        assert count(counter) == 4
        report.write_text(json.dumps(catalogue(p)))
        runner.tick(3060)
        assert count(counter) == 5 and conn.execute("select count(*) from outbox").fetchone() == (3,)
        assert conn.execute("select due from adapter_deadlines").fetchone() == (3600,)
    finally:
        conn.close()


def test_local_configuration_change_reconciles_without_graph_subscription_change(setup, tmp_path):
    p, config, _, _, counter, command, _ = setup
    conn = connect(tmp_path / "outbox.sqlite")

    class PendingSink:
        def delivery_scope(self):
            return {"fixture": "pending"}

        def deliver(self, *_):
            raise ConnectionError("synthetic pending receipt")

    try:
        first = Emitter("fixture", command(), 3600, p.owner, event="scheduled", baseline_emits=True, change_driven=True)
        runner = ChangeRunner(first, conn, PendingSink())
        runner.tick(1801)
        assert count(counter) == 2
        config["reader"]["timeout_s"] = 3
        updated = Emitter(
            "fixture", command(), 3600, p.owner, event="scheduled", baseline_emits=True, change_driven=True
        )
        replacement = ChangeRunner(updated, conn, PendingSink())
        assert replacement.subscription["graphs"] == runner.subscription["graphs"]
        assert replacement.subscription["configuration_sha256"] != runner.subscription["configuration_sha256"]
        assert conn.execute("SELECT last_success FROM adapter_reconcile").fetchone() is None
        replacement.tick(1802)
        assert count(counter) == 4 and conn.execute("SELECT COUNT(*) FROM outbox").fetchone() == (1,)
    finally:
        conn.close()


def test_projection_configuration_requires_an_object_not_pairs(setup):
    _, config, _, _, counter, _, run = setup
    config["projection"] = list(config["projection"].items())
    result = run()
    assert result.returncode == 2 and not result.stdout and count(counter) == 0
