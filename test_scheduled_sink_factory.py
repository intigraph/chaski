"""Opt-in factory with actual subprocesses/outbox; Shuttle CLI is a test double."""

from dataclasses import asdict, replace
from pathlib import Path
import hashlib
import json
import sys

import pytest
import yaml

import chaski
import incremental
from emitter import Emitter, connect, deliver_pending, observe
from scheduled_receiver import run_id
from scheduled_sink_factory import execution_manifest_sha256
from shuttle_process_port import PortRefused
from test_shuttle_process_port import setup as port_fixture


@pytest.fixture
def configured(tmp_path):
    _, scope, trace, config, executable = port_fixture.__wrapped__(tmp_path)
    # A real child with an owned counter, never the production program.
    counter = tmp_path / "child-calls"
    program = (
        "import os,json;from pathlib import Path;"
        f'p=Path({str(counter)!r});p.write_text(str(int(p.read_text())+1) if p.exists() else "1");'
        'os.write(int(os.environ["RESULT_FD"]),json.dumps(dict('
        'schema_version=1,outcome="skipped-by-hold",exit_code=0)).encode())'
    )
    spec = dict(
        scope=asdict(scope),
        ledger=str(tmp_path / "slots.sqlite"),
        shuttle=dict(
            executable=str(executable),
            config=str(config),
            config_sha256=hashlib.sha256(config.read_bytes()).hexdigest(),
            timeout_s=5,
        ),
        execute=dict(
            argv=[sys.executable, "-c", program],
            cwd=str(tmp_path),
            env={},
            result_fd_env="RESULT_FD",
            timeout_s=2,
            output_limit=1048576,
        ),
        workers=1,
        capacity=2,
    )
    scope = replace(scope, manifest_sha256=execution_manifest_sha256(spec["execute"]))
    spec["scope"] = asdict(scope)
    definition = dict(
        label="fixture",
        command=["unused"],
        schedule="PT1H",
        owner="operator",
        trigger="changes",
        event="scheduled",
        baseline_emits=True,
        sink={"scheduled": spec},
    )
    path = tmp_path / "emitters.yml"

    def load():
        path.write_text(yaml.safe_dump({"emitters": [definition]}))
        return chaski.load_emitters(path)[0]

    return load, definition, spec, scope, trace, counter, config, executable


def test_actual_outbox_factory_waits_for_terminal_process_receipt(configured, tmp_path):
    load, _, spec, scope, trace, counter, _, _ = configured
    e, d = load()
    sink = chaski.make_sink(e, d, "http://unused.invalid")
    spec["execute"]["argv"][:] = [sys.executable, "-c", "raise SystemExit(9)"]
    conn = connect(tmp_path / "outbox.sqlite")
    try:
        observe(conn, e, [dict(item=scope.job, owner=e.owner, verdict="UNBLOCKED", event_id=run_id(scope, 600))], 600)
        assert deliver_pending(conn, sink, 600, e.label) == (0, 1)
        assert conn.execute("SELECT delivered_at FROM outbox").fetchone() == (None,)
        with sink.receiver._lock:
            future = next(iter(sink.receiver._pending.values()))
        future.result(timeout=5)
        assert deliver_pending(conn, sink, 1600, e.label) == (1, 0)
        assert counter.read_text() == "1"
        events = [json.loads(x) for x in trace.read_text().splitlines()]
        assert [x["action"] for x in events] == ["admit", "started", "finished", "publish"]
        assert events[2]["packet"]["outcome"] == {"outcome": "skipped-by-hold", "exit_code": 0}
        assert deliver_pending(conn, sink, 2600, e.label) == (0, 0)
    finally:
        sink.close()
        conn.close()
    assert spec["execute"]["env"] == {}  # no descriptor or ambient environment leaked into policy


def test_lost_publication_receipt_retries_publication_without_child_replay(configured, tmp_path):
    load, _, spec, scope, trace, counter, config, executable = configured
    config.write_text(json.dumps({"trace": str(trace), "mode": "badpublish"}))
    spec["shuttle"]["config_sha256"] = hashlib.sha256(config.read_bytes()).hexdigest()
    executable.write_text(
        executable.read_text().replace(
            "if mode == 'fail': sys.exit(3)",
            "if mode == 'fail' or (mode == 'badpublish' and action == 'publish' and "
            "sum(json.loads(x)['action']=='publish' for x in "
            "Path(config['trace']).read_text().splitlines())==1): sys.exit(3)",
        )
    )
    e, d = load()
    sink = chaski.make_sink(e, d, "http://unused.invalid")
    conn = connect(tmp_path / "outbox.sqlite")
    try:
        observe(conn, e, [dict(item=scope.job, owner=e.owner, verdict="UNBLOCKED", event_id=run_id(scope, 600))], 600)
        assert deliver_pending(conn, sink, 600, e.label) == (0, 1)
        with sink.receiver._lock:
            future = next(iter(sink.receiver._pending.values()))
        with pytest.raises(PortRefused):
            future.result(timeout=5)
        assert deliver_pending(conn, sink, 1600, e.label) == (0, 1)  # consume actual worker error
        assert deliver_pending(conn, sink, 2600, e.label) == (0, 1)  # publication replay submitted
        with sink.receiver._lock:
            future = next(iter(sink.receiver._pending.values()))
        future.result(timeout=5)
        assert deliver_pending(conn, sink, 3600, e.label) == (1, 0)
        assert counter.read_text() == "1"
        events = [json.loads(x)["action"] for x in trace.read_text().splitlines()]
        assert events == ["admit", "started", "finished", "publish", "admit", "publish"]
    finally:
        sink.close()
        conn.close()


@pytest.mark.parametrize("change", ["changed", "symlink", "fifo", "writable", "ledger-symlink"])
def test_local_custody_refuses_before_ledger_or_any_process(configured, tmp_path, change):
    import os

    load, _, spec, _, trace, counter, config, _ = configured
    e, d = load()
    if change == "changed":
        config.write_text("{}")
    elif change == "writable":
        config.chmod(0o666)
    elif change == "ledger-symlink":
        Path(spec["ledger"]).symlink_to(tmp_path / "not-a-ledger")
    else:
        config.unlink()
        if change == "symlink":
            config.symlink_to(tmp_path / "elsewhere")
        else:
            os.mkfifo(config)
    with pytest.raises((ValueError, OSError)):
        chaski.make_sink(e, d, "http://unused.invalid")
    assert not trace.exists() and not counter.exists()
    assert not (tmp_path / "not-a-ledger").exists()
    if change != "ledger-symlink":
        assert not (tmp_path / "slots.sqlite").exists()


@pytest.mark.parametrize(
    "field,value",
    [
        ("workers", True),
        ("workers", 0),
        ("capacity", 65),
        ("ledger", "relative"),
        ("scope", {}),
        ("execute", {}),
        ("shuttle", {}),
        ("extra", 1),
    ],
)
def test_bad_configuration_refuses_during_load_without_creating_state(configured, tmp_path, field, value):
    load, _, spec, _, trace, counter, _, _ = configured
    spec[field] = value
    with pytest.raises(chaski.RuleError):
        load()
    assert not (tmp_path / "slots.sqlite").exists() and not trace.exists() and not counter.exists()


@pytest.mark.parametrize(
    "field,value",
    [
        ("event", "unblocked"),
        ("baseline_emits", False),
        ("generation_event_ids", True),
        ("trigger", "schedule"),
        ("label", "invalid label"),
        ("key", "foreign"),
        ("from", "OK"),
        ("to", "BAD"),
        ("resolve_recovery", True),
    ],
)
def test_incompatible_emitter_policy_refuses(configured, field, value):
    load, definition, *_ = configured
    definition[field] = value
    with pytest.raises(chaski.RuleError):
        load()


def test_consumer_closes_every_sink_and_database_before_releasing_lease(tmp_path, monkeypatch):
    import emitter
    from consumer_lease import ConsumerLease, ConsumerLeaseError

    state, inbox = tmp_path / "cursor.json", tmp_path / "emitter.sqlite"
    events = []

    class Sink:
        def __init__(self, name):
            self.name = name

        def close(self):
            with pytest.raises(ConsumerLeaseError):
                with ConsumerLease(state, inbox):
                    pass
            events.append(self.name)

    class Conn:
        def close(self):
            events.append("database")

    monkeypatch.setattr(chaski, "load_rules", lambda _: [])
    monkeypatch.setattr(
        chaski,
        "load_emitters",
        lambda _: [(Emitter("one", ["unused"], 60, "operator"), {}), (Emitter("two", ["unused"], 60, "operator"), {})],
    )
    monkeypatch.setattr(chaski, "make_sink", lambda e, *_: Sink(e.label))
    monkeypatch.setattr(emitter, "connect", lambda _: Conn())

    class Runner:
        def __init__(self, e, *_):
            self.emitter = e

    monkeypatch.setattr(emitter, "Runner", Runner)
    monkeypatch.setattr(chaski, "_serve_consumer", lambda *_: events.append("serve") or 0)
    assert (
        chaski.main(
            [
                "--quipu",
                "http://unused.invalid",
                "--rules",
                str(tmp_path / "rules.yml"),
                "--state",
                str(state),
                "--emitters",
                str(tmp_path / "emitters.yml"),
                "--emitter-db",
                str(inbox),
                "--once",
            ]
        )
        == 0
    )
    assert events == ["serve", "two", "one", "database"]
    with ConsumerLease(state, inbox):
        pass


def test_shutdown_failure_still_drains_other_owned_sinks():
    calls = []

    class Sink:
        def __init__(self, name):
            self.name = name

        def close(self):
            calls.append(self.name)
            if self.name == "two":
                raise RuntimeError("synthetic close failure")

    with pytest.raises(RuntimeError, match="synthetic"):
        chaski._close_sinks([Sink("one"), Sink("two")])
    assert calls == ["two", "one"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("argv", [sys.executable, "-c", "pass"]),
        ("env", {"EXAMPLE": "changed"}),
        ("timeout_s", 3),
        ("output_limit", 1024),
    ],
)
def test_changed_literal_recipe_cannot_keep_old_manifest_digest(configured, tmp_path, field, value):
    load, _, spec, _, trace, counter, _, _ = configured
    spec["execute"][field] = value
    with pytest.raises(chaski.RuleError, match="manifest digest"):
        load()
    assert not trace.exists() and not counter.exists() and not (tmp_path / "slots.sqlite").exists()


def test_partial_startup_failure_closes_constructed_sinks_and_database(tmp_path, monkeypatch):
    import emitter

    events = []

    class Conn:
        def close(self):
            events.append("database")

    class Sink:
        def __init__(self, name):
            self.name = name

        def close(self):
            events.append(self.name)

    class Runner:
        def __init__(self, e, *_):
            self.emitter = e

    def failed(*_):
        raise RuntimeError("synthetic adapter startup failure")

    monkeypatch.setattr(chaski, "load_rules", lambda _: [])
    monkeypatch.setattr(
        chaski,
        "load_emitters",
        lambda _: [
            (Emitter("one", ["unused"], 60, "operator"), {}),
            (Emitter("two", ["unused"], 60, "operator", change_driven=True), {}),
        ],
    )
    monkeypatch.setattr(chaski, "make_sink", lambda e, *_: Sink(e.label))
    monkeypatch.setattr(emitter, "connect", lambda _: Conn())
    monkeypatch.setattr(emitter, "Runner", Runner)
    monkeypatch.setattr(incremental, "ChangeRunner", failed)
    args = type(
        "Args",
        (),
        dict(
            quipu="http://unused.invalid",
            rules=tmp_path / "rules.yml",
            state=tmp_path / "cursor.json",
            event_poll=60,
            emitters=tmp_path / "emitters.yml",
            emitter_db=tmp_path / "emitter.sqlite",
            emitter_write_interval=0,
        ),
    )()
    with pytest.raises(RuntimeError, match="adapter startup"):
        chaski.run_consumer(args)
    assert events == ["two", "one", "database"]
