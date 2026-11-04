"""Stack-prime conformance and read-only boundaries; no live receiver probes."""

import ast
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import chaski
import chaski_prime
import chaski_prime_state
from chaski_primer import PRIMER

ROOT = Path(__file__).resolve().parent
GOOD = b"chaski_changes_cursor_offset 17\nchaski_changes_lag_transactions 0\nchaski_events_lag 0\n"


def snapshot(root):
    result = {}
    for path in [root, *root.rglob("*")]:
        info = path.stat()
        result[str(path.relative_to(root))] = (
            info.st_mode,
            info.st_size,
            info.st_mtime_ns,
            path.read_bytes() if path.is_file() else None,
        )
    return result


def call(root, *args, source=ROOT):
    env = dict(os.environ, HOME=str(root / "home"), XDG_STATE_HOME=str(root / "xdg"))
    # Do not set PYTHONDONTWRITEBYTECODE: prime must protect fresh source imports itself.
    started = time.monotonic()
    result = subprocess.run(
        [sys.executable, str(source / "chaski.py"), "prime", *map(str, args)],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        timeout=2,
    )
    assert time.monotonic() - started < 1
    return result


@contextmanager
def receiver(body=GOOD, code=200, slow=False):
    requests = []
    release = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append((self.command, self.path))
            if slow:
                release.wait(timeout=2)
            self.send_response(code)
            if code == 302:
                self.send_header("Location", "/metrics")
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/metrics", requests
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_generated_installed_module_docs_and_skill_match():
    primer = (ROOT / "primer.md").read_text()
    assert PRIMER == primer, "run python3 scripts/sync-primer.py"
    assert (ROOT / "docs/agent-primer.md").read_text() == primer, "run python3 scripts/sync-primer.py"
    body = (ROOT / "skills/chaski/SKILL.md").read_text().split("<!-- BEGIN chaski prime -->\n")[1]
    assert body.split("<!-- END chaski prime -->")[0] == primer, "run python3 scripts/sync-primer.py"
    tree = ast.parse((ROOT / "chaski_primer.py").read_text())
    assert ast.literal_eval(tree.body[1].value) == primer
    _, sections = chaski_prime.select_section(None)
    assert sections[:4] == ["What chaski is", "Agent workflow", "Core commands", "MCP tools"]
    assert sections[-1] == "Next steps"


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["--json"],
        ["--static", "--json"],
        ["--brief"],
        ["--brief", "--static"],
        ["--brief", "--json"],
        ["--section", "CoRe", "--static"],
    ],
)
def test_native_cli_contract_and_empty_home_cwd_no_writes(tmp_path, args):
    (tmp_path / "home").mkdir()
    before = snapshot(tmp_path)
    result = call(tmp_path, *args)
    assert result.returncode == 0, result.stderr
    if "--json" in args:
        obj = json.loads(result.stdout)
        assert {"schema", "tool", "version", "primer", "live", "status"} <= obj.keys()
        assert obj["schema"] == "stack-prime/1" and obj["tool"] == "chaski"
        assert obj["version"] and obj["primer"]["sections"]
        assert ("text" in obj["primer"]) == ("--brief" not in args)
        assert {"status", "elapsed_ms", "summary"} <= obj["live"].keys()
        assert obj["live"]["status"] == ("skipped" if "--static" in args else "absent")
        assert obj["status"] == ("ok" if "--static" in args else "degraded")
    elif "--brief" in args:
        lines = result.stdout.strip().splitlines()
        assert 5 <= len(lines) <= 10
        assert lines[0].startswith("chaski ")
        assert lines[-1] == "Run `chaski prime` for the full primer."
    elif "--section" in args:
        assert result.stdout.startswith("## Core commands")
        assert "## MCP tools" not in result.stdout and "## Live state" not in result.stdout
    else:
        assert "# Chaski agent primer" in result.stdout and "## Next steps" in result.stdout
        assert "## Live state" in result.stdout and "State: absent" in result.stdout
    assert before == snapshot(tmp_path)


@pytest.mark.parametrize(
    "args", [["--unknown"], ["--section", "missing"], ["--section", ""], ["--brief", "--section", "core"]]
)
def test_native_usage_errors_exit_two(tmp_path, args):
    result = call(tmp_path, *args)
    assert result.returncode == 2
    if "missing" in args:
        assert "What chaski is, Agent workflow, Core commands, MCP tools" in result.stderr


def test_fresh_source_imports_create_no_bytecode_in_working_tree(tmp_path):
    for name in ["chaski.py", "chaski_prime.py", "chaski_prime_state.py", "chaski_primer.py", "pyproject.toml"]:
        shutil.copyfile(ROOT / name, tmp_path / name)
    before = snapshot(tmp_path)
    result = call(tmp_path, "--json", source=tmp_path)
    assert result.returncode == 0, result.stderr
    assert before == snapshot(tmp_path)


def test_configured_definitions_legacy_offset_and_inbox_metadata_only(tmp_path):
    rules = tmp_path / "rules.yaml"
    rules.write_text("""rules:
- label: fixture-rule
  triggerKind: event
  eventTypes: commit
  condition: 'SELECT ?focus WHERE { ?focus ?p ?o }'
  severity: info
  action: alert
  owner: fixture
""")
    emitters = tmp_path / "emitters.yaml"
    emitters.write_text("emitters:\n- label: fixture-emitter\n  command: [must-not-execute]\n")
    legacy = tmp_path / "cursor.json"
    legacy.write_text('{"cursor": 9}')
    inbox = tmp_path / "inbox.db"
    inbox.write_bytes(b"not SQLite; file metadata is intentional")
    (tmp_path / "inbox.db-wal").write_bytes(b"unread WAL fixture")
    before = snapshot(tmp_path)
    args = SimpleNamespace(rules=rules, emitters=emitters, state=legacy, inbox=inbox, metrics_url=None)
    with (
        patch("sqlite3.connect", side_effect=AssertionError("prime must not open SQLite")),
        patch.object(chaski, "Reactor", side_effect=AssertionError("prime must not construct a receiver")),
    ):
        state = chaski_prime_state.inspect(args)
    assert state["status"] == "ok"
    assert state["summary"]["rules_configured"] == state["summary"]["emitters_configured"] == 1
    assert state["summary"]["sources"]["legacy"]["offset"] == 9
    assert state["summary"]["sources"]["inbox"]["verification"] == "file_metadata_only"
    assert state["summary"]["sources"]["inbox"]["cursor"] is None
    result = call(
        tmp_path, "--json", "--rules", rules, "--emitters", emitters, "--state", legacy, "--emitter-db", inbox
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["live"]["status"] == "ok"
    assert before == snapshot(tmp_path)


@pytest.mark.parametrize(
    "body,status",
    [
        (GOOD, "ok"),
        (b"chaski_changes_cursor_offset -1\n", "partial"),
        (b"# empty metrics\n", "unreadable"),
        (GOOD + b"chaski_changes_cursor_offset 17\n", "unreadable"),
        (b"chaski_changes_cursor_offset NaN\n", "unreadable"),
        (b"x" * 65537, "unreadable"),
        (b"\xff", "unreadable"),
    ],
)
def test_metrics_native_get_bounds_unknown_is_not_zero(tmp_path, body, status):
    with receiver(body) as (url, requests):
        before = snapshot(tmp_path)
        result = call(tmp_path, "--json", "--metrics-url", url)
        assert result.returncode == 0, result.stderr
        obj = json.loads(result.stdout)
        assert obj["live"]["status"] == status
        source = obj["live"]["summary"]["sources"]["receiver"]
        assert source["url"] == url and source["freshness"] == "not_measured"
        if status == "ok":
            assert source["reported_stream_cursor"] == 17 and source["reported_lag_tx"] == 0
            assert source["source_epoch_binding"] == "not_measured"
        assert requests == [("GET", "/metrics")]
        assert before == snapshot(tmp_path)


@pytest.mark.parametrize("code", [302, 503])
def test_no_redirects_or_http_retries(tmp_path, code):
    with receiver(code=code) as (url, requests):
        result = call(tmp_path, "--json", "--metrics-url", url)
        assert json.loads(result.stdout)["live"]["status"] == "unreadable"
        assert requests == [("GET", "/metrics")]


def test_unreachable_is_not_a_deadline_and_keeps_full_primer(tmp_path):
    server = ThreadingHTTPServer(("127.0.0.1", 0), BaseHTTPRequestHandler)
    url = f"http://127.0.0.1:{server.server_port}/metrics"
    server.server_close()
    result = call(tmp_path, "--metrics-url", url)
    assert result.returncode == 0
    assert "## Next steps" in result.stdout and "State: unreachable" in result.stdout and url in result.stdout
    result = call(tmp_path, "--json", "--metrics-url", url)
    assert json.loads(result.stdout)["live"]["status"] == "unreachable"


def test_slow_metrics_reports_partial_elapsed_under_one_second(tmp_path):
    with receiver(slow=True) as (url, requests):
        before = snapshot(tmp_path)
        result = call(tmp_path, "--json", "--metrics-url", url)
        obj = json.loads(result.stdout)
        assert obj["live"]["status"] == "partial"
        assert 0 <= obj["live"]["elapsed_ms"] < 1000
        assert requests == [("GET", "/metrics")]
        assert before == snapshot(tmp_path)


def test_whole_read_deadline_releases_only_private_worker():
    release, done = threading.Event(), threading.Event()

    def slow(_args):
        release.wait(timeout=2)
        done.set()
        return {"status": "ok", "summary": {}}

    try:
        state = chaski_prime.summarize(SimpleNamespace(), slow)
        assert state["status"] == "partial" and state["summary"]["reason"] == "deadline"
        assert 0 <= state["elapsed_ms"] < 1000
    finally:
        release.set()
        assert done.wait(timeout=1)


def test_invalid_inputs_and_static_skip_never_initialize(tmp_path):
    bad = tmp_path / "bad.json"
    for text in ['{"cursor": true}', '{"cursor": -2}', "[]", "not JSON"]:
        bad.write_text(text)
        result = call(tmp_path, "--json", "--state", bad)
        assert json.loads(result.stdout)["live"]["status"] == "unreadable"
    result = call(tmp_path, "--json", "--state", tmp_path / "missing/cursor.json")
    assert json.loads(result.stdout)["live"]["status"] == "absent"
    assert not (tmp_path / "missing").exists()
    result = call(
        tmp_path,
        "--static",
        "--json",
        "--state",
        bad,
        "--metrics-url",
        "https://user:private@example.org/metrics?private=1",
    )
    assert json.loads(result.stdout)["live"]["status"] == "skipped"
    result = call(tmp_path, "--json", "--metrics-url", "https://user:private@example.org/metrics?private=1")
    assert json.loads(result.stdout)["live"]["status"] == "unreadable" and "private" not in result.stdout
