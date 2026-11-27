"""aegis-ztja.1: a failing adapter's stderr TAIL (where a traceback names its
exception) survives into the error record and log, bounded and redacted.

The adapters are REAL subprocesses so the path the runners use is exercised.
"""
import logging
import sys

import pytest

import adapter_stderr as ae
import emitter as em
import incremental

EXCEPTION = "ValueError: directive_edges: the real cause is here"
SECRET = "ghp_" + "A1b2C3d4E5f6G7h8I9j0K1l2M3n4O5p6Q7r8"
BEARER = "Bearer " + "s3cr3tT0kenValue.abcdef0123456789"

FAILING = (
    "import sys\n"
    "sys.stderr.write('Traceback (most recent call last):\\n')\n"
    "sys.stderr.write(('  File \"adapter.py\", line 1, in noise\\n') * 300)\n"  # ~11 KiB
    f"sys.stderr.write('Authorization: {BEARER}\\n')\n"
    f"sys.stderr.write('token={SECRET}\\n')\n"
    f"raise ValueError('directive_edges: the real cause is here')\n"
)


def adapter(tmp_path, source):
    path = tmp_path / "adapter.py"
    path.write_text(source)
    return [sys.executable, str(path)]


def emitter_for(command):
    return em.Emitter(label="directive-untraced", command=command, interval_s=60,
                      owner="crew", timeout_s=30)


def assert_diagnostic(message):
    assert EXCEPTION in message                       # the final exception line survives
    assert message.rstrip().endswith(EXCEPTION)
    assert "bytes elided]..." in message               # truncation is explicit
    assert message.startswith("adapter exited 1: Traceback")  # the head is kept too
    assert SECRET not in message and "s3cr3tT0kenValue" not in message
    assert "[REDACTED]" in message
    detail = message.split(": ", 1)[1]
    assert len(detail.encode()) <= ae.HEAD_BYTES + ae.DEFAULT_TAIL_BYTES + 64


def test_scheduled_adapter_failure_keeps_tail_and_redacts(tmp_path):
    with pytest.raises(RuntimeError) as caught:
        em.run_adapter(emitter_for(adapter(tmp_path, FAILING)))
    assert_diagnostic(str(caught.value))


def test_incremental_adapter_failure_keeps_tail_and_redacts(tmp_path):
    with pytest.raises(RuntimeError) as caught:
        incremental.call(emitter_for(adapter(tmp_path, FAILING)), {"entities": []})
    assert_diagnostic(str(caught.value))


def test_runner_logs_the_exception_and_keeps_it_out_of_metrics(tmp_path, caplog):
    conn = em.connect(":memory:")
    runner = em.Runner(emitter_for(adapter(tmp_path, FAILING)), conn,
                       em.JsonlSink(tmp_path / "out.jsonl"))
    with caplog.at_level(logging.WARNING, logger="chaski.emitter"):
        runner.tick(1000.0)
    logged = "\n".join(r.getMessage() for r in caplog.records)
    assert EXCEPTION in logged and SECRET not in logged
    assert runner.runs["unknown"] == 1
    metrics = "\n".join(runner.metrics(lambda v: v))
    assert "real cause" not in metrics and "elided" not in metrics


def test_zero_exit_stores_nothing(tmp_path, caplog):
    ok = adapter(tmp_path, "import sys\nsys.stderr.write('chatter ' * 2000)\nprint('[]')\n")
    assert em.run_adapter(emitter_for(ok)) == []
    conn = em.connect(":memory:")
    runner = em.Runner(emitter_for(ok), conn, em.JsonlSink(tmp_path / "out.jsonl"))
    with caplog.at_level(logging.WARNING, logger="chaski.emitter"):
        runner.tick(1000.0)
    assert runner.runs == {"ok": 1, "unknown": 0}
    assert "chatter" not in caplog.text


def test_short_stderr_is_unchanged_and_unmarked():
    assert ae.stderr_diagnostic("  boom\n") == "boom"
    assert ae.stderr_diagnostic(None) == ""
    assert ae.stderr_diagnostic(b"bytes ok") == "bytes ok"


def test_tail_bound_is_configurable_and_clamped(monkeypatch):
    text = "x" * 10000 + "END"
    monkeypatch.setenv(ae.TAIL_ENV, "100")
    out = ae.stderr_diagnostic(text)
    assert out.endswith("END") and f"...[{len(text) - ae.HEAD_BYTES - 100} bytes elided]..." in out
    monkeypatch.setenv(ae.TAIL_ENV, "not-a-number")
    assert ae.tail_bytes() == ae.DEFAULT_TAIL_BYTES
    monkeypatch.setenv(ae.TAIL_ENV, str(10**9))
    assert ae.tail_bytes() == ae.MAX_TAIL_BYTES


def test_secret_split_by_the_cut_is_redacted_before_cutting():
    # The secret sits exactly where the tail begins; redaction runs on the
    # whole text first, so no fragment of it survives the cut.
    text = "h" * ae.HEAD_BYTES + "m" * 5000 + "password=" + "Z9" * 30 + "\n" + "t" * 50
    out = ae.stderr_diagnostic(text, tail=60)
    assert "Z9Z9" not in out


@pytest.mark.parametrize("raw", [
    "Authorization: Bearer abcdefgh12345678",
    '{"api_key": "abcdefgh12345678"}',  # gitleaks:allow (fake fixture)
    "QUIPU_TOKEN=abcdefgh12345678",  # gitleaks:allow (fake fixture)
    "https://user:hunter2pass@example.invalid/x",
    "sk-" + "a" * 32,
    "eyJhbGciOiJI.eyJzdWIiOiIx.c2lnbmF0dXJlX3g",
])
def test_secret_shapes_are_redacted(raw):
    out = ae.redact(raw)
    assert "[REDACTED]" in out
    assert "abcdefgh12345678" not in out and "hunter2pass" not in out
    assert "a" * 32 not in out and "c2lnbmF0dXJlX3g" not in out


def test_utf8_boundary_does_not_raise():
    out = ae.stderr_diagnostic("é" * 5000, tail=101, head=11)
    assert "bytes elided" in out
