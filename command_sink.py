"""Command sink — deliver an emitter event to an external command.

The domain work some events need (classify, propose, write) belongs in the
project that owns that domain, not in chaski. A verdict adapter is already an
external command; this sink is its delivery-side twin. It runs `argv` with the
frozen event as JSON on stdin, and exit 0 is the receipt.

The receiver decides exactly-once, as for every sink: the command must be
idempotent on the event id, because a crash between its success and the
emitter's checkpoint re-sends the same event. A non-zero exit, a timeout or a
failure to start raises, and the emitter retries with backoff.
"""
from __future__ import annotations

import json
import re
import subprocess


def failure_diagnostic(output: str) -> str:
    """Keep one bounded correlation record, never expand arbitrary stderr.

    Receivers redact CHASKI_FAILURE before emitting it. Validate the complete
    fixed schema here so an exception message or additional output cannot gain
    a larger logging budget by merely using that prefix.
    """
    text = output.strip()
    prefix = "CHASKI_FAILURE "
    if text.startswith(prefix) and len(text) <= 1024:
        try:
            row = json.loads(text[len(prefix):])
        except ValueError:
            row = None
        patterns = {
            "stage": r"[a-z][a-z0-9-]{0,47}",
            "invocation": r"[0-9a-f]{32}",
            "event_id": r"[0-9a-f]{64}",
            "scope_sha256": r"[0-9a-f]{64}",
            "error": r"[A-Za-z_][A-Za-z0-9_]{0,63}",
            "failure": r"failure-[0-9a-f]{64}\.log",
        }
        if (isinstance(row, dict)
                and set(row) == set(patterns) | {"retention", "process_status"}
                and all((row[key] is None and key not in {"stage", "error"})
                        or (isinstance(row[key], str)
                            and re.fullmatch(pattern, row[key]))
                        for key, pattern in patterns.items())
                and row["retention"] in ("retained", "unknown")
                and (row["process_status"] is None
                     or (type(row["process_status"]) is int
                         and -255 <= row["process_status"] <= 255))):
            return prefix + json.dumps(row, separators=(",", ":"), sort_keys=True)
    return text[:300]


class CommandSink:
    def __init__(self, argv: list[str], timeout_s: float = 120.0):
        if not argv:
            raise ValueError("command sink needs argv")
        self.argv, self.timeout_s = list(argv), timeout_s

    def deliver(self, event: dict) -> None:
        try:
            done = subprocess.run(self.argv, input=json.dumps(event, sort_keys=True),
                                  capture_output=True, text=True, timeout=self.timeout_s,
                                  check=False)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ConnectionError(f"{self.argv[0]}: {exc}") from exc
        if done.returncode != 0:
            raise ConnectionError(f"{self.argv[0]} exited {done.returncode}: "
                                  f"{failure_diagnostic(done.stderr or done.stdout)}")
