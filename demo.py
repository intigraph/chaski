"""An offline canary round trip through the real reaction engine."""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

from chaski import Reactor, Rule


def exercise(focus: str, cursor: Path) -> dict:
    """Controlled query outcomes; no graph connection or notification sink."""
    class Fixture:
        outcome = 0

        def select_focus(self, _query):
            self.outcome += 1
            if self.outcome == 2:
                raise TimeoutError("offline UNKNOWN control")
            return {focus} if self.outcome == 1 else set()

    rule = Rule.from_dict(dict(label="offline-canary", triggerKind="schedule", schedule="PT1M",
                               condition="SELECT ?focus WHERE { ?focus a <urn:chaski:Canary> }",
                               severity="info", action=["alert"], owner="demo"))
    reactor = Reactor(Fixture(), [rule], cursor, 60)
    reactor.evaluate(rule, 1.0)
    state = reactor.states[rule.label]
    if state.firing != {focus} or state.transitions != 1:
        raise RuntimeError("canary did not fire")
    reactor.evaluate(rule, 2.0)
    if state.firing != {focus} or state.last_success != 1.0 or state.evaluations["unknown"] != 1:
        raise RuntimeError("UNKNOWN did not preserve the firing canary")
    reactor.evaluate(rule, 3.0)
    if state.firing or state.transitions != 2 or state.evaluations != {"true": 1, "false": 1, "unknown": 1}:
        raise RuntimeError("canary did not resolve after a measured empty answer")
    return dict(version=1, focus=focus, fired=True, unknown_preserved=True, resolved=True, transitions=2)


def read_report(path: Path) -> dict | None:
    try:
        report = json.loads(path.read_text())
    except FileNotFoundError:
        return None
    if (not isinstance(report, dict) or report.get("version") != 1
            or not isinstance(report.get("focus"), str) or not report["focus"]
            or any(report.get(key) is not True for key in ("fired", "unknown_preserved", "resolved"))
            or report.get("transitions") != 2):
        raise ValueError("invalid offline canary report")
    return report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["run", "status"])
    default = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local/state")) / "chaski/demo.json"
    parser.add_argument("--state", type=Path, default=default, help="local demo receipt (not live reactor state)")
    parser.add_argument("--focus", help="canary identity, required for run")
    args = parser.parse_args(argv)
    try:
        if args.action == "status":
            report = read_report(args.state)
            print(json.dumps(report, sort_keys=True) if report else "No offline canary runs.")
            return 0
        if not args.focus or len(args.focus) > 512:
            parser.error("run requires --focus (1 to 512 characters)")
        # An isolated cursor prevents accidental reads from a live consumer.
        with tempfile.TemporaryDirectory(prefix="chaski-canary-") as directory:
            report = exercise(args.focus, Path(directory) / "cursor.json")
        args.state.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        descriptor, temporary = tempfile.mkstemp(prefix=".canary-", dir=args.state.parent)
        try:
            with os.fdopen(descriptor, "w") as stream:
                json.dump(report, stream, sort_keys=True)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, args.state)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        print("Canary: FIRING -> UNKNOWN (preserved) -> RESOLVED.")
        return 0
    except (OSError, ValueError, RuntimeError) as error:
        print(f"UNKNOWN: offline canary: {error}", file=sys.stderr)
        return 2
