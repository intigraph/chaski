"""Actual consumer + ChangeRunner setup retry, empty catalogue, fake clock/adapter."""

import json
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, sys.argv[1])
import emitter as em
import incremental as inc
import chaski


class Sink:
    def __init__(self):
        self.attempts = 0

    def setup_pending(self):
        return True

    def setup(self):
        self.attempts += 1
        raise ConnectionError("synthetic setup unavailable")

    def deliver(self, event):
        raise AssertionError("no firing")


class End(Exception):
    pass


def adapter(e, request=None):
    if request is None:
        return dict(version=1, attributes=["status"], graphs=["fixture"])
    if request.get("discover"):
        return {"items": []}
    raise AssertionError("empty catalogue should not evaluate")


with tempfile.TemporaryDirectory() as d, patch.object(inc, "call", adapter):
    c = em.connect(":memory:")
    e = em.Emitter("control", [], 86400, "fixture", change_driven=True)
    s = Sink()
    runner = inc.ChangeRunner(e, c, s, em.WriteBudget(5))
    r = chaski.Reactor(SimpleNamespace(), [], Path(d) / "cursor", 60)
    r.runners = [runner]
    clock = [100.0]
    turns = []
    waits = []
    original = r.tick

    def tick(now):
        turns.append(now)
        original(now)

    r.tick = tick

    def wait(timeout):
        waits.append(timeout)
        if timeout is None or clock[0] + timeout >= 160:
            raise End
        assert timeout > 0
        clock[0] += timeout
        return False

    r.change_feed = SimpleNamespace(ready=True, poll=lambda: None, take_dirty=lambda: set())
    r.change_stream = SimpleNamespace(pump=lambda feed: None, wait=wait)
    with (
        patch.object(chaski, "serve_metrics", lambda *a: None),
        patch.object(chaski.time, "time", lambda: clock[0]),
        patch.object(chaski.time, "sleep", wait),
        patch.object(chaski.threading, "Event", lambda: SimpleNamespace(wait=wait)),
    ):
        try:
            chaski._serve_consumer(SimpleNamespace(once=False, changes_stream=False, metrics_port=0), r, [])
        except End:
            pass
    print(
        json.dumps(
            dict(
                source=sys.argv[1],
                scenario="incremental-setup-failure",
                window_s=60,
                turns=len(turns),
                setup_attempts=s.attempts,
                next_wait=waits[-1],
                retained_reconcile=c.execute("SELECT last_success FROM adapter_reconcile").fetchone()[0],
                outbox_count=c.execute("SELECT COUNT(*) FROM outbox").fetchone()[0],
                external_requests=0,
            )
        )
    )
    c.close()
