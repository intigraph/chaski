"""Actual base/head consumer loop, retained disabled recovery, fake clock/network."""

import json
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, sys.argv[1])
import emitter as em
import chaski


class Sink:
    def delivery_scope(self):
        return {"fixture": "original-receiver"}

    def deliver(self, event):
        pass

    def prepare_recovery(self, event):
        return {"fixture": "receipt"}

    def resolve(self, event):
        raise AssertionError("disabled recovery must not send")


class End(Exception):
    pass


with tempfile.TemporaryDirectory() as d:
    e = em.Emitter("control", [], 3600, "fixture", resolve_recovery=True)
    c = em.connect(":memory:")
    s = Sink()
    em.observe(c, e, [{"item": "a", "verdict": "BLOCKED"}], 100)
    em.observe(c, e, [{"item": "a", "verdict": "UNBLOCKED", "event_id": "fixture-firing"}], 101)
    assert em.deliver_pending(c, s, 101, e.label) == (1, 0)
    em.observe(c, e, [{"item": "a", "verdict": "BLOCKED"}], 102)
    assert c.execute("SELECT COUNT(*) FROM recovery_outbox WHERE delivered_at IS NULL").fetchone()[0] == 1
    e.resolve_recovery = False
    budget = em.WriteBudget(5)
    budget._last = 101
    runner = em.Runner(e, c, s, budget)
    runner.last_attempt = 102
    r = chaski.Reactor(SimpleNamespace(), [], Path(d) / "cursor", 60)
    r.runners = [runner]
    clock = [103.0]
    turns = []
    waits = []
    orig = r.tick

    def tick(now):
        turns.append(now)
        orig(now)

    r.tick = tick

    def wait(timeout):
        waits.append(timeout)
        if timeout is None or clock[0] + timeout >= 163:
            raise End
        assert timeout > 0, "busy zero wait"
        if len(turns) > 10000:
            raise AssertionError("unbounded turns")
        clock[0] += timeout
        return False

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
                scenario="retained-disabled-recovery",
                window_s=60,
                turns=len(turns),
                min_wait_s=min(w for w in waits if w is not None),
                pending_recovery=c.execute(
                    "SELECT COUNT(*) FROM recovery_outbox WHERE delivered_at IS NULL"
                ).fetchone()[0],
                actual_external_requests=0,
            )
        )
    )
    c.close()
