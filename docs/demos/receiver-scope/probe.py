import json
import tempfile
import copy
import datetime as dt
from pathlib import Path
import emitter as em
from alertmanager_sink import AlertmanagerSink


class ReceiverProbe(AlertmanagerSink):
    def __init__(self):
        super().__init__(
            "http://receiver-a.invalid", {"alertname": "Control", "severity": "warning"}, "control", clock=lambda: 102
        )
        self.receivers = {"http://receiver-a.invalid": [], "http://receiver-b.invalid": []}

    def _request(self, method, path, body=None):
        if method == "POST":
            for alert in body:
                end = dt.datetime.fromisoformat(alert["endsAt"].replace("Z", "+00:00")).timestamp()
                self.receivers[self.base] = [] if end <= 200 else copy.deepcopy(body)
        return 200, json.dumps(self.receivers[self.base]).encode()


def rec(v):
    return {"item": "one", "verdict": v, "event_id": "fire-one"}


for changed in [False, True]:
    with tempfile.TemporaryDirectory() as root:
        conn = em.connect(Path(root) / "state.db")
        e = em.Emitter("control", [], 0, "owner", resolve_recovery=True)
        sink = ReceiverProbe()
        em.observe(conn, e, [rec("BLOCKED")], 100)
        em.observe(conn, e, [rec("UNBLOCKED")], 101)
        print(
            "firing delivery",
            em.deliver_pending(conn, sink, 102, e.label),
            "original active",
            len(sink.receivers["http://receiver-a.invalid"]),
        )
        em.observe(conn, e, [rec("BLOCKED")], 103)
        if changed:
            sink.base = "http://receiver-b.invalid"
        result = em.deliver_pending(conn, sink, 104, e.label, resolve_recovery=True)
        delivered = conn.execute("SELECT delivered_at FROM recovery_outbox").fetchone()[0]
        print(
            "changed receiver",
            changed,
            "recovery delivery",
            result,
            "marked delivered",
            delivered is not None,
            "original active",
            len(sink.receivers["http://receiver-a.invalid"]),
            "current active",
            len(sink.receivers[sink.base]),
        )
        conn.close()
