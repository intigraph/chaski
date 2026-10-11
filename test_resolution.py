"""Recovery retains exact alert identity and cannot overtake an unreceived firing."""
import json
import unittest

import emitter as em
from alertmanager_sink import AlertmanagerSink


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.db = em.connect(":memory:")
        self.e = em.Emitter(label="family", command=[], interval_s=60, owner="crew",
                            from_verdict="BELOW", to_verdict="ABOVE",
                            resolve_event="recovered")

    def tearDown(self):
        self.db.close()

    def observe(self, verdict, now):
        r = {"item": "family-1", "verdict": verdict, "event_id": "crossing-1",
             "owner": "crew"}
        return em.observe(self.db, self.e, [r], now)

    def test_unknown_and_recovery(self):
        self.assertEqual(self.observe("BELOW", 100), [])
        self.assertEqual(self.observe("ABOVE", 200), ["crossing-1"])
        self.assertEqual(self.observe("ABOVE", 210), [])
        self.assertEqual(self.observe("UNKNOWN", 220), [])
        resolved = self.observe("BELOW", 300)
        self.assertEqual(len(resolved), 1)
        self.assertEqual(self.observe("BELOW", 310), [])
        firing = json.loads(self.db.execute("select payload from outbox where event_id='crossing-1'").fetchone()[0])
        recovery = json.loads(self.db.execute("select payload from outbox where event_id=?", resolved).fetchone()[0])
        sink = AlertmanagerSink("https://example.invalid", {"alertname":"Failures", "severity":"warning"}, "family", clock=lambda: 400)
        self.assertEqual(sink.labels(firing), sink.labels(recovery))
        self.assertEqual(sink.payload(firing)["startsAt"], sink.payload(recovery)["startsAt"])
        self.assertEqual(recovery["resolved_at"], 300)
        self.assertLess(sink.payload(recovery)["endsAt"], sink.payload(firing)["endsAt"])

    def test_resolution_waits_for_firing_receipt(self):
        self.observe("BELOW", 100); self.observe("ABOVE", 200); self.observe("BELOW", 300)
        class Sink:
            seen = []
            fail = True
            def deliver(self, event):
                if self.fail: raise ConnectionError("unreceived")
                self.seen.append(event)
        sink=Sink()
        self.assertEqual(em.deliver_pending(self.db,sink,400,self.e.label), (0,1))
        self.assertEqual(sink.seen, [])
        sink.fail=False
        self.assertEqual(em.deliver_pending(self.db,sink,500,self.e.label), (2,0))
        self.assertNotIn("resolution_of",sink.seen[0])
        self.assertEqual(sink.seen[1]["resolution_of"],"crossing-1")

    def test_generation_identity_and_complete_missing(self):
        self.e.generation_event_ids = True
        self.e.missing_is_from = True
        self.observe("BELOW",100)
        first=self.observe("ABOVE",200)
        self.assertEqual(len(first),1)
        self.assertEqual(self.observe("ABOVE",210),[])
        # Partial absence is not a complete zero count.
        self.assertEqual(em.observe(self.db,self.e,[],220,scope=set()),[])
        recovered=em.observe(self.db,self.e,[],300)
        self.assertEqual(len(recovered),1)
        second=self.observe("ABOVE",400)
        self.assertEqual(len(second),1)
        self.assertNotEqual(first,second)

    def test_resolution_receipt_requires_valid_readback(self):
        self.observe("BELOW",100); self.observe("ABOVE",200)
        resolved=self.observe("BELOW",300)
        event=json.loads(self.db.execute("select payload from outbox where event_id=?",resolved).fetchone()[0])
        sink=AlertmanagerSink("https://example.invalid",{"alertname":"Failures","severity":"warning"},"family")
        requests=[]
        def request(method,path,body=None):
            requests.append((method,path,body))
            return (200,b"[]")
        sink._request=request
        sink.deliver(event)
        self.assertEqual(requests[0][2][0]["labels"]["event_id"],"crossing-1")
        self.assertIn("crossing-1",requests[1][1])
        sink._request=lambda method,path,body=None:(200,b'[{"labels":{"event_id":"crossing-1"}}]')
        with self.assertRaises(ConnectionError):sink.deliver(event)
        sink._request=lambda method,path,body=None:(200,b'{}')
        with self.assertRaises(ConnectionError):sink.deliver(event)


if __name__ == "__main__": unittest.main()
