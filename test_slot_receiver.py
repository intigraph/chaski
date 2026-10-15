"""Real process/SQLite admission: crashes and competing deliveries."""
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from slot_receiver import SlotConflict, SlotLedger, SlotUnknown, slot_id


class Admission(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.db = self.root / "claims.sqlite3"
        self.ledger = SlotLedger(self.db)
        self.slot = slot_id("urn:job:one", "revision1", 100)
        self.binding = {"host": "host-a", "manifest": "sha256:fixed"}

    def test_success_failure_and_hold_results_replay_without_execution(self):
        for index, outcome in enumerate(("succeeded", "failed", "skipped-by-hold")):
            calls = []
            def action():
                calls.append(1)
                return {"outcome": outcome, "exit": 0 if outcome == "succeeded" else 1}
            key = self.slot + str(index)
            receipt = self.ledger.run(key, self.binding, action)
            self.assertEqual(receipt, SlotLedger(self.db).run(key, self.binding, action))
            self.assertEqual(calls, [1])

    def test_changed_manifest_or_host_cannot_reuse_old_success(self):
        self.ledger.run(self.slot, self.binding, lambda: {"exit": 0})
        for changed in ({**self.binding, "host": "other"},
                        {**self.binding, "manifest": "new"}):
            with self.assertRaises(SlotConflict):
                self.ledger.run(self.slot, changed, lambda: self.fail("executed"))

    def test_lost_result_after_real_side_effect_remains_unknown_after_restart(self):
        child = """
import os, sys
from pathlib import Path
from slot_receiver import SlotLedger
p=Path(sys.argv[1])
def effect():
    with (p/'effect').open('a') as f:
        f.write('once\\n'); f.flush(); os.fsync(f.fileno())
    os._exit(23)
SlotLedger(p/'claims.sqlite3').run(sys.argv[2], {'host':'host-a','manifest':'sha256:fixed'}, effect)
"""
        proc = subprocess.run([sys.executable, "-c", child, str(self.root), self.slot], check=False)
        self.assertEqual(proc.returncode, 23)
        with self.assertRaises(SlotUnknown):
            SlotLedger(self.db).run(self.slot, self.binding, lambda: self.fail("reexecuted"))
        self.assertEqual((self.root / "effect").read_text(), "once\n")

    def test_two_concurrent_processes_cannot_both_execute(self):
        child = """
import json, sys, time
from pathlib import Path
from slot_receiver import SlotLedger, SlotUnknown
p=Path(sys.argv[1])
def effect():
    with (p/'effect').open('a') as f: f.write('once\\n')
    time.sleep(.2)
    return {'exit':0}
try:
    print(json.dumps(SlotLedger(p/'claims.sqlite3').run('slot', {'host':'a'}, effect)))
except SlotUnknown:
    print('UNKNOWN')
"""
        commands = [[sys.executable, "-c", child, str(self.root)] for _ in range(2)]
        procs = [subprocess.Popen(c, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for c in commands]
        answers = [p.communicate(timeout=10) for p in procs]
        self.assertEqual([p.returncode for p in procs], [0, 0], answers)
        self.assertEqual((self.root / "effect").read_text(), "once\n")
        self.assertIn('{"exit": 0}', [a[0].strip() for a in answers])

    def test_callback_failure_does_not_grant_an_execution_retry(self):
        with self.assertRaisesRegex(RuntimeError, "lost"):
            self.ledger.run(self.slot, self.binding, lambda: (_ for _ in ()).throw(RuntimeError("lost")))
        with self.assertRaises(SlotUnknown):
            self.ledger.run(self.slot, self.binding, lambda: self.fail("executed"))

    def test_slot_identity_does_not_depend_on_delivery_and_separates_slots_revisions_jobs(self):
        self.assertEqual(self.slot, slot_id("urn:job:one", "revision1", 100))
        self.assertEqual(len({self.slot, slot_id("urn:job:one", "revision1", 101),
                              slot_id("urn:job:one", "revision2", 100),
                              slot_id("urn:job:two", "revision1", 100)}), 4)
        for bad in (True, -1, 1.0, "100"):
            with self.assertRaises(ValueError):
                slot_id("job", "revision", bad)

    def test_unserializable_binding_refused_before_any_claim(self):
        with self.assertRaises(ValueError):
            self.ledger.run(self.slot, {"invalid": float("nan")}, lambda: self.fail("executed"))
        self.assertEqual(self.ledger.run(self.slot, self.binding, lambda: {"exit": 0}), {"exit": 0})
