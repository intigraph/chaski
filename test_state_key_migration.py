"""Real SQLite migration controls, including the installed emitter's transition path."""

import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

import emitter as em
import incremental
import state_key_migration as migration


class Migration(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = Path(self.tmp.name) / "emitter.db"
        self.backup = Path(self.tmp.name) / "before.db"
        self.conn = em.connect(self.db)
        self.conn.executescript(incremental.SCHEMA)
        self.conn.row_factory = sqlite3.Row
        self.addCleanup(self.conn.close)
        self.emitter = em.Emitter(label="workitem-unblocked", command=["unused"], interval_s=60, owner="fixture")
        self.short, self.iri = "item-one", "urn:fixture:item-one"

    def observe(self, key, verdict, event="event-one"):
        return em.observe(
            self.conn,
            self.emitter,
            [{"item": key, "verdict": verdict, "event_id": event, "evidence": "fixture", "assignee": "fixture"}],
            10,
            scope=[key],
        )

    def plan(self):
        return {
            "schema": migration.SCHEMA,
            "mappings": [
                {
                    "emitter": self.emitter.label,
                    "from": self.short,
                    "to": self.iri,
                    "expected": migration.digest(
                        {
                            "from": migration.stable(migration.row(self.conn, self.emitter.label, self.short)),
                            "to": migration.stable(migration.row(self.conn, self.emitter.label, self.iri)),
                        }
                    ),
                }
            ],
        }

    def test_silent_baseline_merge_restores_history_then_native_transition_is_once(self):
        self.observe(self.short, "BLOCKED")
        self.observe(self.iri, "UNBLOCKED")
        plan = self.plan()
        before = self.db.read_bytes()
        report = migration.migrate(self.db, plan)
        self.assertEqual((report["merge"], report["noop"], report["conflicts"]), (1, 0, []))
        self.assertEqual(self.db.read_bytes(), before)
        result = migration.migrate(self.db, plan, apply=True, backup=self.backup)
        self.assertEqual(result["changed"], 1)
        self.assertEqual(self.backup.stat().st_mode & 0o777, 0o600)
        self.assertIsNone(migration.row(self.conn, self.emitter.label, self.short))
        self.assertEqual(migration.row(self.conn, self.emitter.label, self.iri)["verdict"], "BLOCKED")
        self.assertEqual(self.observe(self.iri, "UNBLOCKED"), ["event-one"])
        self.assertEqual(self.observe(self.iri, "UNBLOCKED"), [])
        self.assertTrue(migration.migrate(self.db, plan, apply=True, backup=self.backup)["already_applied"])
        self.assertEqual(self.conn.execute("SELECT count(*) FROM outbox").fetchone()[0], 1)
        old = sqlite3.connect(self.backup)
        self.addCleanup(old.close)
        self.assertEqual(old.execute("PRAGMA integrity_check").fetchone()[0], "ok")
        self.assertEqual(old.execute("SELECT count(*) FROM verdicts").fetchone()[0], 2)

    def test_prior_event_ids_payloads_generations_and_receipts_are_preserved(self):
        self.observe(self.short, "BLOCKED")
        self.observe(self.short, "UNBLOCKED")
        self.conn.execute("UPDATE outbox SET delivered_at=9,attempts=3,delivery_scope=?", ("retained-scope",))
        self.conn.commit()
        self.observe(self.short, "BLOCKED")
        before = dict(self.conn.execute("SELECT * FROM outbox").fetchone())
        old_generation = migration.row(self.conn, self.emitter.label, self.short)["generation"]
        result = migration.migrate(self.db, self.plan(), apply=True, backup=self.backup)
        self.assertEqual(result["rename"], 1)
        after = dict(self.conn.execute("SELECT * FROM outbox").fetchone())
        self.assertEqual(after, {**before, "item": self.iri})
        self.assertEqual(json.loads(after["payload"])["item"], self.short)
        canonical = migration.row(self.conn, self.emitter.label, self.iri)
        self.assertEqual(canonical["generation"], old_generation)
        self.assertEqual(canonical["last_event"], before["event_id"])

    def test_conflicting_transition_history_refuses_all_before_any_write(self):
        self.observe(self.short, "BLOCKED")
        self.observe(self.iri, "BLOCKED")
        self.observe(self.iri, "UNBLOCKED")
        plan = self.plan()
        report = migration.migrate(self.db, plan)
        self.assertEqual(len(report["conflicts"]), 1)
        before = self.db.read_bytes()
        with self.assertRaises(ValueError):
            migration.migrate(self.db, plan, apply=True, backup=self.backup)
        self.assertEqual(self.db.read_bytes(), before)
        self.assertFalse(self.backup.exists())

    def test_all_mappings_are_checked_before_any_good_mapping_changes(self):
        self.observe(self.short, "BLOCKED")
        plan = self.plan()
        self.observe("item-two", "BLOCKED")
        self.observe("urn:fixture:item-two", "BLOCKED")
        self.observe("urn:fixture:item-two", "UNBLOCKED", "event-two")
        plan["mappings"].append(
            {
                "emitter": self.emitter.label,
                "from": "item-two",
                "to": "urn:fixture:item-two",
                "expected": migration.digest(
                    {
                        "from": migration.stable(migration.row(self.conn, self.emitter.label, "item-two")),
                        "to": migration.stable(migration.row(self.conn, self.emitter.label, "urn:fixture:item-two")),
                    }
                ),
            }
        )
        with self.assertRaises(ValueError):
            migration.migrate(self.db, plan, apply=True, backup=self.backup)
        self.assertIsNotNone(migration.row(self.conn, self.emitter.label, self.short))
        self.assertIsNone(migration.row(self.conn, self.emitter.label, self.iri))
        self.assertFalse(self.backup.exists())

    def test_canonical_only_is_noop_without_fabricating_a_transition(self):
        self.observe(self.iri, "UNBLOCKED")
        result = migration.migrate(self.db, self.plan(), apply=True, backup=self.backup)
        self.assertEqual(result["changed"], 0)
        self.assertEqual(self.conn.execute("SELECT count(*) FROM outbox").fetchone()[0], 0)
        self.assertEqual(self.observe(self.iri, "UNBLOCKED"), [])

    def test_changed_state_refuses_but_observation_time_alone_does_not(self):
        self.observe(self.short, "BLOCKED")
        plan = self.plan()
        self.conn.execute("UPDATE verdicts SET updated=200")
        self.conn.commit()
        self.assertEqual(migration.migrate(self.db, plan)["conflicts"], [])
        self.observe(self.short, "UNBLOCKED")
        with self.assertRaises(ValueError):
            migration.migrate(self.db, plan, apply=True, backup=self.backup)

    def test_exclusive_backup_refuses_overwrite_before_state_change(self):
        self.observe(self.short, "BLOCKED")
        self.backup.write_bytes(b"preserve")
        with self.assertRaises(FileExistsError):
            migration.migrate(self.db, self.plan(), apply=True, backup=self.backup)
        self.assertEqual(self.backup.read_bytes(), b"preserve")
        self.assertIsNotNone(migration.row(self.conn, self.emitter.label, self.short))

    def test_deadline_collision_is_not_silently_resolved(self):
        self.observe(self.short, "BLOCKED")
        self.observe(self.iri, "UNBLOCKED")
        self.conn.execute("INSERT INTO adapter_deadlines VALUES (?,?,?,?)", (self.emitter.label, self.iri, 30, 1))
        self.conn.commit()
        self.assertEqual(len(migration.migrate(self.db, self.plan())["conflicts"]), 1)

    def test_many_to_one_and_extra_plan_fields_refuse(self):
        self.observe(self.short, "BLOCKED")
        plan = self.plan()
        plan["mappings"].append({**plan["mappings"][0], "from": "another"})
        with self.assertRaises(ValueError):
            migration.migrate(self.db, plan)
        plan = self.plan()
        plan["force"] = True
        with self.assertRaises(ValueError):
            migration.migrate(self.db, plan)


if __name__ == "__main__":
    unittest.main()
