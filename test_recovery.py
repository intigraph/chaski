"""Private recovery controls: persistence, exact prior identity and lost receipts."""
import copy
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import emitter as em
from alertmanager_sink import AlertmanagerSink, ChainSink
from graph_sink import NS, QuipuFiringSink, firing_iri


def rec(item, verdict, identity=None):
    return {"item": item, "verdict": verdict, "event_id": identity or "fire-" + item}


class Recovery(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.db = Path(self.scratch.name) / "state.db"
        self.conn = em.connect(self.db)
        self.addCleanup(lambda: self.conn.close())
        self.e = em.Emitter("control", [], 0, "owner", resolve_recovery=True)
        self.sink = em.JsonlSink(Path(self.scratch.name) / "received.jsonl")
        em.observe(self.conn, self.e, [rec("a", "BLOCKED"), rec("b", "BLOCKED")], 100)
        em.observe(self.conn, self.e, [rec("a", "UNBLOCKED"), rec("b", "UNBLOCKED")], 101)

    def pending(self):
        return self.conn.execute("SELECT payload FROM recovery_outbox").fetchall()

    def deliver(self):
        return em.deliver_pending(self.conn, self.sink, 102, self.e.label)

    def recover(self):
        return em.observe(self.conn, self.e, [rec("a", "BLOCKED"), rec("b", "UNBLOCKED")], 103)

    def test_exact_recovery_preserves_firing_and_still_firing_control(self):
        self.assertEqual(self.deliver(), (2, 0))
        prior = self.conn.execute("SELECT payload FROM outbox WHERE event_id='fire-a'").fetchone()[0]
        self.assertEqual(len(self.recover()), 1)
        event = json.loads(self.pending()[0][0])
        self.assertEqual(event["prior"], json.loads(prior))
        self.assertEqual(event["resolved_at"], 103)
        self.assertEqual(event["item"], "a")
        self.assertEqual(em.deliver_pending(self.conn, self.sink, 104, self.e.label, resolve_recovery=True), (1, 0))
        self.assertEqual(self.conn.execute("SELECT payload FROM outbox WHERE event_id='fire-a'").fetchone()[0], prior)
        self.assertEqual(self.conn.execute("SELECT verdict FROM verdicts WHERE item='b'").fetchone()[0], "UNBLOCKED")

    def test_recovery_restart_replay_retains_first_clock_and_one_identity(self):
        self.deliver()
        self.recover()
        before = self.pending()
        self.conn.close()
        self.conn = em.connect(self.db)
        em.observe(self.conn, self.e, [rec("a", "BLOCKED"), rec("b", "UNBLOCKED")], 200)
        self.assertEqual(self.pending(), before)

    def test_unknown_does_not_queue_or_untrack(self):
        self.deliver()
        em.observe(self.conn, self.e, [rec("a", "UNKNOWN"), rec("b", "UNBLOCKED")], 103)
        self.assertEqual(self.pending(), [])
        self.assertEqual(self.conn.execute("SELECT verdict,tracked FROM verdicts WHERE item='a'").fetchone(),
                         ("UNBLOCKED", 1))

    def test_partial_removal_only_recovers_requested_key(self):
        self.deliver()
        em.observe(self.conn, self.e, [], 103, scope={"a"})
        self.assertEqual([json.loads(r[0])["item"] for r in self.pending()], ["a"])
        self.assertEqual(self.conn.execute("SELECT tracked FROM verdicts WHERE item='b'").fetchone()[0], 1)

    def test_undelivered_and_indeterminate_retained_until_original_receipt(self):
        self.conn.execute("UPDATE outbox SET attempts=1,last_error='lost answer' WHERE event_id='fire-a'")
        self.recover()
        self.assertEqual(self.pending(), [])
        self.assertIsNone(self.conn.execute("SELECT delivered_at FROM outbox WHERE event_id='fire-a'").fetchone()[0])
        self.deliver()
        self.assertEqual(len(em.queue_recoveries(self.conn, self.e)), 1)
        self.assertEqual(json.loads(self.pending()[0][0])["resolved_at"], 103)

    def test_default_off_preserves_original_event_contract(self):
        self.deliver()
        self.e.resolve_recovery = False
        self.recover()
        self.assertEqual(self.pending(), [])

    def test_outer_transaction_rollback_includes_recovery_outbox(self):
        self.deliver()
        self.conn.execute("BEGIN IMMEDIATE")
        self.recover()
        self.conn.execute("ROLLBACK")
        self.assertEqual(self.pending(), [])
        self.assertEqual(self.conn.execute("SELECT verdict FROM verdicts WHERE item='a'").fetchone()[0], "UNBLOCKED")

    def test_fault_inside_observation_restores_verdict_and_outbox(self):
        self.deliver()
        with patch.object(em, "queue_recoveries", side_effect=RuntimeError("ordered fault")):
            with self.assertRaises(RuntimeError):
                self.recover()
        self.assertEqual(self.pending(), [])
        self.assertEqual(self.conn.execute("SELECT verdict FROM verdicts WHERE item='a'").fetchone()[0], "UNBLOCKED")

    def test_bounded_queue_drains_later_without_duplicate(self):
        self.deliver()
        with patch.object(em, "queue_recoveries", return_value=[]):
            em.observe(self.conn, self.e, [], 103)
        self.assertEqual(len(em.queue_recoveries(self.conn, self.e, limit=1)), 1)
        self.assertEqual(len(em.queue_recoveries(self.conn, self.e, limit=1)), 1)
        self.assertEqual(em.queue_recoveries(self.conn, self.e, limit=1), [])

    def test_prepare_failure_never_calls_a_resolution_write(self):
        self.deliver()
        self.recover()
        class Refused:
            def prepare_recovery(self, event):
                raise ConnectionError("ambiguous identity")
            def resolve(self, event):
                raise AssertionError("write after refusal")
        self.assertEqual(em.deliver_pending(self.conn, Refused(), 104, self.e.label, resolve_recovery=True), (0, 1))
        self.assertNotIn("receipt", json.loads(self.pending()[0][0]))

    def test_prepared_receipt_survives_lost_answer_and_restart(self):
        self.deliver()
        self.recover()
        calls = []
        class Lost:
            def prepare_recovery(self, event):
                calls.append("prepare")
                return {"identity": "old-route"}
            def resolve(self, event):
                calls.append(copy.deepcopy(event))
                if len(calls) == 2:
                    raise ConnectionError("write landed; answer lost")
        sink = Lost()
        self.assertEqual(em.deliver_pending(self.conn, sink, 104, self.e.label, resolve_recovery=True), (0, 1))
        self.conn.close()
        self.conn = em.connect(self.db)
        self.assertEqual(em.deliver_pending(self.conn, sink, 200, self.e.label, resolve_recovery=True), (1, 0))
        self.assertEqual(calls.count("prepare"), 1)
        self.assertEqual(calls[1], calls[2])

    def test_legacy_database_migration_preserves_rows_and_is_repeatable(self):
        path = Path(self.scratch.name) / "legacy.db"
        c = sqlite3.connect(path)
        c.executescript(em.SCHEMA)
        c.execute("INSERT INTO verdicts VALUES ('legacy','item','BLOCKED',0,NULL,1,NULL,100)")
        c.commit()
        c.close()
        for _ in range(2):
            c = em.connect(path)
            self.assertEqual(c.execute("SELECT verdict FROM verdicts").fetchone()[0], "BLOCKED")
            self.assertIn("prior_event", {r[1] for r in c.execute("PRAGMA table_info(recovery_outbox)")})
            c.close()


PRIOR = {"event_id": "prior-id", "emitter": "control", "item": "one", "observed_at": 100}
EVENT = {"event_id": "resolved-id", "prior": PRIOR, "resolved_at": 200}


class FakeAM(AlertmanagerSink):
    def __init__(self):
        super().__init__("http://localhost", {"alertname": "NEW", "severity": "warning"}, "control")
        self.alerts = [{"labels": {**{k: PRIOR[k] for k in ("event_id", "emitter", "item")},
                                   "alertname": "OLD", "keeper": "original"},
                        "startsAt": "1970-01-01T00:01:40Z"}]
        self.writes = []
    def _request(self, method, path, body=None):
        if method == "POST":
            self.writes.append(copy.deepcopy(body))
            self.alerts = []
        return 200, json.dumps(self.alerts).encode()


class FakeGraph(QuipuFiringSink):
    def __init__(self):
        super().__init__("http://localhost", None, {"label": "control"})
        self.rows = [{"p": NS + "eventId", "e": PRIOR["event_id"]}]
        self.ends = []
        self.writes = []
        self.lost = False
    def _post(self, path, body):
        if path == "/set":
            self.writes.append(copy.deepcopy(body))
            self.ends = [{"e": {"value": body["value"]["value"], "datatype": "xsd:dateTime"}}]
            if self.lost:
                self.lost = False
                raise ConnectionError("lost graph answer")
            return {"asserted": 0 if len(self.writes) > 1 else 1}
        return {"rows": self.rows if "?p ?e" in body["query"] else self.ends}


class SinkRecovery(unittest.TestCase):
    def test_alertmanager_uses_actual_prior_labels_not_current_policy(self):
        sink = FakeAM()
        old = copy.deepcopy(sink.alerts[0])
        receipt = sink.prepare_recovery(EVENT)
        sink.resolve({**EVENT, "receipt": receipt})
        self.assertEqual(sink.writes[0][0]["labels"], old["labels"])
        self.assertEqual(sink.writes[0][0]["startsAt"], old["startsAt"])
        self.assertEqual(sink.writes[0][0]["endsAt"], "1970-01-01T00:03:20Z")

    def test_ambiguous_label_sets_refused_without_post(self):
        sink = FakeAM()
        sink.alerts.append(copy.deepcopy(sink.alerts[0]))
        with self.assertRaises(ConnectionError):
            sink.prepare_recovery(EVENT)
        self.assertEqual(sink.writes, [])

    def test_empty_prior_is_no_post_and_checks_again(self):
        sink = FakeAM()
        sink.alerts = []
        receipt = sink.prepare_recovery(EVENT)
        sink.resolve({**EVENT, "receipt": receipt})
        self.assertEqual(sink.writes, [])
        sink.alerts = [{"labels": {k: PRIOR[k] for k in ("event_id", "emitter", "item")}}]
        with self.assertRaises(ConnectionError):
            sink.resolve({**EVENT, "receipt": receipt})

    def test_malformed_and_wrong_item_readback_refuse(self):
        for bad in [{}, [{"labels": {}}], [{"labels": {**PRIOR, "item": "other"}}]]:
            sink = FakeAM()
            sink.alerts = bad
            with self.assertRaises(ConnectionError):
                sink.prepare_recovery(EVENT)
            self.assertEqual(sink.writes, [])

    def test_graph_sets_only_exact_ended_predicate_and_retries_noop(self):
        sink = FakeGraph()
        event = {**EVENT, "receipt": sink.prepare_recovery(EVENT)}
        sink.resolve(event)
        sink.resolve(event)
        body = sink.writes[0]
        self.assertEqual(body, sink.writes[1])
        self.assertEqual(body["entity"], firing_iri("control", "prior-id"))
        self.assertEqual(body["predicate"], NS + "resolvedAt")
        self.assertEqual(body["value"]["datatype"], "http://www.w3.org/2001/XMLSchema#dateTime")
        self.assertEqual(body["source"], "chaski:recovery:resolved-id")

    def test_graph_lost_receipt_replays_exact_set(self):
        sink = FakeGraph()
        event = {**EVENT, "receipt": sink.prepare_recovery(EVENT)}
        sink.lost = True
        with self.assertRaises(ConnectionError):
            sink.resolve(event)
        sink.resolve(event)
        self.assertEqual(sink.writes[0], sink.writes[1])

    def test_graph_missing_ambiguous_or_wrong_event_refuses(self):
        for rows in [[], [{"p": NS + "eventId", "e": "wrong"}],
                     [{"p": NS + "eventId", "e": "prior-id"}] * 2]:
            sink = FakeGraph()
            sink.rows = rows
            with self.assertRaises(ConnectionError):
                sink.prepare_recovery(EVENT)
            self.assertEqual(sink.writes, [])

    def test_graph_preserves_existing_earlier_close(self):
        sink = FakeGraph()
        sink.rows.append({"p": NS + "resolvedAt", "e": {"value": "1970-01-01T00:02:30Z", "datatype": "xsd:dateTime"}})
        sink.ends = [{"e": {"value": "1970-01-01T00:02:30Z", "datatype": "xsd:dateTime"}}]
        sink.resolve({**EVENT, "receipt": sink.prepare_recovery(EVENT)})
        self.assertEqual(sink.writes, [])

    def test_chain_completes_all_identity_reads_before_any_write(self):
        graph, am = FakeGraph(), FakeAM()
        am.alerts.append(copy.deepcopy(am.alerts[0]))
        chain = ChainSink([graph, am])
        with self.assertRaises(ConnectionError):
            chain.prepare_recovery(EVENT)
        self.assertEqual(graph.writes, [])
        self.assertEqual(am.writes, [])

class RecoveryConfiguration(unittest.TestCase):
    def test_opt_in_is_boolean_and_never_changes_command_or_external_contract(self):
        import chaski
        import yaml
        base = {"label": "r", "command": ["true"], "schedule": "PT1M", "owner": "owner",
                "sink": {"jsonl": "private.jsonl"}}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "emitters.yml"
            for value in ["true", 1, None]:
                path.write_text(yaml.safe_dump({"emitters": [{**base, "resolve_recovery": value}]}))
                with self.assertRaises(chaski.RuleError):
                    chaski.load_emitters(path)
            path.write_text(yaml.safe_dump({"emitters": [{**base, "resolve_recovery": True}]}))
            self.assertTrue(chaski.load_emitters(path)[0][0].resolve_recovery)
            path.write_text(yaml.safe_dump({"emitters": [{**base, "resolve_recovery": True,
                                                         "trigger": "external", "baseline_emits": True}]}))
            with self.assertRaises(chaski.RuleError):
                chaski.load_emitters(path)

class TypedGraphReceipt(unittest.TestCase):
    def test_plain_or_wrong_datatype_is_unknown_not_a_confirmed_close(self):
        for value in ['1970-01-01T00:03:20Z',
                      {'value': '1970-01-01T00:03:20Z', 'datatype': 'xsd:string'},
                      {'value': '1970-01-01T00:03:20', 'datatype': 'xsd:dateTime'}]:
            sink = FakeGraph()
            event = {**EVENT, 'receipt': sink.prepare_recovery(EVENT)}
            sink.rows = []
            original = sink._post
            def changed(path, body):
                out = original(path, body)
                if path == '/query':
                    return {'rows': [{'e': value}]}
                return out
            sink._post = changed
            with self.assertRaises(ConnectionError):
                sink.resolve(event)

class BoundedRecovery(unittest.TestCase):
    setUp = Recovery.setUp
    pending = Recovery.pending
    deliver = Recovery.deliver
    recover = Recovery.recover
    def test_invalid_limits_cannot_turn_into_an_unbounded_sql_sweep(self):
        self.deliver()
        for value in [-1, 0, 6, True, 1.5]:
            with self.assertRaises(em.ProtocolError):
                em.queue_recoveries(self.conn, self.e, value)
        self.assertEqual(self.pending(), [])

    def test_collision_refuses_and_rolls_back_original_reservation(self):
        import hashlib
        self.deliver()
        identity = json.dumps([self.e.label, 'fire-a'], separators=(',', ':'))
        event_id = 'resolved-' + hashlib.sha256(identity.encode()).hexdigest()
        self.conn.execute('INSERT INTO recovery_outbox (event_id,emitter,item,generation,payload,created,prior_event)'
                          ' VALUES (?,?,?,?,?,?,?)', (event_id, 'other', 'other', 0, '{}', 102, 'other'))
        with self.assertRaises(em.ProtocolError):
            self.recover()
        self.assertEqual(self.conn.execute("SELECT verdict,recovery_queued FROM verdicts v JOIN outbox o"
                                           " ON v.item=o.item WHERE o.event_id='fire-a'").fetchone(), ('UNBLOCKED', 0))

    def test_partial_scope_never_resolves_a_legacy_untracked_item_outside_it(self):
        self.deliver()
        self.conn.execute("UPDATE verdicts SET tracked=0 WHERE item='a'")
        em.observe(self.conn, self.e, [], 103, scope={'b'})
        self.assertEqual([json.loads(r[0])['item'] for r in self.pending()], ['b'])

class QueueCompatibility(unittest.TestCase):
    setUp = Recovery.setUp
    pending = Recovery.pending
    deliver = Recovery.deliver
    recover = Recovery.recover

    def test_default_reader_never_treats_recovery_as_firing(self):
        self.deliver()
        self.recover()
        self.assertEqual(self.conn.execute('SELECT count(*) FROM outbox WHERE delivered_at IS NULL').fetchone()[0], 0)
        class NoSend:
            def deliver(self, event):
                raise AssertionError('recovery reached a legacy firing reader')
        self.assertEqual(em.deliver_pending(self.conn, NoSend(), 104, self.e.label), (0, 0))
        self.assertEqual(len(self.pending()), 1)

    def test_pending_recovery_is_visible_to_existing_outbox_health_metrics(self):
        self.deliver()
        self.recover()
        runner = em.Runner(self.e, self.conn, self.sink)
        runner._publish()
        self.assertIn('chaski_emitter_outbox_pending{emitter="control"} 1', runner.metrics(str))

    def test_command_sink_cannot_be_opted_into_remission(self):
        import chaski
        import yaml
        config = {'emitters': [{'label': 'r', 'command': ['true'], 'schedule': 'PT1M', 'owner': 'owner',
                                'resolve_recovery': True,
                                'sink': {'graph': {'severity': 'warning', 'action': ['alert'],
                                                   'condition': 'condition', 'comment': 'comment'},
                                         'command': {'argv': ['true']}}}]}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'emitters.yml'
            path.write_text(yaml.safe_dump(config))
            with self.assertRaises(chaski.RuleError):
                chaski.load_emitters(path)

class LegacyReconcile(unittest.TestCase):
    setUp = Recovery.setUp
    pending = Recovery.pending
    deliver = Recovery.deliver

    def runner(self):
        import incremental
        self.conn.executescript(incremental.SCHEMA)
        runner = object.__new__(incremental.ChangeRunner)
        runner.conn, runner.emitter = self.conn, self.e
        runner.subscription = {'version': 1, 'graphs': [], 'attributes': [], 'types': []}
        return runner

    def test_enabling_recovery_invalidates_daily_discovery_without_changing_off_digest(self):
        import hashlib
        runner = self.runner()
        self.e.resolve_recovery = False
        runner._reconcile_on_new_subscription(self.conn, self.e.label)
        old_digest = hashlib.sha256(json.dumps(runner.subscription, sort_keys=True).encode()).hexdigest()
        self.assertEqual(self.conn.execute('SELECT digest FROM adapter_subscription').fetchone()[0], old_digest)
        self.conn.execute('INSERT INTO adapter_reconcile VALUES (?,?)', (self.e.label, 100))
        self.e.resolve_recovery = True
        runner._reconcile_on_new_subscription(self.conn, self.e.label)
        self.assertEqual(self.conn.execute('SELECT count(*) FROM adapter_reconcile').fetchone()[0], 0)
        self.assertNotEqual(self.conn.execute('SELECT digest FROM adapter_subscription').fetchone()[0], old_digest)

    def test_fresh_complete_discovery_can_recover_preupgrade_untracked_identity(self):
        self.deliver()
        self.conn.execute("UPDATE verdicts SET tracked=0 WHERE item='a'")
        runner = self.runner()
        with patch('incremental.call', return_value={'version': 1, 'items': ['b']}):
            runner.reconcile(103)
        self.assertEqual([json.loads(r[0])['item'] for r in self.pending()], ['a'])
        self.assertEqual(self.conn.execute("SELECT verdict,tracked FROM verdicts WHERE item='b'").fetchone(),
                         ('UNBLOCKED', 1))

    def test_unknown_discovery_does_not_close_a_legacy_untracked_identity(self):
        self.deliver()
        self.conn.execute("UPDATE verdicts SET tracked=0 WHERE item='a'")
        runner = self.runner()
        with patch('incremental.call', side_effect=em.ProtocolError('discovery UNKNOWN')):
            with self.assertRaises(em.ProtocolError):
                runner.reconcile(103)
        self.assertEqual(self.pending(), [])
        self.assertIsNone(self.conn.execute("SELECT recovery_at FROM verdicts WHERE item='a'").fetchone()[0])
