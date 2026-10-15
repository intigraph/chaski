"""Bridge sequencing and replay controls; the signing port is a test double."""
import base64
from dataclasses import asdict
import json
import tempfile
from pathlib import Path
import unittest

from scheduled_receiver import ExecutionScope, receive, run_id
from slot_receiver import SlotLedger, SlotUnknown


class Port:
    def __init__(self):
        self.calls = []
        self.fail = None

    def record(self, name):
        self.calls.append(name)
        if self.fail == name:
            raise RuntimeError(name)

    def admit(self, scope, slot):
        self.record('admit')

    def started(self, run, scope):
        self.record('started')

    def finished(self, run, scope, outcome):
        self.record('finished:' + outcome['outcome'])

    def publish(self, run, scope, outcome):
        self.record('publish')


class ReceiverTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.ledger = SlotLedger(Path(self.temp.name) / 'ledger.db')
        self.scope = ExecutionScope('urn:example:job', 'rev1', 'host-a', 'a'*64,
                                    'fixture', 'urn:example:workflow')
        self.slot = 120
        self.delivery = {**asdict(self.scope), 'scheduled_at': self.slot}
        self.port = Port()
        self.executions = 0

    def execute(self):
        self.port.record('execute')
        self.executions += 1
        return {'outcome': 'succeeded', 'exit_code': 0}

    def receive(self, execute=None):
        return receive(self.ledger, self.scope, self.slot, self.delivery,
                       self.port, execute or self.execute)

    def test_order_and_duplicate_publication_without_duplicate_execution(self):
        first = self.receive()
        self.assertEqual(['admit', 'started', 'execute', 'finished:succeeded', 'publish'], self.port.calls)
        self.assertEqual(first, self.receive())
        self.assertEqual(1, self.executions)
        self.assertEqual(['admit', 'publish'], self.port.calls[-2:])

    def test_lost_publication_reply_retries_only_publication(self):
        self.port.fail = 'publish'
        with self.assertRaises(RuntimeError):
            self.receive()
        self.port.fail = None
        self.assertEqual('succeeded', self.receive()['outcome'])
        self.assertEqual(1, self.executions)
        self.assertEqual(1, self.port.calls.count('started'))

    def test_failed_started_cannot_execute_or_automatically_retry(self):
        self.port.fail = 'started'
        with self.assertRaises(RuntimeError):
            self.receive()
        self.port.fail = None
        with self.assertRaises(SlotUnknown):
            self.receive()
        self.assertEqual(0, self.executions)

    def test_failed_terminal_signing_does_not_repeat_child(self):
        self.port.fail = 'finished:succeeded'
        with self.assertRaises(RuntimeError):
            self.receive()
        self.port.fail = None
        with self.assertRaises(SlotUnknown):
            self.receive()
        self.assertEqual(1, self.executions)
        self.assertNotIn('publish', self.port.calls)

    def test_admission_refusal_precedes_claim_and_work(self):
        self.port.fail = 'admit'
        with self.assertRaises(RuntimeError):
            self.receive()
        self.assertEqual(['admit'], self.port.calls)
        self.port.fail = None
        self.receive()  # refusal never created a claim
        self.assertEqual(1, self.executions)

    def test_every_delivery_binding_field_is_checked_before_admission(self):
        for field in self.delivery:
            with self.subTest(field=field):
                delivery = dict(self.delivery)
                delivery[field] = 'wrong'
                with self.assertRaises(ValueError):
                    receive(self.ledger, self.scope, self.slot, delivery, self.port, self.execute)
        self.assertEqual([], self.port.calls)
        with self.assertRaises(ValueError):
            receive(self.ledger, self.scope, 1, {**asdict(self.scope), 'scheduled_at': True},
                    self.port, self.execute)

    def test_scope_is_recoverable_from_signed_run_name(self):
        run = run_id(self.scope, self.slot)
        encoded = run.removeprefix('scheduled-v1-')
        raw = base64.urlsafe_b64decode(encoded + '='*(-len(encoded) % 4))
        self.assertEqual({**self.delivery, 'version': 1}, json.loads(raw))
        self.assertNotIn('|', run)
        self.assertNotEqual(run, run_id(self.scope, self.slot + 60))

    def test_actual_hold_and_failure_are_distinct_terminal_results(self):
        for name, code in [('skipped-by-hold', 0), ('failed', 7)]:
            with self.subTest(name=name):
                self.slot += 60
                self.delivery['scheduled_at'] = self.slot
                result = self.receive(lambda: {'outcome': name, 'exit_code': code})
                self.assertEqual(name, result['outcome'])
                self.assertIn('finished:' + name, self.port.calls)

    def test_invalid_child_result_stays_unknown_without_publication(self):
        with self.assertRaises(ValueError):
            self.receive(lambda: {'outcome': 'succeeded', 'exit_code': 7})
        with self.assertRaises(SlotUnknown):
            self.receive()
        self.assertNotIn('publish', self.port.calls)

    def test_child_exception_never_authorizes_retry(self):
        def fail():
            self.executions += 1
            raise RuntimeError('child outcome unknown')
        with self.assertRaises(RuntimeError):
            self.receive(fail)
        with self.assertRaises(SlotUnknown):
            self.receive()
        self.assertEqual(1, self.executions)
