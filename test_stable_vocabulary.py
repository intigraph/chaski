"""Canonical writer terms and exact historical graph receipts, without HTTP."""
import unittest
import json
from pathlib import Path
from unittest import mock

import graph_sink as gs
import github_routes
import scheduled_catalogue_adapter as catalogue
from types import SimpleNamespace

OLD = 'http://aegis.gastown.local/ontology/'
CORE = 'https://w3id.org/intigraph/camayoc/core/ns#'
QP = 'https://w3id.org/intigraph/quipu/ns#'
REACTION = {'label': 'fixture', 'condition': 'ASK {}', 'schedule': 'PT1M',
            'severity': 'info', 'action': ['notice'], 'owner': 'fixture', 'comment': 'fixture'}
PRIOR = {'emitter': 'fixture', 'event_id': 'event-1'}


class StableVocabulary(unittest.TestCase):
    def sink(self, ns=OLD):
        return gs.QuipuFiringSink('https://unused.example.test', None, REACTION, ns=ns)

    def test_writer_terms_match_the_published_crosswalk_and_unknown_refuses(self):
        rows = json.loads((Path(__file__).parent / 'tests/fixtures/chaski-ownership.json').read_text())
        self.assertEqual({gs.NS + name: iri for name, iri in gs._VOCABULARY.items()},
                         {row['old']: row['new'] for row in rows})
        with self.assertRaises(KeyError):
            gs.term('UnpublishedTerm')

    def test_mapped_terms_are_independent_of_the_instance_namespace(self):
        sink = self.sink('urn:fixture:')
        reaction = sink.reaction_turtle()
        firing = sink.firing_turtle({'event_id': 'event-1', 'item': 'https://example.test/focus',
                                     'observed_at': 1790400000})
        self.assertIn('<urn:fixture:reaction-fixture>', reaction)
        self.assertIn('<'+QP+'Reaction>', reaction)
        self.assertIn('<'+QP+'ReactionFiring>', firing)
        self.assertIn('<'+CORE+'eventId>', firing)
        self.assertNotIn(OLD, reaction+firing)
        self.assertNotIn('scbrown.github.io/quechua', reaction+firing)

    def test_new_recovery_of_old_firing_writes_canonical_resolved_at(self):
        sink = self.sink()
        with mock.patch.object(sink, '_post', return_value={
                'rows': [{'p': OLD+'eventId', 'e': PRIOR['event_id']}]}):
            receipt = sink.prepare_recovery({'prior': PRIOR, 'resolved_at': 1790400100})
        self.assertEqual(receipt['predicate'], CORE+'resolvedAt')

    def test_duplicate_canonical_and_legacy_event_identity_refuses(self):
        sink = self.sink()
        with mock.patch.object(sink, '_post', return_value={'rows': [
                {'p': OLD+'eventId', 'e': PRIOR['event_id']},
                {'p': CORE+'eventId', 'e': PRIOR['event_id']}]}):
            with self.assertRaises(ConnectionError):
                sink.prepare_recovery({'prior': PRIOR, 'resolved_at': 1790400100})

    def test_completed_old_recovery_is_read_only_and_keeps_its_exact_predicate(self):
        sink = self.sink()
        ended = {'value': '2026-09-26T05:20:00Z',
                 'datatype': 'http://www.w3.org/2001/XMLSchema#dateTime'}
        with mock.patch.object(sink, '_post', return_value={'rows': [
                {'p': OLD+'eventId', 'e': PRIOR['event_id']},
                {'p': OLD+'resolvedAt', 'e': ended}]}):
            receipt = sink.prepare_recovery({'prior': PRIOR, 'resolved_at': 1790400100})
        with mock.patch.object(sink, '_post', return_value={'rows': [{'e': ended}]}) as post:
            sink.resolve({'receipt': receipt, 'resolved_at': 1790400100, 'event_id': 'end-1'})
        self.assertEqual(receipt['predicate'], OLD+'resolvedAt')
        self.assertEqual([call.args[0] for call in post.call_args_list], ['/query'])

    def test_delivery_scope_records_the_vocabulary_boundary(self):
        self.assertEqual(self.sink().delivery_scope()['vocabulary'], 'intigraph-owned-a389be53/v1')

    def test_truncated_or_error_answers_do_not_establish_exact_recovery(self):
        sink = self.sink()
        for extra in ({'truncated': True}, {'error': 'partial result'}):
            with self.subTest(extra=extra), mock.patch.object(sink, '_post', return_value={
                    'rows': [{'p': OLD+'eventId', 'e': PRIOR['event_id']}], **extra}):
                with self.assertRaises(ConnectionError):
                    sink.prepare_recovery({'prior': PRIOR, 'resolved_at': 1790400100})

    def test_duplicate_event_readback_does_not_acknowledge_delivery(self):
        sink = self.sink()
        with mock.patch.object(sink, '_post', return_value={'rows': [
                {'e': PRIOR['event_id']}, {'e': PRIOR['event_id']}]}):
            self.assertFalse(sink._received(PRIOR))

    def test_github_readers_name_both_exact_spellings_for_every_mapped_term(self):
        queries = ' '.join((github_routes.activity_query('https://example.test/graph', 'owner/repo', '2026'),
                            github_routes.verdict_query('owner/repo'),
                            github_routes.author_query('https://example.test/graph', 'owner/repo')))
        for name, canonical in github_routes._READ_TERMS.items():
            self.assertIn('<'+canonical+'>', queries)
            self.assertIn('<'+OLD+name+'>', queries)
        self.assertIn('UNION', queries)
        self.assertIn('<'+OLD+'GitHubActivity>', queries)  # unmapped vocabulary stays read-only legacy

    def test_catalogue_routes_old_and_published_quechua_changes_without_a_read(self):
        projection = SimpleNamespace(graph='https://example.test/jobs',
                                     scope=SimpleNamespace(job='urn:fixture:job'))
        for base in (catalogue.QUECHUA, catalogue.LEGACY_QUECHUA):
            request = {'now': 1, 'route': [{'graph': projection.graph,
                       'attribute': base+'enabled', 'entity': projection.scope.job}]}
            self.assertEqual(catalogue.changes(projection, None, request)['items'], [projection.scope.job])


if __name__ == '__main__':
    unittest.main()
