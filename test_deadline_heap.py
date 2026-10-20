"""Deadline replacement, cancellation and event wake routing, with real state."""
from pathlib import Path
from types import SimpleNamespace

import pytest

from deadline_heap import DeadlineHeap
from chaski import Reactor, Rule


def rule(label='r', **extra):
    return Rule.from_dict(dict(label=label, triggerKind='schedule', schedule='PT1H',
        condition='SELECT ?focus WHERE { ?focus a <urn:test> }', severity='info',
        action=['alert'], owner='fixture', **extra))


def test_replace_cancel_and_same_deadline_never_replay_stale_entries():
    heap = DeadlineHeap()
    heap.set('a', 10)
    heap.set('b', 10)
    heap.set('a', 30)
    heap.cancel('b')
    assert list(heap.pop_due(10)) == [] and heap.peek() == 30
    assert list(heap.pop_due(30)) == [('a', 30)]
    assert list(heap.pop_due(30)) == [] and heap.peek() is None


def test_repeated_far_future_edit_and_cancel_has_bounded_storage():
    heap = DeadlineHeap()
    heap.set('first', 1)
    for n in range(10000):
        heap.set('edited', n + 100)
    assert len(heap._heap) <= 2 * len(heap) + 64
    heap.cancel('edited')
    assert list(heap.pop_due(20000)) == [('first', 1)]


@pytest.mark.parametrize('bad', [True, None, float('nan'), float('inf'), '10'])
def test_invalid_deadline_cannot_become_an_idle_success(bad):
    heap = DeadlineHeap()
    with pytest.raises(ValueError):
        heap.set('job', bad)


class Query:
    def __init__(self):
        self.reads = 0
        self.fail = False
    def select_focus(self, _):
        self.reads += 1
        if self.fail:
            raise RuntimeError('fixture unavailable')
        return {'urn:positive'}


def test_idle_ten_thousand_rules_do_not_rescan_or_evaluate(tmp_path):
    q = Query()
    reactor = Reactor(q, [rule(str(i)) for i in range(10000)], tmp_path/'cursor', 60)
    for state in reactor.states.values():
        state.last_attempt = 1000
    reactor.tick(1000)
    class NoIteration(dict):
        def values(self):
            pytest.fail('idle tick scanned the rule population')
    reactor._rules = NoIteration(reactor._rules)
    for now in range(1001, 1060):
        assert reactor.wait_timeout(now) == 4600-now
        reactor.tick(now)  # even an external spurious wake performs no rule scan
    assert q.reads == 0 and reactor.due_evaluations == 0
    assert len(reactor.deadlines) == 10000


def test_due_update_disable_and_unknown_preserve_state_without_replay(tmp_path):
    q = Query()
    reactor = Reactor(q, [rule()], tmp_path/'cursor', 60)
    reactor.tick(3600)
    assert q.reads == 1 and reactor.states['r'].firing == {'urn:positive'}
    reactor.tick(3600)
    assert q.reads == 1
    changed = rule()
    changed.interval_s = 30
    reactor.update_rule(changed)
    assert reactor.wait_timeout(3600) == 30
    q.fail = True
    reactor.tick(3630)
    assert q.reads == 2 and reactor.states['r'].firing == {'urn:positive'}
    assert reactor.states['r'].evaluations['unknown'] == 1
    changed.enabled = False
    reactor.update_rule(changed)
    assert reactor.wait_timeout(3630) is None
    reactor.tick(9000)
    assert q.reads == 2


def test_matching_stream_event_wakes_only_its_runner(tmp_path):
    turns = []
    reactor = Reactor(Query(), [], tmp_path/'cursor', 60)
    dirty = set()
    def take_dirty():
        out = set(dirty)
        dirty.clear()
        return out
    reactor.change_feed = SimpleNamespace(ready=True, take_dirty=take_dirty)
    reactor.change_stream = SimpleNamespace(pump=lambda feed: None)
    for label in ['a','b']:
        reactor.runners.append(SimpleNamespace(emitter=SimpleNamespace(label=label,change_driven=True),
            tick=lambda now, label=label: turns.append((label,now)),
            next_wakeup=lambda now: now+3600))
    reactor.tick(1000)
    turns.clear()
    dirty.add('b')
    reactor.tick(1001)
    assert turns == [('b',1001)]
    reactor.tick(1002)
    assert turns == [('b',1001)]


def test_empty_reactor_can_block_without_periodic_graph_read(tmp_path):
    q = Query()
    reactor = Reactor(q, [], Path(tmp_path)/'cursor', 60)
    reactor.tick(1000)
    assert reactor.wait_timeout(1000) is None and q.reads == 0
