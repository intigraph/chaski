"""Delivery invariants for changed subsets, restart recovery and time deadlines."""
import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

import incremental as inc
from emitter import Emitter, ProtocolError, connect, observe


class Sink:
    def deliver(self, event):
        pass


def record(item, verdict="BLOCKED"):
    return {"item": item, "verdict": verdict, "event_id": "event-" + item}


@pytest.fixture
def setup(tmp_path, monkeypatch):
    conn = connect(tmp_path / "state.db")
    calls = []
    def adapter(emitter, request=None):
        calls.append(request)
        if request is None:
            return {"version": 1, "attributes": ["status"], "graphs": ["ROOT"]}
        if "route" in request:
            return {"items": sorted({c["entity"] for c in request["route"]})}
        if request.get("discover"):
            return {"items": ["a", "b"]}
        scope = set(request["items"]) | {c["entity"] for c in request["changes"]}
        return {"scope": list(scope), "records": [record(i) for i in scope], "next_checks": {i: None for i in scope}}
    monkeypatch.setattr(inc, "call", adapter)
    e = Emitter("e", ["unused"], 86400, "owner", change_driven=True)
    runner = inc.ChangeRunner(e, conn, Sink())
    yield conn, e, runner, calls
    conn.close()


def change(tx=2, entity="a", graph="ROOT", attr="status"):
    return {"tx": tx, "sequence": 0, "entity": entity, "attribute": attr,
            "graph": graph, "op": "assert", "value": "closed"}


def test_partial_run_does_not_untrack_other_items(setup):
    conn, e, _, _ = setup
    observe(conn, e, [record("a"), record("b")], 1)
    observe(conn, e, [], 2, scope={"a"})
    assert dict(conn.execute("SELECT item,tracked FROM verdicts")) == {"a": 0, "b": 1}
    with pytest.raises(ProtocolError):
        observe(conn, e, [record("c")], 3, scope={"a"})


def test_restart_and_idle_make_no_adapter_reads(setup):
    conn, e, runner, calls = setup
    runner.tick(100)
    assert calls[-1]["items"] == ["a", "b"]
    calls.clear()
    for now in range(105, 900, 5):
        runner.tick(now)
    assert calls == []
    restarted = inc.ChangeRunner(e, conn, Sink())
    calls.clear()  # --describe is local metadata, not a graph read
    restarted.tick(1000)
    assert calls == []


def test_deadlines_survive_restart(setup):
    conn, e, runner, calls = setup
    runner.tick(100)
    conn.execute("INSERT INTO adapter_deadlines (emitter,item,due) VALUES ('e','a',500)")
    restarted = inc.ChangeRunner(e, conn, Sink())
    calls.clear()
    restarted.tick(499)
    assert not calls
    restarted.tick(500)
    assert calls[-1]["items"] == ["a"]
    assert conn.execute("SELECT COUNT(*) FROM adapter_deadlines").fetchone()[0] == 0


def test_failure_keeps_pending_then_atomic_retry(setup, monkeypatch):
    conn, e, runner, calls = setup
    runner.tick(100)
    conn.execute("INSERT INTO change_inbox VALUES ('e',2,0,?)", (json.dumps(change()),))
    original = inc.observe
    def failure(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("crash before inbox acknowledgement")
    monkeypatch.setattr(inc, "observe", failure)
    runner.tick(110)
    assert conn.execute("SELECT COUNT(*) FROM adapter_deadlines WHERE item='a'").fetchone()[0] == 1
    assert conn.execute("SELECT updated FROM verdicts WHERE item='a'").fetchone()[0] == 100
    monkeypatch.setattr(inc, "observe", original)
    runner.tick(170)
    assert conn.execute("SELECT COUNT(*) FROM change_inbox").fetchone()[0] == 0
    assert conn.execute("SELECT updated FROM verdicts WHERE item='a'").fetchone()[0] == 170


class Feed:
    def __init__(self, pages):
        self.pages = iter(pages)
        self.since = []
    def _request(self, method, path):
        self.since.append(int(parse_qs(urlparse(path).query)["since"][0]))
        return next(self.pages)


def test_tail_before_discovery_and_commit_inbox_with_cursor(setup):
    conn, _, runner, _ = setup
    q = Feed([{"records": [], "next_tx": 999, "watermark_tx": 10},
              {"records": [change(11), change(12, graph="inferred")], "next_tx": 12, "watermark_tx": 12}])
    feed = inc.ChangeFeed(q, [runner])
    feed.poll()
    assert conn.execute("SELECT tx FROM change_cursor").fetchone()[0] == 10
    feed.poll()
    assert q.since == [9223372036854775807, 10]
    assert conn.execute("SELECT tx FROM change_cursor").fetchone()[0] == 12
    assert conn.execute("SELECT tx FROM change_inbox").fetchall() == [(11,)]


def test_bad_page_cannot_advance_cursor(setup):
    conn, _, runner, _ = setup
    conn.execute("INSERT INTO change_cursor VALUES (1,10)")
    feed = inc.ChangeFeed(Feed([{"records": [change(12)], "next_tx": 11, "watermark_tx": 12}]), [runner])
    feed.poll()
    assert feed.errors == 1
    assert conn.execute("SELECT tx FROM change_cursor").fetchone()[0] == 10
    assert conn.execute("SELECT COUNT(*) FROM change_inbox").fetchone()[0] == 0


def test_change_queue_preempts_daily_catalogue(setup):
    conn, _, runner, calls = setup
    runner.reconcile(100)
    conn.executemany("INSERT INTO adapter_deadlines (emitter,item,due,priority) VALUES ('e',?,100,2)",
                     [(f"old-{i}",) for i in range(20)])
    conn.execute("INSERT INTO change_inbox VALUES ('e',2,0,?)", (json.dumps(change(entity="urgent")),))
    runner.tick(110)
    assert "urgent" in calls[-1]["items"]
    assert len(calls[-1]["items"]) == inc.BATCH
    assert conn.execute("SELECT COUNT(*) FROM adapter_deadlines").fetchone()[0] > 0


def test_routing_failure_keeps_fact_changes(setup, monkeypatch):
    conn, _, runner, _ = setup
    runner.tick(100)
    conn.execute("INSERT INTO change_inbox VALUES ('e',2,0,?)", (json.dumps(change()),))
    def failure(*args, **kwargs):
        raise RuntimeError("reader unavailable")
    monkeypatch.setattr(inc, "call", failure)
    runner.tick(110)
    assert conn.execute("SELECT COUNT(*) FROM change_inbox").fetchone()[0] == 1


def test_database_refusal_rolls_back_cursor_and_inbox(setup):
    conn, _, runner, _ = setup
    conn.execute("INSERT INTO change_cursor VALUES (1,10)")
    conn.executescript("CREATE TRIGGER refuse_cursor BEFORE INSERT ON change_cursor "
                       "BEGIN SELECT RAISE(ABORT,'disk failure'); END;")
    feed = inc.ChangeFeed(Feed([{"records": [change(11)], "next_tx": 11, "watermark_tx": 11}]), [runner])
    feed.poll()
    assert feed.errors == 1
    assert conn.execute("SELECT tx FROM change_cursor").fetchone()[0] == 10
    assert conn.execute("SELECT COUNT(*) FROM change_inbox").fetchone()[0] == 0


def test_factless_transactions_do_not_create_permanent_lag(setup):
    conn, _, runner, _ = setup
    conn.execute("INSERT INTO change_cursor VALUES (1,10)")
    feed = inc.ChangeFeed(Feed([{"records": [], "next_tx": 10, "watermark_tx": 12}]), [runner])
    feed.poll()
    assert feed.lag == 0
    assert conn.execute("SELECT tx FROM change_cursor").fetchone()[0] == 12


def test_a_changed_subscription_reconciles_at_once_and_an_unchanged_one_does_not(setup, monkeypatch):
    """aegis-z1epad: an attribute added to the subscription was missed for most
    of a day, because the old adapter consumed its changes."""
    conn, e, runner, calls = setup
    runner.tick(100)                                  # first reconcile, at t=100
    calls.clear()
    inc.ChangeRunner(e, conn, Sink()).tick(200)       # same subscription: no rediscovery
    assert not any(c and c.get("discover") for c in calls)
    real = inc.call

    def widened(emitter, request=None):
        if request is None:
            return {"version": 1, "attributes": ["status", "idleLimit"], "graphs": ["ROOT"]}
        return real(emitter, request)
    monkeypatch.setattr(inc, "call", widened)
    calls.clear()
    inc.ChangeRunner(e, conn, Sink()).tick(300)       # widened: rediscover now, not at t=86500
    assert any(c and c.get("discover") for c in calls)
    calls.clear()
    inc.ChangeRunner(e, conn, Sink()).tick(400)       # and only once
    assert not any(c and c.get("discover") for c in calls)


def test_stream_named_graph_delivery_commits_inbox_before_resume(setup):
    conn, _, runner, _ = setup
    runner.graphs = {"urn:tests:work-board"}
    conn.execute("INSERT INTO change_cursor VALUES (1,10)")
    inc.bind_stream_source(conn, {"store_id": "fixture", "log_epoch": "one"}, 10)
    feed = inc.ChangeFeed(Feed([]), [runner])
    page = {"records": [change(11, graph="urn:tests:work-board")],
            "next_tx": 11, "watermark_tx": 12, "source": {"store_id": "fixture", "log_epoch": "one"}}
    feed.accept_page(page, 10, stream_id=11)
    assert conn.execute("SELECT tx FROM change_cursor").fetchone()[0] == 11
    assert conn.execute("SELECT tx FROM change_inbox").fetchall() == [(11,)]
    with pytest.raises(ProtocolError):
        feed.accept_page(page, 10, stream_id=11)
    assert conn.execute("SELECT COUNT(*) FROM change_inbox").fetchone()[0] == 1


def test_stream_failure_never_acknowledges_cursor(setup):
    conn, _, runner, _ = setup
    conn.execute("INSERT INTO change_cursor VALUES (1,10)")
    inc.bind_stream_source(conn, {"store_id": "fixture", "log_epoch": "one"}, 10)
    conn.execute("CREATE TRIGGER refuse_delivery BEFORE INSERT ON change_inbox "
                 "BEGIN SELECT RAISE(ABORT,'fixture'); END")
    feed = inc.ChangeFeed(Feed([]), [runner])
    page = {"records": [change(11)], "next_tx": 11, "watermark_tx": 11,
            "source": {"store_id": "fixture", "log_epoch": "one"}}
    with pytest.raises(Exception, match="fixture"):
        feed.accept_page(page, 10, stream_id=11)
    assert conn.execute("SELECT tx FROM change_cursor").fetchone()[0] == 10
    assert conn.execute("SELECT COUNT(*) FROM change_inbox").fetchone()[0] == 0
    conn.execute("DROP TRIGGER refuse_delivery")
    feed.accept_page(page, 10, stream_id=11)
    assert conn.execute("SELECT tx FROM change_cursor").fetchone()[0] == 11


@pytest.mark.parametrize("field,value", [("next_tx",True), ("next_tx",-1), ("watermark_tx",True), ("watermark_tx",9)])
def test_stream_invalid_prefix_keeps_cursor(setup, field, value):
    conn, _, runner, _ = setup
    conn.execute("INSERT INTO change_cursor VALUES (1,10)")
    inc.bind_stream_source(conn, {"store_id": "fixture", "log_epoch": "one"}, 10)
    feed = inc.ChangeFeed(Feed([]), [runner])
    page = {"records": [change(11)], "next_tx": 11, "watermark_tx": 11,
            "source": {"store_id": "fixture", "log_epoch": "one"}}
    page[field] = value
    with pytest.raises(ProtocolError):
        feed.accept_page(page, 10, stream_id=11)
    assert conn.execute("SELECT tx FROM change_cursor").fetchone()[0] == 10
    assert conn.execute("SELECT COUNT(*) FROM change_inbox").fetchone()[0] == 0


def test_real_http_stream_reconnect_uses_only_committed_named_graph_cursor(setup):
    import http.server
    import threading
    import time
    from types import SimpleNamespace
    from change_stream import ChangeStream

    conn, _, runner, _ = setup
    runner.graphs = {"urn:tests:work-board"}
    conn.execute("INSERT INTO change_cursor VALUES (1,10)")
    inc.bind_stream_source(conn, {"store_id": "fixture", "log_epoch": "one"}, 10)
    observed = []
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            since = int(parse_qs(urlparse(self.path).query)["since"][0])
            observed.append((since, self.headers.get("Last-Event-ID")))
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            if since == 10:
                page = {"records": [change(11, graph="urn:tests:work-board")],
                        "next_tx": 11, "watermark_tx": 11, "source": {"store_id": "fixture", "log_epoch": "one"}}
                self.wfile.write(('event: quipu.changes\nid: 11\ndata: '+json.dumps(page)+'\n\n').encode())
                self.wfile.flush()
        def log_message(self, *args):
            pass
    server = http.server.HTTPServer(('127.0.0.1',0),Handler)
    thread = threading.Thread(target=server.serve_forever,daemon=True)
    thread.start()
    feed = inc.ChangeFeed(Feed([]),[runner])
    stream = ChangeStream(SimpleNamespace(base=f'http://127.0.0.1:{server.server_port}'),10)
    try:
        deadline = time.monotonic()+4
        while time.monotonic()<deadline and not any(c==11 for c,_ in observed):
            stream.pump(feed)
            time.sleep(.01)
        assert observed[:2] == [(10,'10'),(11,'11')]
        assert conn.execute("SELECT tx FROM change_cursor").fetchone()[0] == 11
        assert conn.execute("SELECT COUNT(*) FROM change_inbox").fetchone()[0] == 1
    finally:
        stream.close()
        server.shutdown()
        thread.join()
        server.server_close()
        stream.thread.join(timeout=2)
    assert not stream.thread.is_alive()


def test_reactor_backlog_drains_event_woken_bounded_turns(setup, tmp_path, monkeypatch):
    import queue
    import threading
    from types import SimpleNamespace
    import change_stream
    from chaski import Reactor
    from change_stream import ChangeStream

    conn, _, runner, _ = setup
    runner.graphs = {"urn:tests:work-board"}
    conn.execute("INSERT INTO change_cursor VALUES (1,0)")
    inc.bind_stream_source(conn, {"store_id": "fixture", "log_epoch": "one"}, 0)
    feed = inc.ChangeFeed(Feed([]), [runner])
    stream = object.__new__(ChangeStream)
    stream.cursor = 0
    stream.received = 0
    stream.logged_first_commit = False
    stream.pending = queue.Queue(maxsize=1)
    stream.wakeup = threading.Event()

    # A controlled sender enqueues the next page only after durable ACK. This
    # pins page-budget yields without depending on OS thread scheduling within
    # 50ms. The separate clocked control proves the production time-budget yield;
    # real HTTP controls prove asynchronous delivery/reconnect.
    monkeypatch.setattr(change_stream.time, 'monotonic', lambda: 0.0)
    class Ack(threading.Event):
        def __init__(self, tx):
            super().__init__()
            self.tx = tx
        def set(self):
            super().set()
            assert conn.execute("SELECT tx FROM change_cursor").fetchone()[0] == self.tx
            if self.tx < 13:
                enqueue(self.tx + 1)
    def enqueue(tx):
        graph = "urn:tests:work-board" if tx == 13 else "urn:irrelevant"
        stream.pending.put_nowait((tx, {'records': [change(tx, graph=graph)],
                                       'next_tx': tx, 'watermark_tx': 13,
                                       'source': {'store_id': 'fixture', 'log_epoch': 'one'}}, Ack(tx)))
        stream.wakeup.set()
    enqueue(1)
    original_pump = stream.pump
    stream.pump = lambda incoming: original_pump(incoming, max_pages=3)
    reactor = Reactor(SimpleNamespace(), [], tmp_path / 'cursor.json', 60)
    reactor.change_feed, reactor.change_stream = feed, stream
    adapter_turns = []
    reactor.runners = [SimpleNamespace(emitter=SimpleNamespace(change_driven=True, label='e'),
                                      tick=lambda now: adapter_turns.append((stream.cursor, now)),
                                      next_wakeup=lambda now: now + 86400)]
    for now in range(60, 65):
        assert stream.wait(0), "remainder must wake the next turn without a five-second sleep"
        reactor.tick(now)
    # Startup plus the matching event; irrelevant pages do not evaluate adapters.
    assert adapter_turns == [(3, 60), (13, 64)]
    assert conn.execute("SELECT tx FROM change_cursor").fetchone()[0] == 13
    assert conn.execute("SELECT tx FROM change_inbox").fetchall() == [(13,)]
    assert stream.pending.empty() and stream.pending.maxsize == 1


def test_old_inbox_adopts_entity_index_without_rewriting_state(setup):
    conn, e, _, _ = setup
    conn.execute("DROP INDEX IF EXISTS change_inbox_entity")
    conn.execute("INSERT INTO change_cursor VALUES (1,42)")
    conn.executemany("INSERT INTO change_inbox VALUES ('e',?,0,?)",
                     [(tx, json.dumps(change(tx, item)))
                      for tx, item in [(2, 'a'), (3, 'b'), (4, 'c'), (7, 'a')]])
    before = list(conn.execute("SELECT * FROM change_inbox ORDER BY emitter,tx,seq"))
    twice = [inc.ChangeRunner(e, conn, Sink()), inc.ChangeRunner(e, conn, Sink())]
    assert list(conn.execute("SELECT * FROM change_inbox ORDER BY emitter,tx,seq")) == before
    assert conn.execute("SELECT tx FROM change_cursor").fetchone() == (42,)
    indexes = {row[1] for row in conn.execute("PRAGMA index_list(change_inbox)")}
    assert 'change_inbox_entity' in indexes
    # Use the actual routing SELECT, not a second query with different semantics.
    query = next(value for value in twice[0].route_pending.__code__.co_consts
                 if isinstance(value, str) and value.startswith("SELECT tx,seq,payload"))
    plan = [row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + query, ('e','e',inc.BATCH))]
    assert not any('TEMP B-TREE FOR GROUP BY' in step for step in plan), plan


def test_entity_index_preserves_oldest_entity_batch_and_all_its_changes(setup):
    conn, _, runner, calls = setup
    entries = [(2, 'a'), (3, 'b'), (4, 'c'), (7, 'a')]
    conn.executemany("INSERT INTO change_inbox VALUES ('e',?,0,?)",
                     [(tx, json.dumps(change(tx, item))) for tx,item in entries])
    conn.execute("INSERT INTO change_inbox VALUES ('another',1,0,?)", (json.dumps(change(1, 'x')),))
    conn.execute("INSERT INTO change_cursor VALUES (1,42)")
    calls.clear()
    runner.route_pending(100)
    assert [(row['tx'],row['entity']) for row in calls[-1]['route']] == [(2,'a'),(3,'b'),(7,'a')]
    assert list(conn.execute("SELECT emitter,tx FROM change_inbox ORDER BY emitter,tx")) == [('another',1),('e',4)]
    assert list(conn.execute("SELECT item FROM adapter_deadlines ORDER BY item")) == [('a',),('b',)]
    assert conn.execute("SELECT tx FROM change_cursor").fetchone() == (42,)


def _vm_steps(conn, action):
    steps = 0
    def progress():
        nonlocal steps
        steps += 1
        return 0
    conn.set_progress_handler(progress, 1)
    try:
        result = action()
    finally:
        conn.set_progress_handler(None, 0)
    return steps, result


def _read_chars():
    path = Path('/proc/self/io')
    if not path.exists():
        return 0  # Linux-only read-amplification evidence; VM budgets run everywhere.
    return int(next(line.split()[1] for line in path.read_text().splitlines() if line.startswith('rchar:')))


def test_routing_and_snapshot_work_do_not_scale_with_unselected_inbox(setup):
    """A two-entity batch must not repeatedly walk the other retained payloads.

    Measure the real route/ack and snapshot, not a hand-written candidate SQL.
    The legacy SELECT is the independent output oracle and slow-path control.
    """
    conn, _, runner, calls = setup
    legacy = (
        "SELECT tx,seq,payload FROM change_inbox WHERE emitter=? AND json_extract(payload,'$.entity') IN "
        "(SELECT json_extract(payload,'$.entity') FROM change_inbox WHERE emitter=? "
        " GROUP BY json_extract(payload,'$.entity') ORDER BY MIN(tx),MIN(seq) LIMIT ?) ORDER BY tx,seq")
    costs = []
    reads = []
    for size in (1000, 20000):
        conn.execute('BEGIN IMMEDIATE')
        conn.execute("DELETE FROM change_inbox")
        conn.executemany("INSERT INTO change_inbox VALUES ('e',?,0,?)",
                         ((n + 1, json.dumps({**change(n + 1, f'entity-{n:06d}'),
                                              'padding': 'x' * 1024})) for n in range(size)))
        conn.execute("INSERT OR REPLACE INTO change_cursor VALUES (1,42)")
        conn.execute('COMMIT')
        conn.execute('PRAGMA shrink_memory')
        start = _read_chars()
        legacy_steps, expected = _vm_steps(conn, lambda: conn.execute(legacy, ('e','e',inc.BATCH)).fetchall())
        legacy_reads = _read_chars() - start
        calls.clear()
        conn.execute('PRAGMA shrink_memory')
        start = _read_chars()
        route_steps, _ = _vm_steps(conn, lambda: runner.route_pending(100))
        route_reads = _read_chars() - start
        reads.append((route_reads, legacy_reads))
        assert calls[-1]['route'] == [json.loads(row[2]) for row in expected]
        assert conn.execute("SELECT tx FROM change_cursor").fetchone() == (42,)
        snapshot_steps, _ = _vm_steps(conn, runner._publish)
        assert any(f' {size - inc.BATCH}' in line for line in runner.metrics(str)
                   if line.startswith('chaski_emitter_changes_pending{'))
        costs.append((route_steps, snapshot_steps, legacy_steps))
    # Fixed budget includes writes, maintained projections and metric queries.
    # A mere outer INDEXED BY hint still scans the legacy inner GROUP BY.
    small, large = costs
    print({'inbox_sizes': [1000, 20000], 'route_snapshot_legacy_vm_steps': costs})
    print({'route_legacy_read_chars': reads})
    assert large[0] < 8000 and large[0] < max(100, small[0]) * 2, costs
    assert large[1] < 4000 and large[1] < max(100, small[1]) * 2, costs
    assert large[0] * 5 < large[2], costs
    if reads[-1][1]:
        assert reads[-1][0] * 5 < reads[-1][1], reads


def test_legacy_inbox_projection_preserves_rows_cursor_and_restart(tmp_path, monkeypatch):
    conn = connect(tmp_path / 'legacy.db')
    conn.executescript(inc.SCHEMA)
    entries = [(2,9,'z'), (2,10,'a'), (3,0,'a'), (4,0,'z'), (5,1,'b')]
    conn.executemany("INSERT INTO change_inbox VALUES ('e',?,?,?)",
                     [(tx, seq, json.dumps({**change(tx, item), 'sequence': seq}))
                      for tx, seq, item in entries])
    conn.execute("INSERT INTO change_cursor VALUES (1,42)")
    before = list(conn.execute('SELECT * FROM change_inbox ORDER BY emitter,tx,seq'))
    calls = []
    def adapter(emitter, request=None):
        if request is None:
            return {'version': 1, 'attributes': ['status'], 'graphs': ['ROOT']}
        calls.append(request)
        return {'items': sorted({r['entity'] for r in request['route']})}
    monkeypatch.setattr(inc, 'call', adapter)
    e = Emitter('e', ['unused'], 86400, 'owner', change_driven=True)
    first = inc.ChangeRunner(e, conn, Sink())
    assert list(conn.execute('SELECT * FROM change_inbox ORDER BY emitter,tx,seq')) == before
    first.route_pending(100)
    assert [(r['tx'],r['sequence'],r['entity']) for r in calls[-1]['route']] == entries[:4]
    conn.close()
    conn = connect(tmp_path / 'legacy.db')
    try:
        restarted = inc.ChangeRunner(e, conn, Sink())
        restarted.route_pending(101)
        assert [(r['tx'],r['sequence'],r['entity']) for r in calls[-1]['route']] == entries[4:]
        assert conn.execute('SELECT COUNT(*) FROM change_inbox').fetchone() == (0,)
        assert conn.execute('SELECT tx FROM change_cursor').fetchone() == (42,)
    finally:
        conn.close()


def test_routing_ack_failure_keeps_next_route_and_metrics(setup):
    conn, _, runner, calls = setup
    conn.execute("INSERT INTO change_inbox VALUES ('e',2,0,?)", (json.dumps(change()),))
    runner._publish()
    before = runner.metrics(str)
    conn.execute("CREATE TRIGGER fail_ack BEFORE DELETE ON change_inbox "
                 "BEGIN SELECT RAISE(ABORT,'ack failure'); END")
    with pytest.raises(Exception, match='ack failure'):
        runner.route_pending(100)
    failed = calls[-1]['route']
    runner._publish()
    assert runner.metrics(str) == before
    assert conn.execute('SELECT COUNT(*) FROM adapter_deadlines').fetchone() == (0,)
    conn.execute('DROP TRIGGER fail_ack')
    runner.route_pending(101)
    assert calls[-1]['route'] == failed


def test_metrics_never_observe_partial_subclass_snapshot(setup):
    """Pause the publisher between base metrics and its inbox/deadline reads."""
    import threading
    conn, _, runner, _ = setup
    entered, read = threading.Event(), threading.Event()
    observed = []
    class DuringCounts:
        def execute(self, query, *args):
            if not entered.is_set() and ('change_inbox' in query or 'adapter_deadlines' in query):
                entered.set()
                assert read.wait(2), 'metrics reader blocked behind SQLite publisher'
            return conn.execute(query, *args)
    runner.conn = DuringCounts()
    def reader():
        try:
            assert entered.wait(2)
            observed.append(runner.metrics(str))
        except BaseException as exc:
            observed.append(exc)
        finally:
            read.set()
    thread = threading.Thread(target=reader)
    thread.start()
    try:
        runner._publish()
    finally:
        thread.join(3)
    assert not thread.is_alive()
    assert len(observed) == 1 and isinstance(observed[0], list), observed
    assert any(line.startswith('chaski_emitter_changes_pending{') for line in observed[0])


def test_projection_tracks_old_writer_replay_and_transaction_rollback(setup):
    conn, _, runner, _ = setup
    row = ('e', 2, 0, json.dumps(change()))
    # These are the pre-projection writer's unchanged statements. Triggers also
    # keep rollback binaries compatible; ignored replay must not double-count.
    conn.execute('INSERT INTO change_inbox VALUES (?,?,?,?)', row)
    conn.execute('INSERT OR IGNORE INTO change_inbox VALUES (?,?,?,?)', row)
    conn.execute('BEGIN IMMEDIATE')
    conn.execute('DELETE FROM change_inbox WHERE emitter=? AND tx=? AND seq=?', row[:3])
    conn.execute('ROLLBACK')
    runner._publish()
    assert conn.execute('SELECT pending FROM change_inbox_counts WHERE emitter=?', ('e',)).fetchone() == (1,)
    runner.route_pending(100)
    assert conn.execute('SELECT * FROM change_inbox_frontier').fetchall() == []
    assert conn.execute('SELECT pending FROM change_inbox_counts WHERE emitter=?', ('e',)).fetchone() == (0,)


def test_failed_snapshot_keeps_previous_complete_publication(setup):
    conn, _, runner, _ = setup
    before = runner.metrics(str)
    class FailedCounts:
        def execute(self, query, *args):
            if 'change_inbox_counts' in query:
                raise RuntimeError('count unavailable')
            return conn.execute(query, *args)
    runner.conn = FailedCounts()
    with pytest.raises(RuntimeError, match='count unavailable'):
        runner._publish()
    assert runner.metrics(str) == before


def test_projection_migration_failure_is_atomic(tmp_path, monkeypatch):
    conn = connect(tmp_path / 'migration.db')
    try:
        conn.executescript(inc.SCHEMA)
        conn.execute("INSERT INTO change_inbox VALUES ('e',2,0,?)", (json.dumps(change()),))
        conn.execute('INSERT INTO change_cursor VALUES (1,42)')
        before = list(conn.execute('SELECT * FROM change_inbox'))
        original = inc.INBOX_PROJECTION
        monkeypatch.setattr(inc, 'INBOX_PROJECTION', original + '\nINVALID SQL;')
        with pytest.raises(Exception, match='syntax'):
            inc.prepare_inbox_projection(conn)
        assert list(conn.execute('SELECT * FROM change_inbox')) == before
        assert conn.execute('SELECT tx FROM change_cursor').fetchone() == (42,)
        assert not conn.execute("SELECT name FROM sqlite_master WHERE name LIKE 'change_inbox_projection_%'").fetchall()
        monkeypatch.setattr(inc, 'INBOX_PROJECTION', original)
        inc.prepare_inbox_projection(conn)
        assert conn.execute("SELECT pending FROM change_inbox_counts WHERE emitter='e'").fetchone() == (1,)
    finally:
        conn.close()
