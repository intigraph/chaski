"""Delivery telemetry never substitutes for a durable cursor or reads on scrape."""
import queue
import sqlite3
import threading
from types import SimpleNamespace

import pytest

from change_stream import ChangeStream
from chaski import Reactor
from incremental import ChangeFeed, SCHEMA


def feed_at(cursor=10):
    conn = sqlite3.connect(':memory:', isolation_level=None)
    conn.executescript(SCHEMA)
    conn.execute('INSERT INTO change_cursor VALUES (1,?)', (cursor,))
    runner = SimpleNamespace(conn=conn, accepts=lambda record: False)
    return ChangeFeed(SimpleNamespace(), [runner])


def page(cursor=11):
    return {'records': [{'tx': cursor, 'sequence': 0, 'op': 'assert'}],
            'next_tx': cursor, 'watermark_tx': cursor}


def stream_at(cursor=10):
    stream = object.__new__(ChangeStream)
    stream.cursor = cursor
    stream.errors = 0
    stream.received = 0
    stream.logged_first_commit = False
    stream.pending = queue.Queue(maxsize=1)
    stream.wakeup = threading.Event()
    return stream


def test_receipt_is_not_commit_and_first_path_log_is_bounded(caplog):
    feed, stream = feed_at(), stream_at()
    try:
        with caplog.at_level('INFO', logger='chaski.changes.stream'):
            for cursor in (11, 12):
                stream.pending.put((cursor, page(cursor), threading.Event()))
                assert stream.pump(feed) == 1
        assert stream.received == 2 and feed.cursor_tx == stream.cursor == 12
        assert feed.conn.execute('SELECT tx FROM change_cursor').fetchone() == (12,)
        assert feed.poll_events_received == 0
        assert [r.message for r in caplog.records] == ['stream path committed tx=11']
        # Replayed delivery counts as received, but never advances the DB cursor.
        stream.pending.put((12, page(12), threading.Event()))
        assert stream.pump(feed) == 0
        assert stream.received == 3 and feed.cursor_tx == stream.cursor == 12
        assert feed.errors == 1
    finally:
        feed.conn.close()


def test_transaction_failure_does_not_publish_new_cursor():
    feed = feed_at()
    try:
        feed.accept_page(page(), 10, stream_id=11)
        feed.conn.execute("CREATE TRIGGER refuse_cursor BEFORE INSERT ON change_cursor BEGIN SELECT RAISE(ABORT,'fixture refusal'); END")
        with pytest.raises(sqlite3.IntegrityError):
            feed.accept_page(page(12), 11, stream_id=12)
        assert feed.cursor_tx == 11
        assert feed.conn.execute('SELECT tx FROM change_cursor').fetchone() == (11,)
    finally:
        feed.conn.close()


def test_poll_and_stream_counters_are_independent(tmp_path):
    feed = feed_at()
    try:
        feed.quipu._request = lambda *args: page()
        feed.poll()
        assert feed.cursor_tx == 11 and feed.poll_events_received == 1
        reactor = Reactor(SimpleNamespace(), [], tmp_path / 'state', 60)
        reactor.change_feed = feed
        stream = stream_at(11)
        reactor.change_stream = stream
        stream.pending.put((12, page(12), threading.Event()))
        stream.pump(feed)
        # Scrapes read scalar snapshots, even if SQLite is unavailable.
        feed.conn.close()
        before = (feed.cursor_tx, feed.poll_events_received, stream.received)
        for _ in range(3):
            metrics = reactor.metrics()
            assert 'chaski_changes_cursor_offset 12\n' in metrics
            assert 'chaski_changes_poll_fallback_events_total 1\n' in metrics
            assert 'chaski_changes_stream_events_received_total 1\n' in metrics
        assert before == (feed.cursor_tx, feed.poll_events_received, stream.received)
    finally:
        feed.conn.close()


def test_cursor_unknown_before_first_commit_and_no_stream_series(tmp_path):
    feed = feed_at()
    try:
        reactor = Reactor(SimpleNamespace(), [], tmp_path / 'state', 60)
        reactor.change_feed = feed
        metrics = reactor.metrics()
        assert 'chaski_changes_cursor_offset -1\n' in metrics
        assert 'chaski_changes_stream_events_received_total' not in metrics
    finally:
        feed.conn.close()
