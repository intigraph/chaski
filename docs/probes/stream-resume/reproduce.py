"""Source-level startup demo: real SQLite, mocked network/metrics boundaries."""
import argparse
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
import chaski  # noqa: E402 - use this checkout, not an installed consumer
import change_stream  # noqa: E402


def probe(module):
    db = sqlite3.connect(':memory:')
    db.execute('CREATE TABLE change_cursor(id INTEGER PRIMARY KEY, tx INTEGER)')
    db.execute('INSERT INTO change_cursor VALUES(1,9)')
    db.commit()
    from incremental import bind_stream_source
    bind_stream_source(db, {'store_id': 'fixture', 'log_epoch': 'one'}, 9)
    calls, opened = [], []
    feed = SimpleNamespace(conn=db, ready=False, cursor_tx=-1,
                           poll=lambda: calls.append('GET /changes'))
    class End(Exception):
        pass
    def tick(_):
        raise End
    original_metrics = module.serve_metrics
    original_stream = change_stream.ChangeStream
    module.serve_metrics = lambda *args: None
    change_stream.ChangeStream = lambda _, cursor: opened.append(cursor) or object()
    try:
        reactor = SimpleNamespace(change_feed=feed, quipu=object(), event_poll_s=60, tick=tick)
        try:
            module._serve_consumer(SimpleNamespace(once=False, changes_stream=True, metrics_port=0), reactor, [])
        except End:
            pass
        return {'startup_feed_reads': len(calls), 'resumed_cursor': opened[0],
                'durable_cursor': db.execute('SELECT tx FROM change_cursor').fetchone()[0]}
    finally:
        module.serve_metrics = original_metrics
        change_stream.ChangeStream = original_stream
        db.close()


def main():
    args = argparse.ArgumentParser()
    args.add_argument('--base-source', required=True, type=Path)
    old = args.parse_args().base_source
    spec = importlib.util.spec_from_file_location('baseline_chaski', old)
    baseline = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = baseline
    spec.loader.exec_module(baseline)
    before, after = probe(baseline), probe(chaski)
    print('Synthetic source fixture: real SQLite; feed and metrics calls mocked; no server contacted.')
    print('before:', json.dumps(before, sort_keys=True))
    print('after: ', json.dumps(after, sort_keys=True))
    assert before['startup_feed_reads'] == 1 and after['startup_feed_reads'] == 0
    assert before['durable_cursor'] == after['durable_cursor'] == 9
    assert before['resumed_cursor'] == after['resumed_cursor'] == 9


if __name__ == '__main__':
    main()
