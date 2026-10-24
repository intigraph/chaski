"""Offline synthetic multipart transaction: identical wire input, base and head."""
import io
import subprocess
import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import change_stream  # noqa: E402
from test_multipart_change_stream import transaction, wire  # noqa: E402

BASE = 'd94e5f1711585c17261fc25e160b1783e061510b'
before = types.ModuleType('before')
exec(compile(subprocess.check_output(['git', 'show', BASE + ':change_stream.py']),
             'base/change_stream.py', 'exec'), before.__dict__)
events = transaction()
print('Synthetic transaction: two ordered parts, two records; no network or production state.')
print('Base:', BASE)
try:
    next(before.frames(io.BytesIO(wire(events))))
except Exception as exc:
    print('BEFORE:', type(exc).__name__, str(exc))
else:
    raise AssertionError('base unexpectedly accepted multipart input')
try:
    next(change_stream.frames(io.BytesIO(wire(events[:-1]))))
except Exception as exc:
    print('AFTER without commit:', type(exc).__name__, str(exc), '; no page emitted')
else:
    raise AssertionError('partial transaction unexpectedly emitted a page')
cursor, page = next(change_stream.frames(io.BytesIO(wire(events))))
try:
    assert cursor == 11 and len(list(page['records'])) == 2
    print('AFTER with commit: one page, cursor=11, records=2, private disk-backed staging')
finally:
    page['records'].close()
