#!/usr/bin/env python3
"""Same isolated builder input at base and candidate; no network or graph writes."""
import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
import graph_sink  # noqa: E402 — source-tree demo resolves its repository root
BASE = '64e484528d1b7f6955dbdff01e9d4e2c2a367464'
REACTION = {'label': 'fixture', 'condition': 'ASK {}', 'schedule': 'PT1M',
            'severity': 'info', 'action': ['notice'], 'owner': 'fixture', 'comment': 'fixture'}
EVENT = {'event_id': 'event-1', 'item': 'https://example.test/focus', 'observed_at': 1790400000}
def measure(label, module):
    sink = module.QuipuFiringSink('https://unused.example.test', None, REACTION, ns='urn:fixture:')
    bodies = sink.reaction_turtle() + sink.firing_turtle(EVENT)
    old = any('<' + sink.ns + name + '>' in bodies for name in graph_sink._VOCABULARY)
    identity = module.firing_iri('fixture', EVENT['event_id'], 'urn:fixture:')
    print(f'{label}: instance-base vocabulary present={old}; firing={identity}', flush=True)
    assert old == (label == 'base')
    return identity
with tempfile.TemporaryDirectory() as tmp:
    path = Path(tmp) / 'baseline.py'
    path.write_bytes(subprocess.check_output(['git', '-C', str(ROOT), 'show', BASE + ':graph_sink.py']))
    spec = importlib.util.spec_from_file_location('baseline_graph_sink', path)
    baseline = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(baseline)
    assert measure('base', baseline) == measure('candidate', graph_sink)
    try:
        graph_sink.term('UnpublishedTerm')
    except KeyError:
        print('PASS: identical instance identity; unknown vocabulary refuses without a fallback.')
    else:
        raise AssertionError('unknown term accepted')
