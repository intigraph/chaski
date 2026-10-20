"""Same local consumer-loop probe on base/head; no network, keys or live state."""
import argparse
import json
import math
from pathlib import Path
import statistics
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    sys.path.insert(0, str(args.source.resolve()))
    import chaski

    class Rules(list):
        visits = 0
        def __iter__(self):
            for rule in super().__iter__():
                self.visits += 1
                yield rule

    class Query:
        reads = 0
        due = None
        def __init__(self):
            self.latencies = []
        def select_focus(self, _):
            self.reads += 1
            if self.due is not None:
                self.latencies.append(max(0, time.time()-self.due))
            return set()

    def rules(n):
        return Rules(chaski.Rule.from_dict(dict(label=f'r{i}', triggerKind='schedule',
            schedule='PT1H', condition='SELECT ?focus WHERE { ?focus a <urn:fixture> }',
            severity='info', action=['alert'], owner='fixture')) for i in range(n))

    class EndWindow(Exception):
        pass

    report = {'scope': 'synthetic local source, fake query responses; no service acceptance',
              'idle': [], 'physical_latency': []}
    with tempfile.TemporaryDirectory(prefix='chaski-heap-demo-') as directory:
        for count in (10, 10000):
            q, population = Query(), rules(count)
            reactor = chaski.Reactor(q, population, Path(directory)/str(count), 60)
            for state in reactor.states.values():
                state.last_attempt = 1000
            population.visits = 0
            clock = [1000.0]
            turns = []
            tick = reactor.tick
            def counted(now):
                turns.append(now)
                tick(now)
            reactor.tick = counted
            def wait(timeout):
                if timeout is None or clock[0]+timeout >= 1060:
                    raise EndWindow
                if timeout <= 0:
                    raise AssertionError('busy zero-time idle wait')
                clock[0] += timeout
                return False
            with patch.object(chaski, 'serve_metrics', lambda *a: None), \
                 patch.object(chaski.time, 'time', lambda: clock[0]), \
                 patch.object(chaski.time, 'sleep', wait), \
                 patch.object(chaski.threading, 'Event', lambda: SimpleNamespace(wait=wait)):
                try:
                    chaski._serve_consumer(SimpleNamespace(once=False, changes_stream=False,
                        metrics_port=0), reactor, population)
                except EndWindow:
                    pass
            report['idle'].append(dict(jobs=count, window_s=60, reactor_turns=len(turns),
                idle_wakes_after_bootstrap=max(0,len(turns)-1), rule_visits=population.visits,
                graph_reads=q.reads, evaluations=sum(sum(s.evaluations.values())
                                                   for s in reactor.states.values())))
        if hasattr(chaski.Reactor, 'wait_timeout'):
            for count in (10, 10000):
                q, population = Query(), rules(count)
                reactor = chaski.Reactor(q, population, Path(directory)/('live'+str(count)), 60)
                q.due = time.time()+0.2
                for state in reactor.states.values():
                    state.last_attempt = q.due-3600
                reactor.tick(time.time())
                wait_for = reactor.wait_timeout(time.time())
                assert wait_for is not None and wait_for > 0
                threading.Event().wait(wait_for)
                reactor.tick(time.time())
                assert q.reads == count
                observed = sorted(q.latencies)
                p95 = observed[math.ceil(len(observed)*.95)-1]
                report['physical_latency'].append(dict(jobs=count, measured_queries=q.reads,
                    p50_s=statistics.median(observed), p95_s=p95,
                    target_p95_under_1s=p95<1,
                    scope='real OS wait plus sequential in-process fake queries; network excluded'))
        else:
            report['physical_latency_status'] = 'UNSUPPORTED: base has no deadline wait API'
    raw = json.dumps(report, indent=2, sort_keys=True)+'\n'
    print(raw, end='')
    if args.output:
        args.output.write_text(raw)


if __name__ == '__main__':
    main()
