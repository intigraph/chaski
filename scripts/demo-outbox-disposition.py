"""Record actual before/after library delivery on synthetic disposable state.

DEMO_BASE_SOURCE selects a checkout/export of the documented base revision.
No production paths, adapters, credentials, or network are used.
"""
import os
from pathlib import Path
import subprocess
import sys

PROBE = r'''
import json,sys,tempfile
sys.path.insert(0,sys.argv[1])
import emitter as em
class Sink:
    def __init__(self): self.events=[]
    def deliver(self,event): self.events.append(event['event_id'])
with tempfile.TemporaryDirectory() as root:
    conn=em.connect(root+'/fixture.db')
    definition=em.Emitter(label='fixture',command=[],interval_s=5,owner='fixture')
    em.observe(conn,definition,[{'item':'old','verdict':'BLOCKED'}],1)
    em.observe(conn,definition,[{'item':'old','verdict':'UNBLOCKED','event_id':'old-event'}],2)
    old=conn.execute('SELECT event_id FROM outbox').fetchone()[0]
    if sys.argv[2]=='expired':
        plan={'schema':em.DISPOSITION_PLAN,'emitter':'fixture','created_before':3,
              'expected_count':1,'reason':'obsolete synthetic backlog','ruling_ref':'fixture-ruling',
              'source_utc':'2026-01-01T00:00:00Z',
              'rows':[{'event_id':old,'row_sha256':em.disposition_digest(em.disposition_row(conn,old))}]}
        em.apply_disposition_plan(conn,plan,em.disposition_digest(plan))
    sink=Sink()
    em.deliver_pending(conn,sink,10,'fixture')
    row=conn.execute('SELECT attempts,delivered_at FROM outbox WHERE event_id=?',(old,)).fetchone()
    if sys.argv[2]=='expired':
        assert row==(0,None) and sink.events==[]
    else:
        assert row==(1,10) and sink.events==[old]
    em.observe(conn,definition,[{'item':'fresh','verdict':'BLOCKED'}],11,scope=['fresh'])
    em.observe(conn,definition,[{'item':'fresh','verdict':'UNBLOCKED','event_id':'fresh-event'}],12,scope=['fresh'])
    em.deliver_pending(conn,sink,20,'fixture')
    assert 'fresh-event' in sink.events
    print(json.dumps({'mode':sys.argv[2],'obsolete_attempts':row[0],
                      'obsolete_received':row[1] is not None,'sent':sink.events,
                      'fresh_control_received':'fresh-event' in sink.events}))
    conn.close()
'''


def main():
    candidate = Path(__file__).resolve().parents[1]
    base = Path(os.environ['DEMO_BASE_SOURCE']).resolve()
    for mode, source in [('base', base), ('expired', candidate)]:
        print('Synthetic fixture: same old firing; deliver_pending at t=10; fresh control at t=20. ' + mode,
              flush=True)
        subprocess.run([sys.executable, '-c', PROBE, str(source), mode], check=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
