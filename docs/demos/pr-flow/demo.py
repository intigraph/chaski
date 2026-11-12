"""Run from the repository root: python3 docs/demos/pr-flow/demo.py."""
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
sys.path.insert(0, str(Path.cwd()))
import github_routes as gr
from test_github_work import cfg_doc
from test_github_routes import BASE, REPO, PR177, ep, variant, ts
with TemporaryDirectory() as directory:
    doc = cfg_doc(Path(directory))
    doc['repos'][REPO]['stale_prs'] = {'enabled': True, 'approved_after_minutes': 120}
    try:
        cfg = gr.parse_config(doc)
    except ValueError as error:
        print('Config refused:', error)
        raise SystemExit(0)
    conflict = variant('pull_request.opened', 'conflict-demo', props={
        'githubMergeable': False, 'githubMergeableState': 'dirty',
        'githubBaseSha': 'b'*40, 'occurredAt': '2026-10-27T19:00:00Z'})
    snap = gr.FixtureFacts([ep(n) for n in BASE]+[conflict], [], {PR177:'author'}).snapshot(REPO,0)
    decisions = gr.evaluate_repo(cfg,cfg.repos[REPO],snap,[],ts('2026-10-27T19:01:00Z'))
    for decision in decisions.values():
        request = gr.record_for(cfg,decision)
        import json
        body = json.loads(request['evidence'])
        print('Request:',body['kind'],'route:',body['route'],'assignee:',body.get('assignee'))
