# Repository witness refresh

Source-only recording: compare registry `b9c0613dcaca39751951b60d3de6c3ce28579784` with candidate registry SHA256 `70b14c18ba4f0404c930e7891a9f93b28221c22fbeb6b9a30a6cd34a7568caf6` against current main content `88bc77dc72a565c9a2e9a9828127f75decfe0a29`. The control counts byte-witness mismatches and Markdown fragments whose headings are absent. No index, graph, receiver or deployment is activated.

[Transcript](transcript.txt) and [timing](timing.txt) are small retained source artifacts. Replay from the repository root:

```sh
scriptreplay --log-out docs/demos/pendant-witness-refresh/transcript.txt --log-timing docs/demos/pendant-witness-refresh/timing.txt
```

Repeat the measurement (a full Git checkout with the base commit is required):

```sh
python3 - <<'PY'
import json,hashlib,re,subprocess
from pathlib import Path
base='b9c0613dcaca39751951b60d3de6c3ce28579784'
old=json.loads(subprocess.check_output(['git','show',base+':docs/knowledge/repository-knowledge.json']))
new=json.loads(Path('docs/knowledge/repository-knowledge.json').read_text())
for label,data in [('BEFORE registry at '+base,old),('AFTER candidate registry',new)]:
 stale=missing=fragments=0
 for r in data['records']:
  raw=Path(r['source']).read_bytes()
  stale+=hashlib.sha256(raw).hexdigest()!=r['source_sha256']
  if '#' in r['subject'] and r['source'].endswith('.md'):
   fragments+=1
   anchors={re.sub(r'[^\w\- ]','',h.lower()).replace(' ','-') for h in re.findall(r'^#{1,6} (.+)$',raw.decode(),re.M)}
   missing+=r['subject'].split('#',1)[1] not in anchors
 print(label)
 print('records:',len(data['records']),'stale hashes:',stale,'fragment records:',fragments,'missing anchors:',missing)
PY
```

The before registry is evaluated against the current source tree, reproducing the witness drift. The candidate must show zero stale hashes and zero missing anchors. Heading existence and hashes do not replace native index identity, full-corpus search or public asset acceptance.
