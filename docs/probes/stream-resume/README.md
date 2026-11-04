# Resume without a startup poll

Trigger: start the stream consumer with existing SQLite cursor 9.
Watch `startup_feed_reads`: the source fixture records one feed call before,
zero after. Both resume cursor 9 and leave durable custody unchanged.
Network and metrics are mocked; SQLite and the source startup functions are real.
This demonstrates startup only, not installed or steady-state request rates.

Base: `7b652cc03318089e0af622ee780b278bbb859080`.
Candidate code: `6294292e07cac5431d80b80721eed9980edaaaa4` (later demo files do not
change that startup implementation).

```sh
git show 7b652cc03318089e0af622ee780b278bbb859080:chaski.py > before.py
python docs/probes/stream-resume/reproduce.py --base-source before.py
```

The small output/timing pair is retained here as a terminal recording, not a
video or inline player. Download/decompress `output.txt.gz` and replay:

```sh
gzip -dc docs/probes/stream-resume/output.txt.gz > output.txt
scriptreplay --log-out output.txt --log-timing docs/probes/stream-resume/timing.txt
```

`transcript.txt` is the readable fallback. No live credentials or private data
are inputs. The `before.py` file is temporary and must not be committed.
