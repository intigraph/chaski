# Preserve a firing state through UNKNOWN

This 20-second recording calls the repository’s existing offline canary demo. Real `Reactor` logic sees three synthetic query outcomes: firing, a timeout, then an empty successful answer. The timeout preserves the firing state; only the successful empty answer resolves it. No Quipu connection, notification sink, receiver, stream binding or live cursor is used. The transcript includes the actual UNKNOWN warning.

Recorded from source `2632a253c77197992cf981ba8c5d19cd7aa71600` using Python and PyYAML 6.0.3. `terminal/manifest.json` pins all source inputs and the Python executable. The small launcher calls the existing `demo.main()` entry point without modifying the application.

Use Python 3.11+ with PyYAML 6.0.3. From the repository root, prepare a new, absent local workspace:

```sh
mkdir /tmp/chaski-demo
cp demo.py chaski.py deadline_heap.py docs/demos/terminal/run-demo.py /tmp/chaski-demo/
touch /tmp/chaski-demo/.terminal-demo-sandbox
python3 docs/demos/terminal/render.py docs/demos/terminal/demo.tape.json \
  --workspace /tmp/chaski-demo --tool python3="$(command -v python3)" \
  --output /tmp/chaski-render
```

Use the recorded source revision for the original inputs. Do not reuse a workspace with `canary.json`: the first frame must observe no previous local run. The renderer gives the program a private HOME/XDG environment; the canary receipt stays in that workspace. Deliberate reading holds differ from command execution times, which are recorded in the manifest.

The recorder is unchanged from Tambo’s canonical `stack-terminal-24` harness at `b0765b5d8f16a6a26acb3595552f17826d965b44`, SHA-256 `bebbe000565314f18a58731f2a1a4690577db127b3673fa10a45ce74eef0f23c`. Read `terminal/transcript.txt` for a static alternative.
