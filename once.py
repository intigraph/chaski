"""One bounded scheduled emitter tick; no HTTP listener or graph event poll."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import re
import os
from pathlib import Path
import tempfile
import time

import chaski
import emitter


class RetainedSetup:
    """Reuse an exact setup receipt only while the owned receiver is typed.

    Wrapping only the graph child keeps Alertmanager failures from erasing a
    valid graph setup. A graph delivery failure drops the local cache; retry
    repairs its metadata under the existing idempotent snapshot key.
    """

    def __init__(self, conn, receiver):
        self.conn, self.receiver = conn, receiver
        self.signature = hashlib.sha256(receiver.reaction_turtle().encode()).hexdigest()
        self.key = receiver.reaction_iri()
        conn.execute("CREATE TABLE IF NOT EXISTS receiver_setup (receiver TEXT PRIMARY KEY, signature TEXT NOT NULL)")

    def setup_pending(self):
        row = self.conn.execute("SELECT signature FROM receiver_setup WHERE receiver=?", (self.key,)).fetchone()
        if row and row[0] == self.signature and self.receiver.setup_present():
            self.receiver._reaction_written = True
            return False
        return True

    def setup(self):
        self.receiver.setup()
        if not self.receiver.setup_present():
            raise ConnectionError("setup accepted but not typed on read-back")
        self.conn.execute(
            "INSERT INTO receiver_setup VALUES(?,?) ON CONFLICT(receiver) DO UPDATE SET signature=excluded.signature",
            (self.key, self.signature),
        )

    def deliver(self, event):
        try:
            self.receiver.deliver(event)
        except Exception:
            self.conn.execute("DELETE FROM receiver_setup WHERE receiver=?", (self.key,))
            raise


def retain_setup(conn, sink):
    import alertmanager_sink
    import graph_sink

    if isinstance(sink, graph_sink.QuipuFiringSink):
        return RetainedSetup(conn, sink)
    if isinstance(sink, alertmanager_sink.ChainSink):
        return alertmanager_sink.ChainSink([retain_setup(conn, child) for child in sink.sinks])
    return sink


def write_metrics(path: Path, lines: list[str]) -> None:
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as tmp:
        temp = Path(tmp.name)
        tmp.write("\n".join(lines) + "\n")
    try:
        temp.chmod(0o644)
        temp.replace(path)
    finally:
        temp.unlink(missing_ok=True)


def publish(path: Path, runner: emitter.Runner) -> None:
    # The regular exporter can scrape these fixed-emitter metrics. No family,
    # command, error or argument is a metric label.
    lines = runner.metrics(lambda value: value.replace("\\", "\\\\").replace('"', '\\"'))
    label = runner.emitter.label
    measured = int(runner.last_success > 0 and runner.last_success >= runner.last_attempt)
    lines += [
        f'chaski_local_measurement_ok{{emitter="{label}"}} {measured}',
        f'chaski_local_last_attempt_seconds{{emitter="{label}"}} {runner.last_attempt}',
    ]
    write_metrics(path, lines)


def run_once(config: Path, label: str, state: Path, quipu: str, metrics: Path, write_interval: int = 30) -> int:
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]{0,127}", label):
        raise ValueError("invalid one-shot emitter label")
    definitions = [(e, sink) for e, sink in chaski.load_emitters(config) if e.label == label]
    if len(definitions) != 1:
        raise ValueError("select exactly one configured emitter")
    definition, sink_config = definitions[0]
    definition.persist_runs = True
    if definition.change_driven or definition.externally_triggered:
        raise ValueError("one-shot requires a scheduled adapter")
    if write_interval < 30:
        raise ValueError("one-shot sink budget has a thirty-second floor")
    # Lock before opening/migrating state or contacting any sink. The unit's
    # state and metrics parents are provisioned by the reviewed installation.
    with state.with_suffix(state.suffix + ".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        conn = emitter.connect(state)
        try:
            sink = retain_setup(conn, chaski.make_sink(definition, sink_config, quipu))
            runner = emitter.Runner(definition, conn, sink, emitter.PersistentWriteBudget(conn, write_interval))
            runner.tick(time.time())
            publish(metrics, runner)
            # A successful empty adapter is measurable; failure remains UNKNOWN.
            return 0 if runner.last_success > 0 and runner.last_success >= runner.last_attempt else 2
        finally:
            conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--emitters", type=Path, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--quipu", required=True)
    parser.add_argument("--metrics", type=Path, required=True)
    parser.add_argument("--write-interval", type=int, default=30)
    args = parser.parse_args()
    # Supply bounded error classes rather than exception text containing raw
    # adapter output, operational paths or credential-file names.
    try:
        return run_once(args.emitters, args.label, args.state, args.quipu, args.metrics, args.write_interval)
    except Exception as exc:
        # A concurrent holder owns its measurement. Other startup failures must
        # not leave a previous successful status looking like a fresh result.
        if not isinstance(exc, BlockingIOError) and re.fullmatch(
                r"[A-Za-z][A-Za-z0-9_.-]{0,127}", args.label):
            try:
                write_metrics(args.metrics, [
                    f'chaski_local_measurement_ok{{emitter="{args.label}"}} 0',
                    f'chaski_local_last_attempt_seconds{{emitter="{args.label}"}} {time.time()}'])
            except OSError:
                pass  # Missing metrics is caught by the separate scrape/liveness rule.
        print(f"UNKNOWN one-shot: {type(exc).__name__}", file=os.sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
