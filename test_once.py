"""Restart and retained-setup controls for the bounded scheduled entry."""

import pytest

import emitter
import once
import sys
import json


class Receiver:
    def __init__(self):
        self.present = False
        self.writes = 0
        self.content = "metadata"
        self._reaction_written = False

    def reaction_turtle(self):
        return self.content

    def reaction_iri(self):
        return "https://example.invalid/reaction"

    def setup_present(self):
        return self.present

    def setup(self):
        self.writes += 1
        self.present = True

    def deliver(self, event):
        raise ConnectionError("controlled failure")


def test_setup_restart_reuses_receipt_but_retraction_and_change_repair():
    conn = emitter.connect(":memory:")
    receiver = Receiver()
    first = once.RetainedSetup(conn, receiver)
    assert first.setup_pending()
    first.setup()
    restarted = once.RetainedSetup(conn, receiver)
    assert not restarted.setup_pending()
    assert receiver.writes == 1
    receiver.present = False
    assert restarted.setup_pending()
    restarted.setup()
    receiver.content = "changed metadata"
    changed = once.RetainedSetup(conn, receiver)
    assert changed.setup_pending()
    changed.setup()
    assert receiver.writes == 3


def test_unverified_setup_and_failed_graph_delivery_drop_cache():
    conn = emitter.connect(":memory:")
    receiver = Receiver()
    wrapper = once.RetainedSetup(conn, receiver)
    wrapper.setup()
    with pytest.raises(ConnectionError):
        wrapper.deliver({})
    assert conn.execute("SELECT COUNT(*) FROM receiver_setup").fetchone()[0] == 0
    receiver.setup = lambda: None
    receiver.present = False
    with pytest.raises(ConnectionError):
        wrapper.setup()
    assert conn.execute("SELECT COUNT(*) FROM receiver_setup").fetchone()[0] == 0


def test_startup_failure_replaces_previous_green_with_unknown(tmp_path, monkeypatch):
    metrics = tmp_path / "metrics.prom"
    metrics.write_text('chaski_local_measurement_ok{emitter="local"} 1\n')
    monkeypatch.setattr(once.os.sys, "argv",
                        ["once", "--emitters", str(tmp_path / "absent.yaml"),
                         "--label", "local", "--state", str(tmp_path / "state.db"),
                         "--quipu", "https://example.invalid",
                         "--metrics", str(metrics)])
    assert once.main() == 2
    assert 'chaski_local_measurement_ok{emitter="local"} 0' in metrics.read_text()


def test_partial_scheduled_tick_delivers_known_crossing_but_reports_unknown(tmp_path, monkeypatch):
    state, metrics = tmp_path / "state.db", tmp_path / "metrics.prom"
    output = {"records": [{"item": "known", "verdict": "ABOVE", "event_id": "known"}],
              "scope": ["known"]}
    definition = emitter.Emitter(
        label="local", command=[sys.executable, "-c", "import sys;print(sys.argv[1])", json.dumps(output)],
        interval_s=5, owner="crew", from_verdict="BELOW", to_verdict="ABOVE",
        missing_is_from=True, generation_event_ids=True,
    )
    conn = emitter.connect(state)
    emitter.observe(conn, definition, [{"item": "known", "verdict": "BELOW"}], 10)
    conn.close()
    delivered = []

    class Sink:
        def deliver(self, event):
            delivered.append(event)

    monkeypatch.setattr(once.chaski, "load_emitters", lambda _: [(definition, {})])
    monkeypatch.setattr(once.chaski, "make_sink", lambda *_: Sink())
    assert once.run_once(tmp_path / "fixture.yaml", "local", state,
                         "https://example.invalid", metrics, 30) == 2
    assert len(delivered) == 1 and delivered[0]["item"] == "known"
    assert 'chaski_local_measurement_ok{emitter="local"} 0' in metrics.read_text()
    assert 'result="unknown"} 1' in metrics.read_text()
