"""Restart and retained-setup controls for the bounded scheduled entry."""

import pytest

import emitter
import once


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
