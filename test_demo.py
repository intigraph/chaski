"""Offline installation proof cannot turn a failed canary into success."""
import json
from unittest.mock import patch

import chaski
import demo


def test_canary_status_is_absent_then_reads_measured_receipt(tmp_path, capsys):
    receipt = tmp_path / "demo.json"
    assert chaski.main(["demo", "status", "--state", str(receipt)]) == 0
    assert "unique-canary" not in capsys.readouterr().out
    assert chaski.main(["demo", "run", "--state", str(receipt), "--focus", "unique-canary"]) == 0
    assert "FIRING -> UNKNOWN (preserved) -> RESOLVED" in capsys.readouterr().out
    assert chaski.main(["demo", "status", "--state", str(receipt)]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report == dict(version=1, focus="unique-canary", fired=True, unknown_preserved=True,
                          resolved=True, transitions=2)


def test_failed_engine_does_not_replace_previous_receipt(tmp_path, capsys):
    receipt = tmp_path / "demo.json"
    receipt.write_text("previous evidence")
    with patch.object(chaski.Reactor, "evaluate", side_effect=RuntimeError("engine unavailable")):
        assert demo.main(["run", "--state", str(receipt), "--focus", "control"]) == 2
    assert receipt.read_text() == "previous evidence"
    assert "UNKNOWN" in capsys.readouterr().err


def test_malformed_status_is_unknown_not_an_empty_success(tmp_path, capsys):
    receipt = tmp_path / "demo.json"
    for value in ("not json", "[]", '{"version":1,"focus":"copied marker"}'):
        receipt.write_text(value)
        assert demo.main(["status", "--state", str(receipt)]) == 2
        assert "UNKNOWN" in capsys.readouterr().err


def test_uninstalled_source_version_does_not_start_a_consumer(capsys):
    with patch.object(chaski.metadata, "version", side_effect=chaski.metadata.PackageNotFoundError):
        try:
            chaski.main(["--version"])
        except SystemExit as result:
            assert result.code == 0
        else:
            raise AssertionError("version did not exit")
    assert capsys.readouterr().out == "chaski source checkout\n"
