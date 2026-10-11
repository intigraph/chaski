import unittest
from failure_family_adapter import verdicts, validate_policy


class AdapterTests(unittest.TestCase):
    def setUp(self):
        self.policy = {
            "default_threshold": 5,
            "window_seconds": 3600,
            "default_owner": "crew",
            "family_thresholds": {},
            "tool_owners": {"Read": "reader"},
        }
        self.family = "a" * 64

    def report(self, count):
        return {
            "window_seconds": 3600,
            "unknown": 3,
            "excluded": 2,
            "families": [{"id": self.family, "tool": "Read", "current_window": count}],
        }

    def test_boundary_and_override(self):
        self.assertEqual(verdicts(self.report(4), self.policy)[0]["verdict"], "BELOW")
        above = verdicts(self.report(5), self.policy)[0]
        self.assertEqual(above["verdict"], "ABOVE")
        self.assertEqual(above["owner"], "reader")
        self.policy["family_thresholds"][self.family] = 10
        self.assertEqual(verdicts(self.report(5), self.policy)[0]["verdict"], "BELOW")

    def test_unknown_policy_and_inventory(self):
        for key, value in (
            ("default_owner", "human"),
            ("default_threshold", True),
            ("window_seconds", 0),
            ("family_thresholds", {self.family: False}),
        ):
            with self.assertRaises(ValueError):
                validate_policy({**self.policy, key: value})
        for report in ({}, {**self.report(0), "unknown": None}, self.report(-1)):
            with self.assertRaises(ValueError):
                verdicts(report, self.policy)
        duplicate = self.report(1)
        duplicate["families"] *= 2
        with self.assertRaises(ValueError):
            verdicts(duplicate, self.policy)


if __name__ == "__main__":
    unittest.main()


def test_local_reader_ignores_operator_remote_config(tmp_path, monkeypatch, capsys):
    import json
    from pathlib import Path
    from types import SimpleNamespace
    import failure_family_adapter as adapter

    operator = tmp_path / "operator"
    (operator / ".dp").mkdir(parents=True)
    config = operator / ".dp" / "config.toml"
    config.write_text('store_mode = "remote"\nremote_url = "https://example.invalid"\n')
    monkeypatch.setenv("HOME", str(operator))
    policy = {"default_threshold": 5, "window_seconds": 3600, "default_owner": "crew"}
    monkeypatch.setattr(adapter, "policy_from_graph", lambda *args: policy)
    monkeypatch.setattr(adapter.shutil, "which", lambda value: "/usr/bin/true")
    seen = []
    def child(command, **kwargs):
        home = Path(kwargs["env"]["HOME"])
        assert home != operator
        assert not (home / ".dp" / "config.toml").exists()
        assert Path(command[2]).is_absolute()
        seen.append(home)
        return SimpleNamespace(returncode=0, stdout=json.dumps(
            {"window_seconds": 3600, "unknown": 0, "excluded": 0, "families": []}))
    monkeypatch.setattr(adapter.subprocess, "run", child)
    monkeypatch.setattr(adapter.os.sys, "argv",
                        ["adapter", "--quipu-server", "https://example.invalid",
                         "--policy-iri", "https://example.invalid/policy",
                         "--db", str(tmp_path / "existing.db")])
    adapter.main()
    assert capsys.readouterr().out.strip() == "[]"
    assert not seen[0].exists()
    assert config.read_text().startswith('store_mode = "remote"')
