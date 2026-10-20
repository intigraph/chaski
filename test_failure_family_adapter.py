import unittest
import datetime as dt
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
        self.until = dt.datetime(2026, 1, 1, 12, tzinfo=dt.timezone.utc)
        self.since = self.until - dt.timedelta(hours=1)

    def verdicts(self, report, policy=None):
        return verdicts(report, policy or self.policy, since=self.since, until=self.until)

    def report(self, count):
        return {
            "window_seconds": 3600,
            "unknown": 0,
            "excluded": 2,
            "since": self.since.isoformat(), "until": self.until.isoformat(),
            "complete": True, "max_records": 10000, "input_records": count + 2,
            "families": [{"id": self.family, "tool": "Read", "current_window": count,
                          "count": count}],
        }

    def test_boundary_and_override(self):
        self.assertEqual(self.verdicts(self.report(4))[0]["verdict"], "BELOW")
        above = self.verdicts(self.report(5))[0]
        self.assertEqual(above["verdict"], "ABOVE")
        self.assertEqual(above["owner"], "reader")
        self.policy["family_thresholds"][self.family] = 10
        self.assertEqual(self.verdicts(self.report(5))[0]["verdict"], "BELOW")

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
                self.verdicts(report)
        duplicate = self.report(1)
        duplicate["families"] *= 2
        with self.assertRaises(ValueError):
            self.verdicts(duplicate)

    def test_stale_partial_legacy_and_unknown_reports_cannot_resolve(self):
        for change in ({"until": "2000-01-01T00:00:00Z"}, {"since": "bad"},
                       {"complete": False}, {"complete": 1}, {"unknown": 1},
                       {"max_records": 10001}, {"input_records": True},
                       {"input_records": 10001}, {"input_records": 0}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.verdicts({**self.report(0), **change})
        legacy = self.report(0)
        del legacy["until"]
        with self.assertRaises(ValueError):
            self.verdicts(legacy)

    def test_cardinality_accounting_and_scrubbed_output(self):
        import json
        report = self.report(5)
        report["raw_command"] = "private-secret"
        report["families"][0]["raw_error"] = "private-secret"
        self.assertNotIn("private-secret", json.dumps(self.verdicts(report)))
        for bad in (None, "bad\nsecret", "x" * 129):
            report["families"][0]["tool"] = bad
            with self.assertRaises(ValueError):
                self.verdicts(report)
        report = self.report(1)
        report["families"] *= 10001
        with self.assertRaises(ValueError):
            self.verdicts(report)

    def test_partial_normalization_keeps_proven_crossings_but_not_recovery(self):
        for count, expected in ((4, "UNKNOWN"), (5, "ABOVE")):
            report = self.report(count)
            report["unknown"] = 1
            report["input_records"] += 1
            result = self.verdicts(report)
            self.assertEqual(result["scope"], [self.family])
            self.assertEqual(result["records"][0]["verdict"], expected)
        report = {**self.report(0), "families": [], "unknown": 1, "input_records": 3}
        self.assertEqual(self.verdicts(report), {"records": [], "scope": []})


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
        start, end = command[command.index("--since") + 1], command[command.index("--until") + 1]
        assert dt.datetime.fromisoformat(end) - dt.datetime.fromisoformat(start) == dt.timedelta(hours=1)
        assert command[command.index("--max-records") + 1] == "10000"
        return SimpleNamespace(returncode=0, stdout=json.dumps(
            {"window_seconds": 3600, "unknown": 0, "excluded": 0, "families": [],
             "since": start, "until": end, "max_records": 10000,
             "input_records": 0, "complete": True}))
    monkeypatch.setattr(adapter.subprocess, "run", child)
    monkeypatch.setattr(adapter.os.sys, "argv",
                        ["adapter", "--quipu-server", "https://example.invalid",
                         "--policy-iri", "https://example.invalid/policy",
                         "--db", str(tmp_path / "existing.db")])
    adapter.main()
    assert capsys.readouterr().out.strip() == "[]"
    assert not seen[0].exists()
    assert config.read_text().startswith('store_mode = "remote"')


def test_threshold_restart_lost_ack_unknown_and_exact_recovery(tmp_path, monkeypatch):
    """Real adapter/runner/sink logic, in-memory receiver transport; no HTTP."""
    import json
    import emitter
    from alertmanager_sink import AlertmanagerSink

    case = AdapterTests()
    case.setUp()
    clock = [10]
    active, attempts = {}, []
    lose_ack = [True]
    sink = AlertmanagerSink("https://example.invalid", {
        "alertname": "FailureFamilyFixture", "severity": "warning",
    }, "families", clock=lambda: clock[0], owner="crew")

    def receiver(method, path, body=None):
        if method == "GET":
            return 200, json.dumps(list(active.values())).encode()
        assert method == "POST" and path == "/api/v2/alerts" and len(body) == 1
        alert = body[0]
        identity = json.dumps(alert["labels"], sort_keys=True)
        attempts.append(identity)
        end = dt.datetime.fromisoformat(alert["endsAt"].replace("Z", "+00:00")).timestamp()
        if end <= clock[0]:
            active.pop(identity, None)
        else:
            active[identity] = alert
            if lose_ack[0]:
                lose_ack[0] = False
                raise ConnectionError("fixture lost committed acknowledgement")
        return 200, b"[]"

    monkeypatch.setattr(sink, "_request", receiver)
    definition = emitter.Emitter(
        label="families", command=[], interval_s=5, owner="crew",
        from_verdict="BELOW", to_verdict="ABOVE", generation_event_ids=True,
        missing_is_from=True, resolve_recovery=True, persist_runs=True,
    )
    path = tmp_path / "state.db"
    conn = emitter.connect(path)
    runner = emitter.Runner(definition, conn, sink)

    def tick(report, now):
        clock[0] = now
        monkeypatch.setattr(emitter, "run_adapter", lambda _: emitter.decode_adapter_output(
            json.dumps(case.verdicts(report))))
        runner.tick(now)

    tick(case.report(4), 10)
    partial = {**case.report(5), "unknown": 1, "input_records": 8}
    tick(partial, 20)
    assert len(active) == 1
    assert runner.runs == {"ok": 1, "unknown": 1} and runner.last_success == 10
    assert conn.execute("SELECT delivered_at FROM outbox").fetchone()[0] is None
    conn.close()
    conn = emitter.connect(path)
    runner = emitter.Runner(definition, conn, sink)
    tick(partial, 100)
    assert len(active) == 1 and attempts[0] == attempts[1]
    assert conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 1
    for change in ({"unknown": 1, "input_records": 3}, {"complete": False},
                   {"until": "2000-01-01T00:00:00Z"}):
        tick({**case.report(0), **change}, clock[0] + 10)
        assert conn.execute("SELECT verdict FROM verdicts").fetchone()[0] == "ABOVE"
        assert conn.execute("SELECT COUNT(*) FROM recovery_outbox").fetchone()[0] == 0
    tick({**case.report(0), "families": [], "unknown": 1, "input_records": 3}, 200)
    assert active and conn.execute("SELECT verdict FROM verdicts").fetchone()[0] == "ABOVE"
    tick(case.report(4), 300)
    assert not active and attempts[-1] == attempts[0]
    assert conn.execute("SELECT COUNT(*) FROM recovery_outbox WHERE delivered_at IS NOT NULL").fetchone()[0] == 1
    # Complete empty current windows also resolve families no longer present.
    tick(case.report(5), 400)
    empty = {**case.report(0), "families": [], "input_records": 2}
    tick(empty, 500)
    assert not active
    assert conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 2
    conn.close()
