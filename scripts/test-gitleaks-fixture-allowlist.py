#!/usr/bin/env python3
"""Exercise the exact fixture allowance without printing scanner matches.

# arming: agent-invoked; requires the real gitleaks binary before publication.
"""
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class FixtureAllowance(unittest.TestCase):
    def scan(self, path, text, config=True):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            file = root / path
            file.parent.mkdir(parents=True)
            file.write_text(text)
            report = root / "findings.json"
            command = [shutil.which("gitleaks") or "gitleaks", "dir", ".",
                       "--redact", "--no-banner", "--log-level", "error",
                       "--exit-code", "42", "--report-format", "json",
                       "--report-path", str(report)]
            if config:
                command.extend(["--config", str(ROOT / ".gitleaks.toml")])
            result = subprocess.run(command, capture_output=True, cwd=root)
            self.assertIn(result.returncode, (0, 42), "scanner could not run")
            findings = json.loads(report.read_text()) if report.exists() else []
            return result.returncode, [row["RuleID"] for row in findings]

    def test_fixture_identifiers_and_default_positive(self):
        fixture = (ROOT / "fixtures/github/episodes.json").read_text()
        baseline, rules = self.scan("fixtures/github/episodes.json", fixture, config=False)
        self.assertEqual(baseline, 42)
        self.assertEqual(rules.count("generic-api-key"), 8)
        candidate, rules = self.scan("fixtures/github/episodes.json", fixture)
        self.assertEqual(candidate, 0)
        self.assertEqual(len(rules), 0)

    def test_same_identifiers_at_another_path_still_refuse(self):
        fixture = (ROOT / "fixtures/github/episodes.json").read_text()
        status, rules = self.scan("fixtures/github/other.json", fixture)
        self.assertEqual(status, 42)
        self.assertEqual(rules.count("generic-api-key"), 8)

    def test_another_field_at_the_exact_path_still_refuses(self):
        # Deliberately synthetic detector positive; never used as a credential.
        value = "a9b3c7d1e5f0" * 5 + "a9b3"
        status, rules = self.scan("fixtures/github/episodes.json", f'"api_key": "{value}"\n')
        self.assertEqual(status, 42)
        self.assertIn("generic-api-key", rules)

    def test_other_credential_rules_at_the_exact_path_still_refuse(self):
        # Assemble a fake token rather than adding a real credential to source.
        value = "ghp_" + "Aa1Bb2Cc3Dd4Ee5Ff6Gg7Hh8Ii9Jj0Kk1Ll2"
        status, rules = self.scan("fixtures/github/episodes.json",
                                  f'"githubActivityKey": "{value}"\n')
        self.assertEqual(status, 42)
        self.assertTrue(any(rule != "generic-api-key" for rule in rules))


if __name__ == "__main__":
    unittest.main()
