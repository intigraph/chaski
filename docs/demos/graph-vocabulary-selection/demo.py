#!/usr/bin/env python3
"""Bounded source fixture: explicit writer mode versus receiver vocabulary."""
import importlib.util
from pathlib import Path
import re
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
import graph_sink  # noqa: E402

REACTION = {"label": "fixture", "schedule": "PT1M", "owner": "fixture",
            "condition": "ASK {}", "severity": "info", "action": ["alert"],
            "comment": "Isolated source fixture"}
EVENT = {"event_id": "fixture-1", "item": "urn:fixture:focus", "observed_at": 1790400000}
SANCTIONED = {graph_sink.NS + name for name in ("Reaction", "ReactionFiring")}


def admitted(instance):
    types = re.findall(r"\ba <([^>]+)>", instance.reaction_turtle() + instance.firing_turtle(EVENT))
    return len(types) == 2 and set(types) <= SANCTIONED


with tempfile.TemporaryDirectory() as scratch:
    path = Path(scratch) / "baseline.py"
    path.write_bytes(subprocess.check_output([
        "git", "-C", str(ROOT), "show",
        "310ba13445c61a6d53821f3ea4575a60b6e9651d:graph_sink.py"]))
    spec = importlib.util.spec_from_file_location("baseline_sink", path)
    baseline = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(baseline)
    old = baseline.QuipuFiringSink("https://unused.example.test", None, REACTION, ns="urn:fixture:")
    legacy = graph_sink.QuipuFiringSink("https://unused.example.test", None, REACTION, ns="urn:fixture:")
    canonical = graph_sink.QuipuFiringSink(
        "https://unused.example.test", None, REACTION, ns="urn:fixture:",
        vocabulary="canonical-vocabulary")
    assert not admitted(old) and admitted(legacy) and not admitted(canonical)
    assert legacy.delivery_scope() != canonical.delivery_scope()
    assert old.reaction_iri() == legacy.reaction_iri() == canonical.reaction_iri()
    print("before: canonical writer; compatible with legacy receiver=False")
    print("after: explicit default legacy-vocabulary; compatible=True")
    print("canonical opt-in: compatible=False until receiver profile activation")
    print("PASS: same instance identity; distinct retained vocabulary scopes; no HTTP or data writes")
