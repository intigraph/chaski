"""One actual adapter configuration plus pure projection; no graph or command execution."""

import argparse
from dataclasses import asdict
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
current = importlib.import_module("scheduled_catalogue_adapter")
fixture = importlib.import_module("test_single_occurrence_projection")
projection, catalogue = fixture.projection, fixture.catalogue

args = argparse.ArgumentParser()
args.add_argument("--before", type=Path)
args = args.parse_args()
p = projection()
with tempfile.TemporaryDirectory() as folder:
    path = Path(folder) / "config.json"
    config = dict(
        version=3,
        projection=asdict(p),
        reader=dict(executable="/unused/st", root=folder, env={}, timeout_s=2, output_limit=1048576),
    )
    path.write_text(json.dumps(config))
    path.chmod(0o600)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if args.before:
        spec = importlib.util.spec_from_file_location("scheduled_adapter_before", args.before)
        before = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(before)
        try:
            before.load_config(str(path), digest)
        except ValueError as exc:
            print("BASE: REFUSED", str(exc))
        else:
            raise AssertionError("base unexpectedly accepted adapter v3")
    loaded, _ = current.load_config(str(path), digest)
    identities = []
    for now in (599, 600, 601, 86400):
        out = loaded.evaluate(catalogue(p), now)
        record = out["records"][0]
        print("CANDIDATE at", now, "verdict", record["verdict"], "next", out["next_checks"][p.scope.job])
        if "event_id" in record:
            identities.append(record["event_id"])
    assert len(set(identities)) == 1
    print("One original run identity across all due observations; no rebasing.")
print("Fixture only; no reader process, graph, signing key or scheduler access.")
