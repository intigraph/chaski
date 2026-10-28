#!/usr/bin/env python3
"""Run with only the built wheel installed, from outside the source checkout."""
import base64
import hashlib
import importlib
from importlib import metadata
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

MODULES = ["chaski_prime", "chaski_prime_state", "chaski_primer",
           "alertmanager_sink", "change_frames", "change_stream", "chaski", "command_sink", "consumer_lease",
           "demo", "emitter", "external_event", "failure_family_adapter", "graph_sink", "incremental", "once"]


def main():
    prefix = Path(sys.prefix).resolve()
    if Path.cwd().resolve() == Path(__file__).resolve().parents[1]:
        raise RuntimeError("run the installed smoke outside the source checkout")
    distribution = metadata.distribution("chaski-reactor")
    if "Wheel-Version:" not in (distribution.read_text("WHEEL") or ""):
        raise RuntimeError("distribution is not a built wheel")
    records = {str(record): record for record in distribution.files or []}
    for name in MODULES:
        module = importlib.import_module(name)
        record = records.get(name + ".py")
        actual = Path(module.__file__).resolve()
        if not record or actual != Path(distribution.locate_file(record)).resolve():
            raise RuntimeError(f"{name} did not come from the installed wheel")
        digest = base64.urlsafe_b64encode(hashlib.sha256(actual.read_bytes()).digest()).rstrip(b"=").decode()
        if not record.hash or record.hash.mode != "sha256" or record.hash.value != digest:
            raise RuntimeError(f"{name} differs from the wheel RECORD")
    for name, identity in [("chaski", "a post-commit and time-driven reactor"),
                           ("chaski-event", "existing outbox"), ("chaski-once", "--metrics")]:
        program = shutil.which(name)
        if not program or not Path(program).is_relative_to(prefix):
            raise RuntimeError(f"{name} is not the installed console script")
        result = subprocess.run([program, "--help"], capture_output=True, text=True, check=True, timeout=20)
        if identity not in result.stdout:
            raise RuntimeError(f"{name} identity missing")
    prime = json.loads(subprocess.check_output(["chaski", "prime", "--static", "--json"], text=True))
    if (prime.get("schema") != "stack-prime/1" or prime.get("tool") != "chaski"
            or prime.get("version") != metadata.version("chaski-reactor")
            or prime.get("live", {}).get("status") != "skipped"):
        raise RuntimeError("installed prime contract/version differs")
    version = subprocess.check_output(["chaski", "--version"], text=True).strip()
    if version != "chaski " + metadata.version("chaski-reactor"):
        raise RuntimeError("console version differs from wheel metadata")
    with tempfile.TemporaryDirectory(prefix="chaski-wheel-smoke-") as directory:
        receipt = str(Path(directory) / "demo.json")
        def call(*args):
            return subprocess.check_output(["chaski", "demo", *args, "--state", receipt], text=True)
        marker = "wheel-canary"
        if marker in call("status"):
            raise RuntimeError("fresh state already contains the marker")
        call("run", "--focus", marker)
        report = json.loads(call("status"))
        if (report["focus"] != marker or report["transitions"] != 2
                or any(report[key] is not True for key in ("fired", "unknown_preserved", "resolved"))):
            raise RuntimeError("installed canary round trip failed")
    print(f"{version}: {len(MODULES)} installed modules; 3 console identities; "
          "canary fired, UNKNOWN preserved, resolved")


if __name__ == "__main__":
    main()
