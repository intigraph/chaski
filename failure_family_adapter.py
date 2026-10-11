"""Threshold adapter over desire-path's own bounded command-family aggregation.

Policy is a governed configuration node's single JSON description, read over
Quipu's public query API. It is never guessed from a missing or failed read.
Output includes hashed family identity and numerical evidence only.
"""

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import shutil
import tempfile
import re
import subprocess
import urllib.request

FAMILY = re.compile(r"[0-9a-f]{64}\Z")
OWNER = re.compile(r"[a-z][a-z0-9_-]{0,63}\Z")


def validate_policy(policy):
    if not isinstance(policy, dict):
        raise ValueError("policy must be an object")
    for key, low, high in (("default_threshold", 1, 10000), ("window_seconds", 60, 86400)):
        value = policy.get(key)
        if type(value) is not int or not low <= value <= high:
            raise ValueError("invalid numerical threshold policy")
    overrides = policy.get("family_thresholds", {})
    if not isinstance(overrides, dict) or len(overrides) > 1000:
        raise ValueError("invalid threshold overrides")
    for family, value in overrides.items():
        if not FAMILY.fullmatch(family) or type(value) is not int or not 1 <= value <= 10000:
            raise ValueError("invalid family override")
    owners = policy.get("tool_owners", {})
    if not isinstance(owners, dict):
        raise ValueError("invalid tool ownership")
    for owner in [policy.get("default_owner", ""), *owners.values()]:
        if not isinstance(owner, str) or not OWNER.fullmatch(owner) or owner in {"human", "stiwi"}:
            raise ValueError("policy requires crew ownership")
    return policy


def policy_from_graph(server, iri):
    if not re.fullmatch(r"https?://[^<>\s\"{}]+", iri):
        raise ValueError("invalid governed policy IRI")
    # The writer enforces governance; the reader also requires one directly
    # asserted ConfigSetting type before consuming its JSON configuration.
    namespace = iri.rsplit("/", 1)[0] + "/"
    type_query = f"SELECT ?type WHERE {{ <{iri}> a ?type . FILTER(?type = <{namespace}ConfigSetting>) }}"
    typed_request = urllib.request.Request(
        server.rstrip("/") + "/query",
        data=json.dumps({"query": type_query}).encode(),
        headers={"Content-Type": "application/json", "X-Quipu-Client": "chaski"},
    )
    with urllib.request.urlopen(typed_request, timeout=15) as response:
        typed = json.load(response)
    if "error" in typed or typed.get("truncated") or not typed.get("rows"):
        raise ValueError("governed policy type UNKNOWN")
    query = f"SELECT ?value WHERE {{ <{iri}> <http://www.w3.org/2000/01/rdf-schema#comment> ?value }}"
    request = urllib.request.Request(
        server.rstrip("/") + "/query",
        data=json.dumps({"query": query}).encode(),
        headers={"Content-Type": "application/json", "X-Quipu-Client": "chaski"},
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        data = json.load(response)
    if "error" in data or data.get("truncated") or not isinstance(data.get("rows"), list):
        raise ValueError("policy query UNKNOWN")
    if len(data["rows"]) != 1:
        raise ValueError("policy missing or ambiguous")
    return validate_policy(json.loads(data["rows"][0]["value"]))


def verdicts(report, policy):
    validate_policy(policy)
    if not isinstance(report, dict) or report.get("window_seconds") != policy["window_seconds"]:
        raise ValueError("aggregation window mismatch")
    for key in ("unknown", "excluded"):
        if type(report.get(key)) is not int or report[key] < 0:
            raise ValueError("missing coverage accounting")
    families = report.get("families")
    if not isinstance(families, list) or len(families) > 10000:
        raise ValueError("invalid family inventory")
    records, seen = [], set()
    for family in families:
        identifier = family.get("id")
        count = family.get("current_window")
        tool = family.get("tool")
        if (
            not isinstance(identifier, str)
            or not FAMILY.fullmatch(identifier)
            or identifier in seen
            or type(count) is not int
            or count < 0
            or not isinstance(tool, str)
        ):
            raise ValueError("invalid or duplicate family")
        seen.add(identifier)
        threshold = policy.get("family_thresholds", {}).get(identifier, policy["default_threshold"])
        owner = policy.get("tool_owners", {}).get(tool, policy["default_owner"])
        records.append(
            {
                "item": identifier,
                "verdict": "ABOVE" if count >= threshold else "BELOW",
                "event_id": identifier,
                "owner": owner,
                "evidence": (f"count={count}; threshold={threshold}; "
                             f"window={policy['window_seconds']}s; "
                             f"unsupported_records={report['unknown']}"),
            }
        )
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quipu-server", required=True)
    parser.add_argument("--policy-iri", required=True)
    parser.add_argument("--dp", default="dp")
    parser.add_argument("--db", required=True)
    args = parser.parse_args()
    policy = policy_from_graph(args.quipu_server, args.policy_iri)
    end = dt.datetime.now(dt.timezone.utc)
    # Historical identities remain visible after their hour expires. Complete
    # successful inventories permit the emitter's opt-in missing_is_from;
    # any adapter failure never clears an existing firing.
    start = end - dt.timedelta(days=7)
    # Explicit paths survive a clean child HOME. Operator remote-store config
    # must not silently redirect this local, read-only measurement.
    binary = shutil.which(args.dp)
    if binary is None:
        raise RuntimeError("desire-path executable unavailable")
    database = str(Path(args.db).resolve())
    with tempfile.TemporaryDirectory(prefix="failure-family-reader-") as home:
        child_env = dict(os.environ, HOME=home, XDG_CONFIG_HOME=home)
        result = subprocess.run(
            [
                str(Path(binary).resolve()),
                "--db",
                database,
                "--json",
                "failure-families",
                "--since",
                start.isoformat(),
                "--until",
                end.isoformat(),
                "--window",
                f"{policy['window_seconds']}s",
            ],
            env=child_env,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
    if result.returncode:
        # Do not echo dp stderr: it may contain an operational path or raw input.
        raise RuntimeError("desire-path aggregation failed")
    print(json.dumps(verdicts(json.loads(result.stdout), policy)))


if __name__ == "__main__":
    main()
