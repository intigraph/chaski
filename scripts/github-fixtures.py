#!/usr/bin/env python3
"""Regenerate fixtures/github/episodes.json from a camayoc checkout.

Maps camayoc's recorded GitHub deliveries (tests/fixtures/github/deliveries)
through camayoc's own ingress mapper, so chaski's routing tests read exactly
the facts the ingress writes to Quipu:

    python3 scripts/github-fixtures.py ~/workspace/camayoc > fixtures/github/episodes.json
"""
import json
import subprocess
import sys
from pathlib import Path


def main() -> int:
    cam = Path(sys.argv[1]).resolve()
    sys.path.insert(0, str(cam / "scripts"))
    import github_ingress as gi

    rev = subprocess.run(["git", "-C", str(cam), "rev-parse", "--short", "HEAD"], capture_output=True,
                         text=True).stdout.strip() or "unknown"
    episodes = []
    for f in sorted((cam / "tests/fixtures/github/deliveries").glob("*.json")):
        d = json.loads(f.read_text())
        try:
            body, _ = gi.episode_for(d["headers"].get("X-GitHub-Event"), d["body"], actor="camayoc-github-ingress")
        except Exception as exc:  # ping / unmapped deliveries are not facts
            print(f"{f.name}: {type(exc).__name__}: {exc}", file=sys.stderr)
            continue
        episodes.append({"delivery": f.name, "episode": body})
    json.dump({"source": f"camayoc {rev} tests/fixtures/github/deliveries mapped by scripts/github_ingress.py "
                         "episode_for", "episodes": episodes}, sys.stdout, indent=1, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
