#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$root"
if [ "$(git rev-parse --is-shallow-repository)" != false ]; then
  echo "A full clone is required for the historical baseline." >&2; exit 1
fi
# A missing commit must not masquerade as a missing release checker.
git cat-file -e c56140f:pyproject.toml
probe_dir="$(mktemp -d)"
trap 'rm -rf "$probe_dir"' EXIT

printf '%s\n' '$ git cat-file -e c56140f:scripts/release-artifacts.py'
if git cat-file -e c56140f:scripts/release-artifacts.py; then
  echo 'Unexpected baseline release checker'; exit 1
else
  echo 'Baseline: no release checker in the packaging proposal.'
fi
printf '\n%s\n' '$ python3 scripts/release-artifacts.py v9.9.9'
if python3 scripts/release-artifacts.py v9.9.9; then
  echo 'Unexpected acceptance of a mismatched tag'; exit 1
else
  echo 'Mismatched tag refused before building.'
fi
printf '\n%s\n' '$ python3 scripts/release-artifacts.py v0.1.0'
python3 scripts/release-artifacts.py v0.1.0
printf '\n%s\n' '$ uv build; verify artifact metadata; verify checksums'
if ! uv build --out-dir "$probe_dir/dist" >"$probe_dir/build.log" 2>&1; then
  cat "$probe_dir/build.log"; exit 1
fi
python3 scripts/release-artifacts.py v0.1.0 --dist "$probe_dir/dist"
(cd "$probe_dir/dist" && sha256sum --check checksums.txt)
echo 'Local rehearsal only: no tag, registry upload or host installation.'
