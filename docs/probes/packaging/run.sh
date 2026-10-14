#!/usr/bin/env bash
set -euo pipefail
root=$(git rev-parse --show-toplevel)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
git archive 4f5e7914a6d0e31f6de7d54a50b73dc5e0ab3e55 | tar -x -C "$scratch"
redact() {
  python3 -c 'import sys; text=sys.stdin.read();
for value,label in [(sys.argv[1],"<base>"),(sys.argv[2],"<source>")]: text=text.replace(value,label)
print(text,end="")' "$scratch" "$root"
}
printf '%s\n' '$ uv build (isolated base 4f5e7914)'
set +e
uv build --project "$scratch" --out-dir "$scratch/dist" > "$scratch/base.log" 2>&1
result=$?
set -e
cat "$scratch/base.log" | redact
test "$result" -eq 2
grep -q 'neither `pyproject.toml` nor `setup.py`' "$scratch/base.log"
printf '%s\n' 'Expected baseline packaging failure verified.'
printf '$ uv build (candidate %s)\n' "$(git rev-parse --short HEAD)"
uv build --out-dir "$scratch/candidate-dist" > "$scratch/candidate.log" 2>&1
wheels=("$scratch"/candidate-dist/*.whl)
sources=("$scratch"/candidate-dist/*.tar.gz)
test "${#wheels[@]}" -eq 1
test "${#sources[@]}" -eq 1
printf 'Built %s and %s\n' "$(basename "${wheels[0]}")" "$(basename "${sources[0]}")"
printf '%s\n' '$ installed-wheel smoke (isolated environment, outside source checkout)'
uv run --no-project --isolated --python 3.11 --directory "$scratch" --with "${wheels[0]}" \
  python "$root/scripts/smoke-installed.py" 2>&1 | redact
