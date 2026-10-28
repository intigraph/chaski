# GitHub activity fixtures

`episodes.json` holds camayoc's recorded GitHub deliveries (recorded from the
public `intigraph/camayoc` repository on 2026-10-28; see camayoc
`tests/fixtures/github/README.md`) after mapping by camayoc's own ingress
mapper (`scripts/github_ingress.py episode_for`). Each entry is the `/episode`
body the ingress posts to Quipu, so the routing tests read the same facts
Quipu would store. `ping` and `star.created` are not facts and are absent.

Regenerate from a camayoc checkout:

    python3 scripts/github-fixtures.py ~/workspace/camayoc > fixtures/github/episodes.json

Tests derive second states (a red run, a second PR, a later push) from these
records by changing named fields (`test_github_routes.variant`). Verdict
records are synthetic, because no verdict writer exists yet.
