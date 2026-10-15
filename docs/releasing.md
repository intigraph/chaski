# Releasing Chaski

The distribution is `chaski-reactor`; the installed command is `chaski`.
The unrelated `chaski` project on PyPI is a different package.

## Version and artifacts

Release Please creates a version PR from conventional commits. Its Python
strategy updates `pyproject.toml`; the additional TOML updater changes only
Chaski's own entry in `uv.lock`. The initial manifest starts at `0.0.0`, so
its first release is `0.1.0`. Keep the action pinned: the lock selector uses
the scalar representation in Release Please 17.3.0.

The version PR must pass the normal test and wheel-install matrix before
merging. Release Please creates a draft tag and calls the publication workflow
in the same run, since a workflow-token-created tag does not trigger another
push workflow. A directly pushed stable tag uses the same build job.

Publication checks the tag against both source versions, runs the locked
suite and lint, builds the wheel and source archive, checks their metadata,
and exercises the installed wheel outside the checkout. The GitHub release
contains the two distributions and `checksums.txt`. Publication refuses missing
or empty changelog sections. A draft is published only after these checks pass.

To rehearse locally without publishing:

```sh
uv sync --locked
uv run --locked python -m pytest -q
uv build
python scripts/release-artifacts.py v0.1.0 --dist dist
(cd dist && sha256sum --check checksums.txt)
```

Use the actual project version in place of `v0.1.0`. This checks artifacts;
it does not establish that a host installed them.

## Automation prerequisites

Workflow-token PR creation needs the repository's Actions PR-creation setting
and the operator's scoped approval path for bot-generated CI. Configure these
as a paired change; preserve branch protection and public-fork approval rules.

## Optional PyPI publication

GitHub artifact publication works without a PyPI account change. PyPI upload
is disabled until the account owner registers the project and its trusted
publishers, then explicitly sets the repository variable
`PYPI_PUBLISH_ENABLED=true`.

The publisher settings are owner `scbrown`, repository `chaski`, project
`chaski-reactor`, and environment `pypi`. Register `release-pr.yml` for the
normal version-PR lane and `release.yml` for direct stable tags. Each top-level
workflow has its own isolated publisher job, with no source checkout or build
tool installation. The reusable build job is not the trusted publisher:
[PyPI does not support registering reusable workflows](https://docs.pypi.org/trusted-publishers/troubleshooting/#reusable-workflows-on-github).
No long-lived upload token is needed.

Before enabling uploads, confirm the account registration, environment policy
and both workflow filenames. An unpublished project-name lookup does not
prove that a name can be registered.

## Stopping and rollback

Disable PyPI publication by removing the opt-in variable. Pause Release Please
by disabling its workflow; published immutable artifacts remain available.
For a defective release, mark its GitHub release as a prerelease, stop automated
consumers from advancing, and publish a corrected version. Do not overwrite
checksums or silently replace an existing artifact. Consumer rollback uses a
previous verified artifact through that consumer's normal update lane.
