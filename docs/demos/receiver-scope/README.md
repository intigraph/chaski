# Receiver scope recovery demonstration

The paired probe uses Chaski application code and a temporary SQLite state file,
with two transport-only receivers. It contacts no real service.

Run from the repository root: `PYTHONPATH=. python3 docs/demos/receiver-scope/probe.py`.
The recording compares same-receiver recovery with changing to an empty receiver.
Before this change, the changed receiver was marked delivered while the original
alert stayed active. Afterward the recovery is refused, remains pending, and the
original alert stays visible. Same-receiver recovery still closes the original.

Before source: d40f41a4. See before.log and after.log for actual probe outputs.
These controls do not prove native receiver authentication or scheduled deployment.
