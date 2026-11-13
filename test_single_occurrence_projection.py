from dataclasses import replace
import pytest

from emitter import Emitter, connect, observe
from scheduled_receiver import ExecutionScope, run_id
from single_occurrence_projection import SingleOccurrenceProjection


def projection():
    return SingleOccurrenceProjection(
        ExecutionScope("urn:test:job", "r1", "urn:test:host", "a" * 64, "test-agent", "urn:test:workflow"),
        "urn:test:graph",
        "urn:test:manifest",
        "test-owner",
        600,
        "UTC",
        500,
        1,
    )


def catalogue(p):
    return dict(
        schema_version=1,
        source="declarations_only",
        graph=p.graph,
        membership=dict(status="required_identities_verified", required=[p.scope.job]),
        executable_admission="UNKNOWN",
        signed_run_status="UNKNOWN",
        firing_eligibility="UNKNOWN",
        jobs=[
            dict(
                iri=p.scope.job,
                name="fixture",
                revision=p.scope.revision,
                owner=p.owner,
                enabled=True,
                host=p.scope.host,
                target=p.target,
                event="urn:test:event",
                schedule="urn:test:schedule",
                interval_s=None,
                anchor_s=p.anchor_s,
                timezone=p.timezone,
                repeat_count=1,
            )
        ],
    )


def test_one_deadline_and_one_identity_even_after_missed_or_repeated_observations():
    p = projection()
    for now in (0, 500, 599.999):
        out = p.evaluate(catalogue(p), now)
        assert out["records"][0]["verdict"] == "BLOCKED"
        assert out["next_checks"] == {p.scope.job: 600}
    for now in (600, 601, 660, 86400, 253402300799):
        out = p.evaluate(catalogue(p), now)
        assert out["records"][0]["event_id"] == run_id(p.scope, 600)
        assert out["next_checks"] == {p.scope.job: None}


def test_real_outbox_deduplicates_original_identity_across_restart(tmp_path):
    p = projection()
    e = Emitter("one-slot", ["unused"], 60, p.owner, event="scheduled", baseline_emits=True)
    path = tmp_path / "state.sqlite"
    for now in (599, 600, 601, 86400):
        conn = connect(path)
        try:
            out = p.evaluate(catalogue(p), now)
            observe(conn, e, out["records"], now, scope=set(out["scope"]))
        finally:
            conn.close()
    conn = connect(path)
    try:
        rows = conn.execute("SELECT event_id FROM outbox").fetchall()
        assert [r[0] for r in rows] == [run_id(p.scope, 600)]
    finally:
        conn.close()


@pytest.mark.parametrize(
    "key,value",
    [
        ("repeat_count", 2),
        ("repeat_count", True),
        ("interval_s", 60),
        ("anchor_s", 601),
        ("anchor_s", True),
        ("owner", "other"),
    ],
)
def test_changed_declaration_is_unknown_not_empty(key, value):
    p = projection()
    body = catalogue(p)
    body["jobs"][0][key] = value
    with pytest.raises(ValueError):
        p.evaluate(body, 600)


def test_disabled_has_no_deadline_and_missing_membership_is_unknown():
    p = projection()
    body = catalogue(p)
    body["jobs"][0]["enabled"] = False
    assert p.evaluate(body, 600)["records"][0]["verdict"] == "BLOCKED"
    assert p.evaluate(body, 600)["next_checks"][p.scope.job] is None
    body["membership"]["required"] = []
    with pytest.raises(ValueError):
        p.evaluate(body, 600)
    for kwargs in (dict(effective_at=601), dict(repeat_count=True), dict(repeat_count=2)):
        with pytest.raises(ValueError):
            replace(p, **kwargs)
