"""Writer selection is explicit and never changes retained receiver identity."""
import json
from unittest import mock

import pytest

import chaski
import graph_sink as gs


REACTION = {"label": "fixture", "schedule": "PT1M", "owner": "fixture",
            "severity": "info", "action": ["alert"], "condition": "ASK {}",
            "comment": "fixture"}


def sink(**kwargs):
    return gs.QuipuFiringSink("https://unused.example.test", None, REACTION,
                             ns="urn:fixture:", **kwargs)


def test_default_legacy_writer_uses_sanctioned_terms_not_instance_base():
    instance = sink()
    reaction = instance.reaction_turtle()
    firing = instance.firing_turtle({"event_id": "event", "item": "urn:focus",
                                    "observed_at": 1790400000})
    assert "<urn:fixture:reaction-fixture>" in reaction
    for name in ("Reaction", "schedule", "condition", "owner", "action"):
        assert "<" + gs.NS + name + ">" in reaction
    for name in ("ReactionFiring", "firedBy", "eventId", "focus", "startedAt"):
        assert "<" + gs.NS + name + ">" in firing
    assert gs.QUIPU not in reaction + firing and gs.CORE not in reaction + firing
    assert instance.delivery_scope()["vocabulary"] == "aegis-legacy/v1"


def test_canonical_opt_in_keeps_published_terms_and_a_distinct_scope():
    canonical = sink(vocabulary="canonical-vocabulary")
    assert "<" + gs.QUIPU + "Reaction>" in canonical.reaction_turtle()
    assert canonical.delivery_scope()["vocabulary"] == gs.VOCABULARY_VERSION
    assert canonical.delivery_scope() != sink().delivery_scope()


def test_legacy_recovery_uses_selected_predicate_without_moving_identity():
    instance = sink()
    prior = {"emitter": "fixture", "event_id": "event"}
    with mock.patch.object(instance, "_post", return_value={
            "rows": [{"p": gs.NS + "eventId", "e": "event"}]}):
        receipt = instance.prepare_recovery({"prior": prior, "resolved_at": 1790400000})
    assert receipt["predicate"] == gs.NS + "resolvedAt"
    assert receipt["iri"] == gs.firing_iri("fixture", "event", "urn:fixture:")


@pytest.mark.parametrize("value", ["auto", "", None, True, ["legacy-vocabulary"]])
def test_unknown_writer_selection_refuses_without_http(value):
    with mock.patch("urllib.request.urlopen", side_effect=AssertionError("no HTTP")):
        with pytest.raises(ValueError, match="vocabulary"):
            sink(vocabulary=value)


def test_factory_default_and_explicit_selection(tmp_path):
    definition = {"label": "fixture", "command": ["false"], "schedule": "PT1M",
                  "owner": "fixture", "sink": {"graph": {
                      "severity": "info", "action": ["notice"], "condition": "ASK {}",
                      "comment": "fixture"}}}
    config = tmp_path / "emitters.yaml"
    for selected in (None, "legacy-vocabulary", "canonical-vocabulary"):
        graph = definition["sink"]["graph"]
        if selected is None:
            graph.pop("vocabulary", None)
        else:
            graph["vocabulary"] = selected
        config.write_text(json.dumps({"emitters": [definition]}))
        [(emitter, settings)] = chaski.load_emitters(config)
        instance = chaski.make_sink(emitter, settings, "https://unused.example.test")
        assert instance.vocabulary == (selected or "legacy-vocabulary")
    definition["sink"]["graph"]["vocabulary"] = "auto"
    config.write_text(json.dumps({"emitters": [definition]}))
    with pytest.raises(chaski.RuleError, match="vocabulary"):
        chaski.load_emitters(config)


def test_old_canonical_recovery_receipt_never_rebinds_to_legacy():
    instance = sink()
    prior = sink(vocabulary="canonical-vocabulary").delivery_scope()
    event = {"receipt": {"delivery_scope": prior}, "resolved_at": 1790400000}
    with mock.patch.object(instance, "_post", side_effect=AssertionError("no replay")):
        with pytest.raises(ConnectionError, match="scope changed"):
            instance.resolve(event)
