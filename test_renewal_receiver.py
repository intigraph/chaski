"""Receiver-side lease receipts preserve the existing exact label set."""
import copy
import json

import pytest

from alertmanager_sink import AlertmanagerSink, _iso


def receiver(stale=False):
    sink = AlertmanagerSink("https://example.invalid",
                            {"alertname": "CurrentPolicy", "severity": "critical",
                             "ttl_s": 60}, "lease", clock=lambda: 100)
    event = {"event_id": "identity", "emitter": "lease", "item": "family",
             "observed_at": 20, "recipient": "crew"}
    prior = {"labels": {"alertname": "OriginalPolicy", "severity": "warning",
                        "event_id": "identity", "emitter": "lease", "item": "family",
                        "keeper": "crew"},
             "startsAt": _iso(20), "endsAt": _iso(120), "annotations": {}}
    posts = []
    current = copy.deepcopy(prior)
    def request(method, path, body=None):
        if method == "POST":
            posts.append(copy.deepcopy(body[0]))
            if not stale:
                current.update(body[0])
            return 200, b""
        return 200, json.dumps([current]).encode()
    sink._request = request
    return sink, event, prior, posts


def test_renewal_uses_prior_labels_and_requires_extended_expiry():
    sink, event, prior, posts = receiver()
    sink.renew(event)
    assert posts[0]["labels"] == prior["labels"]
    assert posts[0]["startsAt"] == prior["startsAt"]
    assert posts[0]["endsAt"] == _iso(160)


def test_accepted_post_with_old_expiry_is_not_a_receipt():
    sink, event, _, _ = receiver(stale=True)
    with pytest.raises(ConnectionError, match="lease not received"):
        sink.renew(event)


def test_absent_or_ambiguous_prior_never_posts_new_policy_labels():
    for rows in ([], [{"labels": {"event_id": "other"}}]):
        sink, event, _, posts = receiver()
        sink._request = lambda *args: (200, json.dumps(rows).encode())
        with pytest.raises(ConnectionError):
            sink.renew(event)
        assert posts == []
