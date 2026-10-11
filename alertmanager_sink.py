"""Alertmanager sink — deliver an emitter event as an alert, so the event
reaches a person through whatever already routes and escalates alerts.

The graph sink records that a reaction fired. It tells nobody. This sink is the
push half: it POSTs the event to Alertmanager's v2 API, and the deployment's
existing receivers route it from there (an owner label, an escalation chain).
chaski needs no listener of its own on the receiving side.

Identity is the alert's LABEL SET, and Alertmanager deduplicates on it. Every
label is a function of the frozen outbox payload (the emitter label, the event
id, the item), never of the send, so a re-send after a lost acknowledgement
refreshes the same alert instead of raising a second one.

A send is "received" only when Alertmanager says so twice: the POST returns
2xx, and a read-back of the active alerts finds one carrying this event id.
Anything else raises, and the emitter retries the identical send later.

An alert pushed through the API ends at its `endsAt`. This sink sets it to
`ttl_s` after the later of the event and the send, so a send delayed by
backoff still opens a live alert rather than one that is already resolved.
"""
from __future__ import annotations

import base64
import datetime as dt
import json
import re
import time
import urllib.parse
import urllib.request

USER_AGENT = "chaski/0.2"
DEFAULT_TTL_S = 24 * 3600
# Alertmanager label values are free text, but the routing labels this sink
# sets are names. Anything outside this set is dropped rather than guessed at.
_NAME = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")


def _iso(ts: float) -> str:
    t = dt.datetime.fromtimestamp(ts, dt.timezone.utc).replace(microsecond=0)
    return t.isoformat().replace("+00:00", "Z")


class AlertmanagerSink:
    """`alert` carries this emitter's alert fields: alertname, severity, and
    optionally ttl_s, summary (a format string over the event payload) and
    extra static labels."""

    def __init__(self, base: str, alert: dict, emitter_label: str,
                 user: str | None = None, password: str | None = None,
                 timeout: float = 15.0, clock=None, owner: str | None = None):
        self.base, self.alert, self.emitter_label = base.rstrip("/"), alert, emitter_label
        # Only deployment configuration may choose routing/severity. An
        # adapter's arbitrary extra fields cannot supply alert policy.
        self.kind_overrides = alert.get("kind_overrides", {})
        if not isinstance(self.kind_overrides, dict):
            raise ValueError("kind_overrides must be a mapping")
        for kind, policy in self.kind_overrides.items():
            if not isinstance(kind, str) or not _NAME.fullmatch(kind):
                raise ValueError("invalid override kind")
            if not isinstance(policy, dict) or not policy or set(policy) - {"severity", "alertname"}:
                raise ValueError("kind override permits only severity and alertname")
            if "severity" in policy and policy["severity"] not in {"info", "warning", "critical"}:
                raise ValueError("invalid override severity")
            if "alertname" in policy and (not isinstance(policy["alertname"], str) or
                                           not _NAME.fullmatch(policy["alertname"])):
                raise ValueError("invalid override alertname")
        # An event without a usable recipient goes to the emitter's owner, so it
        # starts at a person who can act rather than at the admin tier.
        self.owner = owner
        self.ttl_s = int(alert.get("ttl_s", DEFAULT_TTL_S))
        self.timeout = timeout
        self._auth = None
        if user and password:
            self._auth = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()
        self._clock = clock or time.time

    def _request(self, method: str, path: str, body=None) -> tuple[int, bytes]:
        req = urllib.request.Request(
            self.base + path, method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json", "User-Agent": USER_AGENT,
                     **({"Authorization": self._auth} if self._auth else {})})
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            return resp.status, resp.read()

    def labels(self, event: dict) -> dict:
        labels = {k: str(v) for k, v in (self.alert.get("labels") or {}).items()}
        labels.update({
            "alertname": self.alert["alertname"],
            "severity": self.alert["severity"],
            "emitter": self.emitter_label,
            "event_id": event.get("resolution_of") or event["event_id"],
            "item": event["item"],
        })
        kind = event.get("work_kind")
        if isinstance(kind, str) and _NAME.fullmatch(kind):
            labels.update(self.kind_overrides.get(kind, {}))
        for who in (event.get("recipient"), self.owner):
            if who and _NAME.match(str(who)):
                labels["keeper"] = str(who)
                break
        return labels

    def payload(self, event: dict) -> dict:
        start = event["observed_at"]
        end = max(start, self._clock()) + self.ttl_s
        if event.get("resolution_of"):
            end = event["resolved_at"]
        fields = {k: ("" if v is None else v) for k, v in event.items()}
        summary = self.alert.get("summary", "{event} for {item}").format_map(_Missing(fields))
        annotations = {"summary": summary}
        if event.get("evidence"):
            annotations["evidence"] = str(event["evidence"])[:1000]
        return {"labels": self.labels(event), "annotations": annotations,
                "startsAt": _iso(start), "endsAt": _iso(end)}

    def _received(self, event: dict) -> bool:
        identity = event.get("resolution_of") or event["event_id"]
        q = urllib.parse.urlencode([("filter", f'event_id="{identity}"'),
                                    ("filter", f'emitter="{self.emitter_label}"')])
        status, raw = self._request("GET", f"/api/v2/alerts?{q}")
        if status // 100 != 2 or not raw:
            raise ConnectionError(f"read-back: HTTP {status}, {len(raw)} bytes")
        rows = json.loads(raw)
        if not isinstance(rows, list) or any(not isinstance(a, dict) for a in rows):
            raise ConnectionError("read-back: malformed alert inventory")
        active = any(a.get("labels", {}).get("event_id") == identity for a in rows)
        return not active if event.get("resolution_of") else active

    def deliver(self, event: dict) -> None:
        status, _ = self._request("POST", "/api/v2/alerts", [self.payload(event)])
        if status // 100 != 2:
            raise ConnectionError(f"POST /api/v2/alerts: HTTP {status}")
        if not self._received(event):
            raise ConnectionError(f"{event['event_id']}: accepted but not active on read-back")

    def renew(self, event: dict) -> None:
        """Refresh the receiver's exact prior labels, never current policy labels."""
        if event.get("resolution_of") or event.get("prior"):
            raise ValueError("cannot renew recovery")
        alerts = self._prior_alerts(event)
        if len(alerts) != 1:
            raise ConnectionError("renewal prior identity absent or ambiguous")
        prior = alerts[0]
        if not all(isinstance(value, str) for value in prior["labels"].values()):
            raise ConnectionError("malformed renewal labels")
        start = prior.get("startsAt")
        if not isinstance(start, str):
            raise ConnectionError("missing renewal start")
        parsed = dt.datetime.fromisoformat(start.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.timestamp() > self._clock():
            raise ConnectionError("invalid renewal start")
        end = _iso(self._clock() + self.ttl_s)
        payload = {"labels": prior["labels"], "startsAt": start, "endsAt": end,
                   "annotations": prior.get("annotations", {})}
        status, _ = self._request("POST", "/api/v2/alerts", [payload])
        if status // 100 != 2:
            raise ConnectionError("renewal POST not accepted")
        received = self._prior_alerts(event)
        if len(received) != 1 or received[0]["labels"] != payload["labels"]:
            raise ConnectionError("renewal exact identity not received")
        observed_end = received[0].get("endsAt")
        if not isinstance(observed_end, str):
            raise ConnectionError("missing renewal expiry receipt")
        expiry = dt.datetime.fromisoformat(observed_end.replace("Z", "+00:00"))
        requested = dt.datetime.fromisoformat(end.replace("Z", "+00:00"))
        if expiry.tzinfo is None or expiry < requested or received[0].get("startsAt") != start:
            raise ConnectionError("renewal lease not received")

    def delivery_scope(self) -> dict:
        return {"kind": "alertmanager", "base": self.base, "emitter": self.emitter_label}

    def _prior_alerts(self, prior: dict) -> list[dict]:
        query = urllib.parse.urlencode([
            ("filter", "event_id=" + json.dumps(prior["event_id"])),
            ("filter", "emitter=" + json.dumps(prior["emitter"]))])
        status, raw = self._request("GET", "/api/v2/alerts?" + query)
        if status // 100 != 2 or not raw:
            raise ConnectionError("recovery read-back unavailable")
        alerts = json.loads(raw)
        if not isinstance(alerts, list) or any(not isinstance(a, dict) or
                not isinstance(a.get("labels"), dict) for a in alerts):
            raise ConnectionError("malformed recovery read-back")
        for alert in alerts:
            labels = alert["labels"]
            if (labels.get("event_id") != prior["event_id"] or
                    labels.get("emitter") != prior["emitter"] or labels.get("item") != prior["item"]):
                raise ConnectionError("ambiguous prior alert identity")
        return alerts

    def prepare_recovery(self, event: dict) -> dict:
        alerts = self._prior_alerts(event["prior"])
        if len(alerts) > 1:
            raise ConnectionError("multiple prior alert label sets")
        # An expired prior alert needs no fabricated label set or new POST.
        if not alerts:
            return {"absent": True, "delivery_scope": self.delivery_scope()}
        alert = alerts[0]
        if not all(isinstance(v, str) for v in alert["labels"].values()):
            raise ConnectionError("malformed prior alert labels")
        start = alert.get("startsAt")
        if not isinstance(start, str):
            raise ConnectionError("missing prior alert start")
        parsed = dt.datetime.fromisoformat(start.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.timestamp() > event["resolved_at"]:
            raise ConnectionError("prior alert start is not authoritative")
        return {"labels": alert["labels"], "startsAt": start,
                "delivery_scope": self.delivery_scope()}

    def resolve(self, event: dict) -> None:
        receipt = event["receipt"]
        if receipt.get("delivery_scope") != self.delivery_scope():
            raise ConnectionError("recovery receiver scope changed")
        if not receipt.get("absent"):
            payload = {"labels": receipt["labels"], "startsAt": receipt["startsAt"],
                       "endsAt": _iso(event["resolved_at"])}
            status, _ = self._request("POST", "/api/v2/alerts", [payload])
            if status // 100 != 2:
                raise ConnectionError("recovery POST not accepted")
        if self._prior_alerts(event["prior"]):
            raise ConnectionError("prior alert still active after recovery")


class _Missing(dict):
    """str.format_map that leaves an unknown field visible instead of raising."""

    def __missing__(self, key):
        return "{" + key + "}"


class ChainSink:
    """Deliver to each sink in order; received only when ALL have received.

    Order is the point: the graph sink goes first, so a person is told about a
    firing that already exists in the graph. A later failure retries the WHOLE
    chain, which is safe because every sink in it is idempotent on the event
    id; the earlier sinks re-receive what they already hold."""

    def __init__(self, sinks: list):
        self.sinks = sinks

    def setup_pending(self) -> bool:
        return any(getattr(s, "setup_pending", lambda: False)() for s in self.sinks)

    def setup(self) -> None:
        for s in self.sinks:
            if getattr(s, "setup_pending", lambda: False)():
                s.setup()

    def delivery_scope(self):
        scopes = [getattr(sink, "delivery_scope", lambda: None)() for sink in self.sinks]
        return {"kind": "chain", "sinks": scopes} if scopes and all(s is not None for s in scopes) else None

    def prepare_recovery(self, event: dict) -> dict:
        return {"delivery_scope": self.delivery_scope(), "sinks": [sink.prepare_recovery(event) for sink in self.sinks]}

    def resolve(self, event: dict) -> None:
        if event["receipt"].get("delivery_scope") != self.delivery_scope():
            raise ConnectionError("recovery sink chain changed")
        receipts = event["receipt"]["sinks"]
        if len(receipts) != len(self.sinks):
            raise ConnectionError("recovery sink chain changed")
        for sink, receipt in zip(self.sinks, receipts):
            sink.resolve({**event, "receipt": receipt})

    def deliver(self, event: dict) -> None:
        for s in self.sinks:
            s.deliver(event)

    def renew(self, event: dict) -> None:
        recipients = [s for s in self.sinks if callable(getattr(s, "renew", None))]
        if not recipients:
            raise ValueError("no lease-aware recipient")
        for sink in recipients:
            sink.renew(event)
