"""Graph sink — deliver an emitter event as an aegis:ReactionFiring in quipu.

The receiver IS the graph, and it deduplicates because the firing's identity is
a function of (reaction label, event id), never of the send:

    firing IRI = <ns>firing-<sha256(json([label, event_id]))[:32]>

and the write is a `/knot` with `replace_snapshot` keyed on that IRI, built only
from the FROZEN outbox payload (`startedAt` is the payload's `observed_at`, not
the send time). A re-send after a lost acknowledgement therefore rewrites the
same bytes under the same key: one firing, however many sends.

A write is "received" only when the graph says so twice: the `/knot` response
carries a transaction id and conforms, and a read-back finds the firing's
eventId. A timeout or an empty body is NOT a failed write (it may have landed),
so it raises and the emitter retries the identical write later, which is safe
exactly because the write is idempotent.

The Reaction the firings point at is written once per sink (its own snapshot),
because the shape requires `firedBy` to be a typed Reaction.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import socket
import urllib.request

# Instance identity stays deployment-selected until its separate migration.
NS = "http://aegis.gastown.local/ontology/"
QUECHUA_NS = "https://scbrown.github.io/quechua/ns#"  # historical read alias
CORE = "https://w3id.org/intigraph/camayoc/core/ns#"
QUIPU = "https://w3id.org/intigraph/quipu/ns#"
QUECHUA = "https://w3id.org/intigraph/quechua/ns#"
# ownership.json SHA256 a389be53b74259d0c515ff6db16d602474dc5fe38dc6cc47f0f54a2d1347d34e.
_VOCABULARY = {**{name: QUIPU + name for name in
    ("Reaction", "ReactionFiring", "owner", "schedule", "severity")},
    **{name: CORE + name for name in ("action", "condition", "eventId", "eventTypes",
        "firedBy", "focus", "startedAt", "triggerKind", "resolvedAt")}}
VOCABULARY_VERSION = "intigraph-owned-a389be53/v1"


def term(name):
    """An exact published vocabulary owner; never use the instance base."""
    return _VOCABULARY[name]


USER_AGENT = "chaski/0.2"
# A caller KIND of its own, so the emitter's writes are separable from chaski's
# rule reads in quipu's per-caller accounting.
CLIENT_LABEL = "chaski-emitter"


_UNSAFE_HEADER = re.compile(r"[^\x21-\x7e ]")


def provenance_headers(env=None) -> dict:
    """Structured write provenance (aegis-7zp4rc): chaski is a service, so it
    names itself (agent=chaski, harness=service, host); QUIPU_AGENT /
    QUIPU_HARNESS / QUIPU_HOST override. No session or model: a service has
    neither, and quipu's coverage metric does not expect them. Values are
    single-line printable ASCII, capped at 128 characters."""
    env = os.environ if env is None else env
    raw = {"Agent": env.get("QUIPU_AGENT") or "chaski",
           "Harness": env.get("QUIPU_HARNESS") or "service",
           "Host": env.get("QUIPU_HOST") or socket.gethostname()}
    out = {}
    for field, value in raw.items():
        text = _UNSAFE_HEADER.sub("", str(value)).strip()[:128]
        if text:
            out[f"X-Quipu-{field}"] = text
    return out


def firing_iri(label: str, event_id: str, ns: str = NS) -> str:
    key = json.dumps([label, event_id], separators=(",", ":"))
    return f"{ns}firing-{hashlib.sha256(key.encode()).hexdigest()[:32]}"


def _lit(s: str) -> str:
    return json.dumps(s)  # a valid Turtle string literal: quotes and escapes


def _datetime_value(value) -> str:
    if (not isinstance(value, dict) or set(value) != {"value", "datatype"} or
            value["datatype"] not in {"xsd:dateTime", "http://www.w3.org/2001/XMLSchema#dateTime"} or
            not isinstance(value["value"], str)):
        raise ConnectionError("graph recovery is not an explicit typed dateTime")
    parsed = dt.datetime.fromisoformat(value["value"].replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ConnectionError("graph recovery has no timezone")
    return value["value"]


class QuipuFiringSink:
    """`reaction` carries the aegis:Reaction fields for this emitter:
    label, schedule, condition, severity, action (list), owner, comment."""

    def __init__(self, base: str, token: str | None, reaction: dict,
                 ns: str = NS, timeout: float = 30.0):
        self.base, self.token, self.reaction, self.ns, self.timeout = base.rstrip("/"), token, reaction, ns, timeout
        self._reaction_written = False

    def _post(self, path: str, body: dict) -> dict:
        req = urllib.request.Request(
            self.base + path, data=json.dumps(body).encode(), method="POST",
            headers={"Content-Type": "application/json", "User-Agent": USER_AGENT,
                     "X-Quipu-Client": CLIENT_LABEL, **provenance_headers(),
                     **({"Authorization": f"Bearer {self.token}"} if self.token else {})})
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            raw = resp.read()
        if not raw:
            raise ConnectionError(f"{path}: empty response (the write may have landed)")
        return json.loads(raw)

    def _knot(self, turtle: str, snapshot: str) -> None:
        out = self._post("/knot", {"turtle": turtle, "actor": "chaski", "source": snapshot,
                                   "replace_snapshot": True, "snapshot": snapshot})
        if out.get("conforms") is False:
            raise ValueError(f"quipu refused {snapshot}: {str(out)[:300]}")
        if not out.get("tx_id"):
            raise ConnectionError(f"{snapshot}: no tx_id in {str(out)[:200]} (indeterminate)")

    def reaction_iri(self) -> str:
        return f"{self.ns}reaction-{self.reaction['label']}"

    def reaction_turtle(self) -> str:
        r = self.reaction
        actions = ", ".join(_lit(a) for a in r["action"])
        kind = r.get("trigger_kind", "schedule")
        if kind not in {"schedule", "event"}:
            raise ValueError("reaction trigger kind must be schedule or event")
        event_types = (f"  <{term('eventTypes')}> {_lit(r['event_types'])} ;\n"
                       if kind == "event" else "")
        return (
            f"<{self.reaction_iri()}> a <{term('Reaction')}> ;\n"
            f"  <http://www.w3.org/2000/01/rdf-schema#label> {_lit(r['label'])} ;\n"
            f"  <{term('triggerKind')}> {_lit(kind)} ; <{term('schedule')}> {_lit(r['schedule'])} ;\n"
            f"{event_types}"
            f"  <{term('condition')}> {_lit(r['condition'])} ;\n"
            f"  <{term('severity')}> {_lit(r['severity'])} ; <{term('action')}> {actions} ;\n"
            f"  <{term('owner')}> {_lit(r['owner'])} ;\n"
            f"  <http://www.w3.org/2000/01/rdf-schema#comment> {_lit(r['comment'])} .\n"
        )

    def firing_turtle(self, event: dict) -> str:
        started = dt.datetime.fromtimestamp(event["observed_at"], dt.timezone.utc)
        started = started.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        focus = event["item"] if "://" in event["item"] else f"{self.ns}{event['item']}"
        return (
            f"<{firing_iri(self.reaction['label'], event['event_id'], self.ns)}> a <{term('ReactionFiring')}> ;\n"
            f"  <{term('firedBy')}> <{self.reaction_iri()}> ;\n"
            f"  <{term('focus')}> <{focus}> ;\n"
            f"  <{term('startedAt')}> \"{started}\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ;\n"
            f"  <{term('eventId')}> {_lit(event['event_id'])} .\n"
        )

    def _term_aliases(self, name):
        return tuple(dict.fromkeys((term(name), NS + name, self.ns + name,
                                    QUECHUA_NS + name, QUECHUA + name)))

    def _expand(self, term):
        """Expand a compact IRI from a quipu /query answer to a full IRI.

        Quipu has returned the aegis namespace both as "aegis:" and, later, as "a:";
        either names this sink's namespace only when it is the legacy aegis one.
        "quechua:" names the Quechua namespace. Anything else (a full IRI, an
        unknown prefix, a non-string) is returned unchanged, so it can only match
        if it already is the exact full IRI.
        """
        if not isinstance(term, str):
            return term
        prefixes = {"quechua:": QUECHUA_NS}
        if self.ns == NS:
            prefixes.update({"aegis:": NS, "a:": NS})
        for prefix, namespace in prefixes.items():
            if term.startswith(prefix):
                return namespace + term[len(prefix):]
        return term

    def _received(self, event: dict) -> bool:
        iri = firing_iri(self.reaction["label"], event["event_id"], self.ns)
        # Explicit UNION preserves literal values on older native readers.
        alternatives = [f"{{ <{iri}> <{predicate}> ?e }}"
                        for predicate in self._term_aliases("eventId")]
        q = "SELECT ?e WHERE { " + " UNION ".join(alternatives) + " }"
        out = self._post("/query", {"query": q})
        rows = out.get("rows")
        if out.get("error") or not isinstance(rows, list) or out.get("truncated", False) is not False:
            raise ConnectionError(f"read-back returned no rows field: {str(out)[:200]}")
        return len(rows) == 1 and isinstance(rows[0], dict) and rows[0].get("e") == event["event_id"]

    def setup_present(self) -> bool:
        """Direct type read-back for a one-shot's retained setup receipt."""
        types = ", ".join("<" + value + ">" for value in self._term_aliases("Reaction"))
        query = (f"SELECT (STR(?kind) AS ?type) WHERE {{ <{self.reaction_iri()}> a ?kind . "
                 f"FILTER(?kind IN ({types})) }}")
        req = urllib.request.Request(
            self.base + "/query", data=json.dumps({"query": query}).encode(),
            headers={"Content-Type": "application/json", "User-Agent": USER_AGENT,
                     "X-Quipu-Client": CLIENT_LABEL})
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            out = json.load(resp)
        rows = out.get("rows")
        if out.get("error") or out.get("truncated") or not isinstance(rows, list):
            raise ConnectionError("setup read-back UNKNOWN")
        expected = self._term_aliases("Reaction")
        if any(not isinstance(row, dict) or self._expand(row.get("type")) not in expected for row in rows):
            raise ConnectionError("setup read-back has unexpected type")
        return bool(rows)

    def setup_pending(self) -> bool:
        """The Reaction every firing points at is owed once per process. The
        emitter spends a write-budget slot on it, separately from any event."""
        return not self._reaction_written

    def setup(self) -> None:
        self._knot(self.reaction_turtle(), f"chaski:reaction:{self.reaction['label']}")
        self._reaction_written = True

    def deliver(self, event: dict) -> None:
        if not self._reaction_written:
            # Called outside deliver_pending (a direct caller): still correct,
            # just not budgeted separately.
            self.setup()
        iri = firing_iri(self.reaction["label"], event["event_id"], self.ns)
        self._knot(self.firing_turtle(event), iri)
        if not self._received(event):
            raise ConnectionError(f"{iri}: written but not found on read-back (indeterminate)")

    def delivery_scope(self) -> dict:
        return {"kind": "quipu", "base": self.base, "namespace": self.ns,
                "reaction": self.reaction["label"], "vocabulary": VOCABULARY_VERSION}

    def prepare_recovery(self, event: dict) -> dict:
        prior = event["prior"]
        if prior["emitter"] != self.reaction["label"]:
            raise ConnectionError("prior graph reaction identity changed")
        iri = firing_iri(prior["emitter"], prior["event_id"], self.ns)
        out = self._post("/query", {"query": f"SELECT (STR(?predicate) AS ?p) ?e WHERE {{ <{iri}> ?predicate ?e }}"})
        rows = out.get("rows")
        if (out.get("error") or out.get("truncated", False) is not False
                or not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows)):
            raise ConnectionError("prior firing read-back unavailable")
        predicates = self._term_aliases("eventId")
        matches = [r for r in rows if self._expand(r.get("p")) in predicates]
        if len(matches) != 1 or matches[0].get("e") != prior["event_id"]:
            raise ConnectionError("missing or ambiguous prior graph firing")
        prior_ends = [(self._expand(r.get("p")), _datetime_value(r["e"])) for r in rows
                      if self._expand(r.get("p")) in self._term_aliases("resolvedAt")]
        receipt = {"iri": iri, "predicate": term("resolvedAt"),
                   "delivery_scope": self.delivery_scope()}
        if prior_ends:
            if len(prior_ends) != 1:
                raise ConnectionError("ambiguous prior graph recovery")
            predicate, prior_end = prior_ends[0]
            ended = dt.datetime.fromisoformat(prior_end.replace("Z", "+00:00"))
            if ended.tzinfo is None or ended.timestamp() > event["resolved_at"]:
                raise ConnectionError("prior graph recovery is not authoritative")
            receipt["predicate"] = predicate  # read-only reuse of an already completed legacy effect
            receipt["already_ended"] = prior_end
        return receipt

    def resolve(self, event: dict) -> None:
        receipt = event["receipt"]
        if receipt.get("delivery_scope") != self.delivery_scope():
            raise ConnectionError("recovery graph receiver scope changed")
        ended = dt.datetime.fromtimestamp(event["resolved_at"], dt.timezone.utc)
        ended = ended.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        if receipt.get("already_ended"):
            ended = receipt["already_ended"]
        # Only this predicate changes; the firing, start and provenance remain.
        else:
            out = self._post("/set", {"entity": receipt["iri"], "predicate": receipt["predicate"],
                                  "value": {"value": ended, "datatype": "http://www.w3.org/2001/XMLSchema#dateTime"},
                                  "actor": "chaski", "source": "chaski:recovery:" + event["event_id"]})
            if type(out.get("asserted")) is not int or out["asserted"] not in {0, 1}:
                raise ConnectionError("graph recovery write indeterminate")
        query = f"SELECT ?e WHERE {{ <{receipt['iri']}> <{receipt['predicate']}> ?e }}"
        rows = self._post("/query", {"query": query}).get("rows")
        if not isinstance(rows, list) or len(rows) != 1 or _datetime_value(rows[0].get("e")) != ended:
            raise ConnectionError("graph recovery not exact on read-back")
