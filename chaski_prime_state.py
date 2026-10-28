"""Bounded orientation reads; no custody, SQLite connections or delivery."""

from __future__ import annotations

import json
from pathlib import Path
import re
import stat
import urllib.error
import urllib.parse
import urllib.request

import yaml

LIMIT = 64 * 1024
MAX_DEFINITIONS = 1000
GAUGES = {"chaski_changes_cursor_offset", "chaski_changes_lag_transactions", "chaski_events_lag"}


def read_small(path: Path) -> bytes:
    if not stat.S_ISREG(path.stat().st_mode):
        raise ValueError("not a regular file")
    with path.open("rb") as stream:
        data = stream.read(LIMIT + 1)
    if len(data) > LIMIT:
        raise ValueError("input bound exceeded")
    return data


def definitions(path: Path, key: str) -> dict:
    doc = yaml.safe_load(read_small(path)) or {}
    if not isinstance(doc, dict):
        raise ValueError("expected a mapping")
    rows = doc.get(key, [])
    if not isinstance(rows, list) or len(rows) > MAX_DEFINITIONS:
        raise ValueError("invalid or oversized definition list")
    labels = set()
    enabled = 0
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("label"), str) or not row["label"]:
            raise ValueError("invalid definition label")
        if row["label"] in labels or type(row.get("enabled", True)) is not bool:
            raise ValueError("duplicate label or invalid enabled field")
        labels.add(row["label"])
        if key == "rules":
            # This validator only checks fields; it neither queries nor evaluates.
            from chaski import Rule

            Rule.from_dict(row)
        enabled += row.get("enabled", True)
    return {
        "status": "ok",
        "path": str(path),
        "count": len(rows),
        "enabled": enabled,
        "validation": "rule_fields" if key == "rules" else "metadata_only",
    }


def legacy_cursor(path: Path) -> dict:
    doc = json.loads(read_small(path))
    if not isinstance(doc, dict) or type(doc.get("cursor")) is not int or not -1 <= doc["cursor"] <= 2**63 - 1:
        raise ValueError("invalid legacy cursor")
    return {
        "status": "partial" if doc["cursor"] == -1 else "ok",
        "path": str(path),
        "offset": None if doc["cursor"] == -1 else doc["cursor"],
        "kind": "legacy_event_offset",
    }


def inbox_metadata(path: Path) -> dict:
    info = path.stat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("not a regular inbox file")
    return {
        "status": "ok",
        "path": str(path),
        "bytes": info.st_size,
        "wal_present": path.with_name(path.name + "-wal").exists(),
        "cursor": None,
        "verification": "file_metadata_only",
    }


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def metrics(url: str) -> dict:
    parsed = urllib.parse.urlsplit(url)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/", "/metrics"}
    ):
        raise ValueError("invalid metrics URL")
    target = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, "/metrics", "", ""))
    base = {"url": target, "freshness": "not_measured", "source_epoch_binding": "not_measured"}
    request = urllib.request.Request(target, headers={"User-Agent": "chaski-prime", "Accept": "text/plain"})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    try:
        with opener.open(request, timeout=0.4) as response:
            body = response.read(LIMIT + 1)
    except urllib.error.HTTPError:
        return {**base, "status": "unreadable", "reason": "http_response"}
    except (urllib.error.URLError, OSError, TimeoutError) as error:
        reason = getattr(error, "reason", error)
        timeout = isinstance(reason, TimeoutError)
        return {
            **base,
            "status": "partial" if timeout else "unreachable",
            "reason": "deadline" if timeout else "connection",
        }
    if len(body) > LIMIT:
        return {**base, "status": "unreadable", "reason": "response_bound"}
    try:
        values = metric_values(body)
    except (ValueError, UnicodeError):
        return {**base, "status": "unreadable", "reason": "metrics_payload"}
    cursor = values.get("chaski_changes_cursor_offset")
    lag = values.get("chaski_changes_lag_transactions")
    return {
        **base,
        "status": "ok" if cursor is not None and lag is not None else "partial",
        "reported_stream_cursor": cursor,
        "reported_lag_tx": lag,
        "reported_event_lag": values.get("chaski_events_lag"),
        "source": "receiver_metrics",
    }


def metric_values(body: bytes) -> dict:
    values = {}
    for line in body.decode("utf-8").splitlines():
        fields = line.split()
        if not fields or fields[0] not in GAUGES:
            continue
        if len(fields) != 2 or fields[0] in values or not re.fullmatch(r"-?[0-9]+", fields[1]):
            raise ValueError("ambiguous or invalid gauge")
        value = int(fields[1])
        if not -1 <= value <= 2**63 - 1:
            raise ValueError("gauge outside bounds")
        values[fields[0]] = None if value == -1 else value
    if not values:
        raise ValueError("no receiver cursor metrics")
    return values


def inspect(args) -> dict:
    sources = {}
    for key, path, reader in [
        ("rules", args.rules, lambda p: definitions(p, "rules")),
        ("emitters", args.emitters, lambda p: definitions(p, "emitters")),
        ("legacy", args.state, legacy_cursor),
        ("inbox", args.inbox, inbox_metadata),
        ("receiver", args.metrics_url, metrics),
    ]:
        if path is None:
            continue
        try:
            sources[key] = reader(path)
        except FileNotFoundError:
            sources[key] = {"status": "absent", "path": str(path)}
        except Exception:
            # Unknown/malformed input never becomes a successful empty result.
            # Error messages may include payloads/credentials; emit only a category.
            sources[key] = {
                "status": "unreadable",
                "reason": "input",
                "path": str(path) if key != "receiver" else "(invalid metrics URL)",
            }
    statuses = {source["status"] for source in sources.values()}
    status = next(
        (item for item in ["unreadable", "unreachable", "partial", "absent"] if item in statuses),
        "ok" if sources else "absent",
    )
    summary = {
        "sources": sources,
        "rules_configured": sources.get("rules", {}).get("count"),
        "emitters_configured": sources.get("emitters", {}).get("count"),
        "activation": "not_measured",
    }
    if status != "ok":
        summary["next"] = "chaski prime --metrics-url http://127.0.0.1:9481/metrics"
    return {"status": status, "summary": summary}
