---
name: chaski
description: Read-only Chaski orientation and cursor-state inspection.
---

<!-- BEGIN chaski prime -->
# Chaski agent primer

## What chaski is

Chaski reacts to graph changes and schedules, keeping delivery separate from facts.
Quipu stores the knowledge and Camayoc prepares governed records.
Chaski evaluates reaction rules and routes emitter outcomes to explicit sinks.
Receiver setup, cursor custody and delivery are separate from read-only orientation.

## Agent workflow

1. Query Quipu for existing decisions, incidents and provenance before acting.
2. Inspect rules, emitter definitions and cursor custody before running a receiver.
3. Keep legacy event offsets separate from durable transaction cursors.
4. Treat failed evaluations as UNKNOWN; keep previous firing state and cursor.
5. Verify source identity, freshness and receipts before claiming successful delivery.

## Core commands

| Command | Use |
| --- | --- |
| `chaski prime` | Read-only orientation; absent state sources are explicit |
| `chaski prime --brief` | Short orientation for stack aggregation |
| `chaski prime --json` | Structured `stack-prime/1` output |
| `chaski prime --static` | Embedded guidance without state or network reads |
| `chaski prime --section core --static` | One section by heading prefix |
| `chaski prime --rules RULES --emitters EMITTERS` | Configured definition counts; no adapters or sinks |
| `chaski prime --state CURSOR --emitter-db INBOX` | Legacy JSON offset and inbox file metadata |
| `chaski prime --metrics-url http://127.0.0.1:9481/metrics` | Read the receiver's reported stream cursor and lag |
| `chaski --help` | Receiver options; running it can update local custody/state |
| `chaski-event --help` | Explicit external-event ingress options |
| `chaski-once --help` | Explicit single-emitter evaluation/delivery options |
| `chaski demo status` | Existing offline canary receipt, not live receiver health |

## MCP tools

None. Chaski exposes CLI commands, a change-stream consumer and receiver metrics.
Prime starts no receiver, schedule, adapter, sink or delivery process.

## Reading live state

Pass the same explicit rule, emitter and state paths used by the receiver.
There is no implicit shared-store discovery. Counts are configured definitions,
not proof they are active. Rule fields use the canonical pure validator; emitter
counts validate metadata only, without creating sinks or invoking adapters.

The whole read has a 650 ms deadline. Text/YAML/JSON and HTTP responses are capped
at 64 KiB; definition lists are bounded. Inbox inspection reads file metadata
only and never opens SQLite, takes a consumer lease or creates WAL sidecars.
A legacy JSON cursor is an event offset, never a transaction cursor.

An optional single metrics GET reports durable transaction cursor and lag when
the receiver exposes them, with no retries or redirects. Missing/negative gauges
remain unknown. A zero lag is not freshness; source-epoch binding, receiver
activation and current delivery health are not measured by prime. Static output
works offline. Failed reads keep guidance, report degraded state and exit 0.
Usage errors exit 2; `--section` matches a case-insensitive heading prefix.

## Next steps

- [README](https://github.com/intigraph/chaski#readme)
- [Installation and operation](https://github.com/intigraph/chaski/blob/main/docs/installation-reference.md)
- [Skill](https://github.com/intigraph/chaski/blob/main/skills/chaski/SKILL.md)
- Command help: `chaski prime --help` (no shipped man page).
<!-- END chaski prime -->
