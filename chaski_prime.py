"""The read-only stack orientation command; never start a receiver."""

from __future__ import annotations

import argparse
from importlib import metadata
import json
from pathlib import Path
import queue
import threading
import time
import tomllib

from chaski_primer import PRIMER

DEADLINE = 0.650


def tool_version() -> str:
    try:
        return metadata.version("chaski-reactor")
    except metadata.PackageNotFoundError:
        try:
            project = tomllib.loads(Path(__file__).with_name("pyproject.toml").read_text())["project"]
            if project["name"] == "chaski-reactor":
                return project["version"]
        except (OSError, ValueError, KeyError):
            pass
        return "source checkout"


def select_section(name: str | None) -> tuple[str, list[str]]:
    blocks = PRIMER.split("\n## ")[1:]
    headings = [block.split("\n", 1)[0] for block in blocks]
    if name is None:
        return PRIMER, headings
    for heading, block in zip(headings, blocks):
        if name.strip() and heading.casefold().startswith(name.strip().casefold()):
            return "## " + block, headings
    raise ValueError(f"unknown primer section {name!r}; sections: " + ", ".join(headings))


def summarize(args, reader=None) -> dict:
    started = time.monotonic()
    result = queue.Queue(maxsize=1)

    def read():
        try:
            if reader is None:
                from chaski_prime_state import inspect

                state = inspect(args)
            else:
                state = reader(args)
        except Exception:
            state = {"status": "unreadable", "summary": {"reason": "reader_failed"}}
        result.put(state)

    threading.Thread(target=read, daemon=True).start()
    try:
        state = result.get(timeout=DEADLINE)
    except queue.Empty:
        state = {"status": "partial", "summary": {"reason": "deadline", "next": "chaski prime --static"}}
    state["elapsed_ms"] = int((time.monotonic() - started) * 1000)
    return state


def state_lines(state: dict) -> list[str]:
    summary = state["summary"]
    sources = summary.get("sources", {})

    def show(value):
        return "unknown" if value is None else str(value)

    lines = [
        "State: " + state["status"],
        f"Configured rules: {show(summary.get('rules_configured'))}; "
        f"emitters: {show(summary.get('emitters_configured'))} (activation not measured)",
    ]
    if "reason" in summary:
        lines[0] += " (" + summary["reason"] + ")"
    legacy = sources.get("legacy", {}).get("offset")
    receiver = sources.get("receiver", {})
    lines.append(
        f"Legacy event offset: {show(legacy)}; reported stream cursor: {show(receiver.get('reported_stream_cursor'))}"
    )
    lines.append(f"Reported transaction lag: {show(receiver.get('reported_lag_tx'))} (freshness not measured)")
    if sources:
        lines.append(
            "Sources: "
            + "; ".join(
                source.get("path", source.get("url", "unknown")) + " [" + source["status"] + "]"
                for source in sources.values()
            )
        )
    if "next" in summary:
        lines.append("Next: " + summary["next"])
    lines.append(f"Read: {state['elapsed_ms']} ms")
    return [line.replace("\n", "\\n").replace("\r", "\\r") for line in lines]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="chaski prime", description=__doc__)
    parser.add_argument("--brief", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--static", action="store_true", dest="static_only")
    parser.add_argument("--section", help="case-insensitive heading prefix")
    parser.add_argument("--rules", type=Path, help="existing rules YAML")
    parser.add_argument("--emitters", type=Path, help="existing emitter YAML; metadata only")
    parser.add_argument("--state", type=Path, help="legacy JSON event cursor (not transaction cursor)")
    parser.add_argument("--emitter-db", "--inbox", type=Path, dest="inbox", help="existing inbox; file metadata only")
    parser.add_argument("--metrics-url", help="receiver metrics URL; one read-only GET")
    args = parser.parse_args(argv)
    if args.brief and args.section is not None:
        parser.error("--brief and --section cannot be combined")
    try:
        text, sections = select_section(args.section)
    except ValueError as error:
        parser.error(str(error))
    version = tool_version()
    live = {"status": "skipped", "elapsed_ms": 0, "summary": {}} if args.static_only else summarize(args)
    primer = {"sections": sections}
    if not args.brief:
        primer["text"] = text
    if args.json:
        print(
            json.dumps(
                {
                    "schema": "stack-prime/1",
                    "tool": "chaski",
                    "version": version,
                    "primer": primer,
                    "live": live,
                    "status": "ok" if live["status"] in {"ok", "skipped"} else "degraded",
                }
            )
        )
    elif args.brief:
        lines = [f"chaski {version}: reacts to graph changes and routes explicit notifications"]
        if args.static_only:
            what, _ = select_section("What chaski is")
            lines.extend(line for line in what.splitlines()[1:] if line.strip())
        else:
            lines.extend(state_lines(live))
        lines.append("Run `chaski prime` for the full primer.")
        print("\n".join(lines))
    else:
        print(text.rstrip())
        if not args.static_only:
            print("\n## Live state\n\n" + "\n".join(state_lines(live)))
    return 0
