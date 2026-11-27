"""Bounded, redacted stderr diagnostics for failed adapter runs (aegis-ztja.1).

A Python traceback names its exception on its LAST line, so keeping only the
head of a failing adapter's stderr hides the cause. The diagnostic keeps a short
head (which usually names the program) and a bounded tail, with an explicit
marker for the bytes elided between them. Credentials are redacted from the
whole text before it is cut, so a secret straddling a cut cannot leak a
fragment. The result goes into the exception message and the log only, never
into a metric label.
"""
from __future__ import annotations

import os
import re

HEAD_BYTES = 256
DEFAULT_TAIL_BYTES = 4096
MAX_TAIL_BYTES = 65536
TAIL_ENV = "CHASKI_ADAPTER_STDERR_TAIL_BYTES"

_REDACTED = "[REDACTED]"
_PATTERNS = [
    # Authorization headers and bearer/basic credentials.
    (re.compile(r"(?i)\b(authorization\s*[:=]\s*)(\S+(?:\s+\S+)?)"), r"\1" + _REDACTED),
    (re.compile(r"(?i)\b(bearer|basic|token)\s+[A-Za-z0-9._~+/=-]{8,}"), r"\1 " + _REDACTED),
    # key=value / key: value for secret-looking keys (JSON quoting allowed).
    (re.compile(r"(?i)([\"']?[\w.-]*(?:token|secret|passw(?:or)?d|api[_-]?key|credential|bearer)"
                r"[\w.-]*[\"']?\s*[:=]\s*)([\"']?)[^\s\"',;&]+"), r"\1\2" + _REDACTED),
    # Credentials embedded in URLs: scheme://user:pass@host
    (re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://)[^/\s:@]+:[^/\s@]+@"), r"\1" + _REDACTED + "@"),
    # Well-known token shapes.
    (re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}"
                r"|sk-[A-Za-z0-9_-]{20,}|xox[abprs]-[A-Za-z0-9-]{10,}|AKIA[0-9A-Z]{16})\b"), _REDACTED),
    (re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b"), _REDACTED),
]


def redact(text: str) -> str:
    for pattern, replacement in _PATTERNS:
        text = pattern.sub(replacement, text)
    return text


def tail_bytes() -> int:
    """The configured tail bound, clamped to [0, MAX_TAIL_BYTES]."""
    raw = os.environ.get(TAIL_ENV, "")
    try:
        value = int(raw) if raw.strip() else DEFAULT_TAIL_BYTES
    except ValueError:
        value = DEFAULT_TAIL_BYTES
    return max(0, min(value, MAX_TAIL_BYTES))


def stderr_diagnostic(stderr: str | bytes | None, tail: int | None = None,
                      head: int = HEAD_BYTES) -> str:
    """Redacted head + bounded tail of `stderr`, marking any elided middle."""
    if isinstance(stderr, bytes):
        stderr = stderr.decode("utf-8", errors="replace")
    data = redact((stderr or "").strip()).encode("utf-8")
    tail = tail_bytes() if tail is None else max(0, tail)
    if len(data) <= head + tail:
        return data.decode("utf-8", errors="replace")
    elided = len(data) - head - tail
    start = data[:head].decode("utf-8", errors="ignore")
    end = data[len(data) - tail:].decode("utf-8", errors="ignore") if tail else ""
    return f"{start}...[{elided} bytes elided]...{end}"


def adapter_failure(returncode: int, stderr: str | bytes | None) -> RuntimeError:
    return RuntimeError(f"adapter exited {returncode}: {stderr_diagnostic(stderr)}")
