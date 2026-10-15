"""Signed-run bridge for an already admitted scheduled delivery.

The trusted port must persist and verify signed records; this module never
interprets graph-supplied commands or claims that a callback is authentication.
No scheduler, listener or automatic retry of child execution is started here.
"""
from __future__ import annotations

import base64
import json
from dataclasses import asdict, dataclass
from typing import Callable, Protocol

from slot_receiver import SlotLedger, slot_id


@dataclass(frozen=True)
class ExecutionScope:
    """Immutable binding supplied by a trusted manifest admission layer."""
    job: str
    revision: str
    host: str
    manifest_sha256: str
    agent: str
    workflow: str

    def validate(self):
        if any(type(value) is not str or not value or len(value) > 2048
               for value in asdict(self).values()):
            raise ValueError("scope requires bounded nonempty string fields")
        if (len(self.manifest_sha256) != 64 or
                any(c not in "0123456789abcdef" for c in self.manifest_sha256)):
            raise ValueError("manifest digest must be lowercase SHA256")


def run_id(scope: ExecutionScope, scheduled_at: int) -> str:
    """Reversible scope inside the identifier covered by Shuttle's signature.

    A hash alone would leave the verifier dependent on an unsigned lookup.
    Base64url avoids both the canonical signature delimiter and IRI escaping.
    """
    scope.validate()
    slot_id(scope.job, scope.revision, scheduled_at)  # validates UTC slot type
    binding = {**asdict(scope), "scheduled_at": scheduled_at, "version": 1}
    raw = json.dumps(binding, sort_keys=True, separators=(",", ":")).encode()
    return "scheduled-v1-" + base64.urlsafe_b64encode(raw).decode().rstrip("=")


class SignedRunPort(Protocol):
    """Trusted adapter contract, not data supplied by an incoming delivery.

    admit must verify the local host, current manifest, eligible declaration,
    registered workflow/key scope and existing private-key custody. started
    must durably persist a signed queued->running record BEFORE returning.
    finished must durably persist the actual signed running->terminal record.
    publish must export and independently read back those exact records using
    the registered key. An exception is UNKNOWN, never successful acceptance.
    All methods must be safe to call for the deterministic run identifier.
    """
    def admit(self, scope: ExecutionScope, scheduled_at: int) -> None: ...
    def started(self, run: str, scope: ExecutionScope) -> None: ...
    def finished(self, run: str, scope: ExecutionScope, outcome: dict) -> None: ...
    def publish(self, run: str, scope: ExecutionScope, outcome: dict) -> None: ...


def _outcome(value: dict) -> dict:
    if not isinstance(value, dict) or set(value) != {"outcome", "exit_code"}:
        raise ValueError("execution needs exactly outcome and exit_code")
    name, code = value["outcome"], value["exit_code"]
    if type(code) is not int or not 0 <= code <= 255:
        raise ValueError("exit_code must be an actual bounded process result")
    if name not in ("succeeded", "failed", "skipped-by-hold"):
        raise ValueError("unknown execution outcome")
    if (name == "failed") != (code != 0):
        raise ValueError("outcome and process exit code disagree")
    return {"outcome": name, "exit_code": code}


def receive(ledger: SlotLedger, scope: ExecutionScope, scheduled_at: int,
            delivery: dict, port: SignedRunPort, execute: Callable[[], dict]) -> dict:
    """Execute at most once, then retry only publication on redelivery.

    Delivery must repeat the complete admitted scope and slot exactly. The
    ledger persists before started, and the terminal result persists before
    publish. A crash or terminal-signing failure leaves the claim UNKNOWN;
    it never authorizes rerunning the child. Admission is checked even for a
    duplicate; a revoked identity cannot use replay to obtain acceptance.
    """
    run = run_id(scope, scheduled_at)
    binding = {**asdict(scope), "scheduled_at": scheduled_at}
    if (not isinstance(delivery, dict) or
            json.dumps(delivery, sort_keys=True, allow_nan=False) !=
            json.dumps(binding, sort_keys=True, allow_nan=False)):
        raise ValueError("delivery differs from the admitted scope or slot")
    port.admit(scope, scheduled_at)

    def action():
        port.started(run, scope)
        outcome = _outcome(execute())
        port.finished(run, scope, dict(outcome))
        return {"run": run, **outcome}

    result = ledger.run(slot_id(scope.job, scope.revision, scheduled_at), binding, action)
    outcome = _outcome({k: result[k] for k in ("outcome", "exit_code")})
    if result.get("run") != run:
        raise ValueError("stored run identity does not match the admitted scope")
    port.publish(run, scope, outcome)
    return result
