"""Local-only receiver replay demonstration; no network or signing keys."""
from pathlib import Path
from tempfile import TemporaryDirectory

try:
    from slot_receiver import SlotLedger, slot_id
except ModuleNotFoundError:
    print("BASE: scheduled-slot admission API is unavailable")
    raise SystemExit(0)

with TemporaryDirectory() as directory:
    path = Path(directory) / "ledger.sqlite3"
    effects = []
    binding = {"host": "example-host", "manifest": "example-revision"}
    slot = slot_id("urn:example:job", "revision-1", 100)

    def action():
        effects.append("executed")
        return {"outcome": "succeeded", "exit": 0}

    first = SlotLedger(path).run(slot, binding, action)
    replay = SlotLedger(path).run(slot, binding, action)
    print(f"HEAD: two deliveries, executions={len(effects)}, identical receipts={first == replay}")
    assert len(effects) == 1 and first == replay
    SlotLedger(path).run(slot_id("urn:example:job", "revision-1", 200), binding, action)
    print(f"CONTROL: a distinct scheduled slot executes; total executions={len(effects)}")
    assert len(effects) == 2
