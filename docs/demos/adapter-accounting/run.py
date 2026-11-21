"""Source fixture only: no adapter process, network, or production database."""
import sqlite3
import subprocess
import types

for label, revision in [("before", "d238c52e5810315cd74487b728f3bae756a77063"),
                        ("after", "5adb838")]:
    source = subprocess.check_output(["git", "show", revision + ":reconcile.py"], text=True)
    module = types.ModuleType("demo_" + label)
    exec(compile(source, "reconcile.py", "exec"), module.__dict__)
    conn = sqlite3.connect(":memory:")
    budget = module.RequestBudget(conn, 1, 40)
    budget.try_reserve(module.LIVE, 10, 0)
    assert budget.try_reserve(module.LIVE, 10, 10)
    budget.settle(10, {"quipu_requests": 2}, "fixture")
    lines = [line for line in budget.metrics(str) if line.startswith("chaski_adapter_")]
    print(label + ": reservation=10, reported requests=2, refund=" + str(budget.refunded_total))
    print("\n".join(lines) if lines else "adapter request/unknown metrics absent")
    if label == "after":
        budget.adapter_accounting_unknown()
        print("failed call with unknown cost:")
        print("\n".join(line for line in budget.metrics(str) if line.startswith("chaski_adapter_")))
    conn.close()
