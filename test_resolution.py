"""Complete-inventory recovery must use the existing received-firing protocol."""
import emitter as em
from test_renewal import Sink


def test_missing_complete_inventory_recovers_exact_received_generation():
    db = em.connect(":memory:")
    definition = em.Emitter("family", [], 0, "crew", from_verdict="BELOW",
                            to_verdict="ABOVE", generation_event_ids=True,
                            resolve_recovery=True, missing_is_from=True)
    sink = Sink()
    em.observe(db, definition, [{"item": "a", "verdict": "BELOW"}], 10)
    em.observe(db, definition, [{"item": "a", "verdict": "ABOVE", "event_id": "a"}], 20)
    em.deliver_pending(db, sink, 20, definition.label)
    firing = sink.deliveries[0]
    em.observe(db, definition, [], 30, scope=set())
    assert db.execute("SELECT COUNT(*) FROM recovery_outbox").fetchone()[0] == 0
    em.observe(db, definition, [], 40)
    em.deliver_pending(db, sink, 40, definition.label, resolve_recovery=True)
    assert sink.deliveries[1]["prior"] == firing
    assert db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 1
    assert db.execute("SELECT COUNT(*) FROM recovery_outbox").fetchone()[0] == 1
