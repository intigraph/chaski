# Stable writer vocabulary

Each emitter graph sink has an explicit `vocabulary` setting. Its default is
`legacy-vocabulary`, which emits the existing aegis classes and properties.
Select `canonical-vocabulary` only after the receiving server's reviewed shape
profile sanctions the published Reaction and ReactionFiring classes and validates
their properties. There is no automatic mode selection or retry in another
vocabulary after a refusal.

```yaml
sink:
  graph:
    vocabulary: legacy-vocabulary
    severity: info
    action: [alert]
    condition: "ASK {}"
    comment: "Example reaction"
```

Canonical mode uses 14 exact published ownership mappings. Classes and
owner/schedule/severity belong to Quipu; the other mapped properties belong to
Camayoc core. The independent snapshot in
`tests/fixtures/chaski-ownership.json` comes from public ownership.json SHA256
`a389be53b74259d0c515ff6db16d602474dc5fe38dc6cc47f0f54a2d1347d34e`.
Unknown writer terms refuse; the instance namespace is not a vocabulary fallback.

Historical readers accept exact legacy and published spellings. GitHub readers
also retain explicit read-only references to unpublished GitHub terms, pending
vocabulary ownership. No new ownership is inferred from a common prefix.
Duplicate event identity or completion aliases refuse rather than acknowledge
ambiguous evidence. Already completed legacy recovery is read-only and retains
its actual predicate; a new recovery uses the selected writer's resolvedAt.

This source change does not migrate deployment identities, stored data or frozen
events. Instance defaults remain unchanged pending the separate install-identity
contract. A neutral explicitly supplied namespace in the demo isolates vocabulary
from that outstanding identity work; it is not a new allocation policy.

The graph receiver scope includes the selected vocabulary version. Previously bound
original deliveries and recoveries refuse before setup or replay across this
boundary; their frozen payload and binding remain intact. IMPORTANT: older
unbound attempted outbox rows predate receiver bindings and cannot prove which
encoding was sent. Runtime adoption must inspect and disposition those rows,
existing bound debt, registration/shape compatibility and rollback before any
upgrade. This PR authorizes no drain, retry, rebinding or runtime activation.

Before activating either mode, read the authenticated live `/shapes` vocabulary
and check the exact emitted class IRIs. Then validate a bounded Reaction and
ReactionFiring against a scratch server with those served shapes; vocabulary
membership alone does not prove property validation. A canonical shape-profile
release and activation is a separate prerequisite for flipping canonical mode.
Changing a selector never repairs a missing or conflicting retained receiver scope.

The canonical builder fixture explicitly selects canonical mode. Run it with
`python3 docs/demos/stable-vocabulary/demo.py`.
Its base/candidate recording is in the adjacent directory; it proves builder
output and identity preservation, not native-store admission or deployment.
