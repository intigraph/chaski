
CREATE TABLE IF NOT EXISTS change_cursor (id INTEGER PRIMARY KEY CHECK(id=1), tx INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS change_inbox (
 emitter TEXT NOT NULL, tx INTEGER NOT NULL, seq INTEGER NOT NULL, payload TEXT NOT NULL,
 PRIMARY KEY(emitter,tx,seq));
-- Route a bounded entity batch without sorting every queued JSON payload to disk.
-- The remaining top-two ordering uses the grouped index keys, not the full inbox.
CREATE INDEX IF NOT EXISTS change_inbox_entity
 ON change_inbox(emitter,json_extract(payload,'$.entity'),tx,seq);
CREATE TABLE IF NOT EXISTS adapter_deadlines (
 emitter TEXT NOT NULL, item TEXT NOT NULL, due REAL NOT NULL, priority INTEGER NOT NULL DEFAULT 1,
 PRIMARY KEY(emitter,item));
CREATE TABLE IF NOT EXISTS adapter_reconcile (
 emitter TEXT PRIMARY KEY, last_success REAL NOT NULL);
CREATE TABLE IF NOT EXISTS adapter_subscription (
 emitter TEXT PRIMARY KEY, digest TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS adapter_deadlines_due ON adapter_deadlines(emitter,due);
