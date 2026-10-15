-- Migration 1: the M1 log store (spec section 9.1). Times are UTC text, 2006-01-02T15:04:05.000Z, so they compare
-- as strings. The pipeline id leads every key (spec section 4.10): ids are deterministic per pipeline, and two
-- pipelines may produce the same run, item or delivery id.

CREATE TABLE packages (
    hash       TEXT PRIMARY KEY,
    bundle     BLOB NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE revisions (
    id            INTEGER PRIMARY KEY,
    pipeline      TEXT NOT NULL,
    package_hash  TEXT NOT NULL REFERENCES packages (hash),
    pipeline_json TEXT NOT NULL,
    applied_at    TEXT NOT NULL,
    state         TEXT NOT NULL -- active | superseded
);

CREATE TABLE pipelines (
    id              TEXT PRIMARY KEY,
    active_revision INTEGER NOT NULL REFERENCES revisions (id),
    state           TEXT NOT NULL -- active | paused | disabled
);

CREATE TABLE steps_state (
    pipeline TEXT NOT NULL,
    step     TEXT NOT NULL,
    paused   INTEGER NOT NULL,
    PRIMARY KEY (pipeline, step)
);

CREATE TABLE runs (
    pipeline    TEXT NOT NULL,
    id          TEXT NOT NULL,
    revision    INTEGER NOT NULL REFERENCES revisions (id),
    trigger     TEXT NOT NULL,
    trigger_key TEXT NOT NULL,
    trigger_source TEXT NOT NULL, -- cron | manual
    scheduled_at   TEXT,          -- the scheduled time of a cron firing
    kind        TEXT NOT NULL, -- trigger | replay
    generation  INTEGER NOT NULL,
    vars        TEXT NOT NULL,
    var_sources TEXT NOT NULL,
    state       TEXT NOT NULL,
    started_at  TEXT NOT NULL,
    settled_at  TEXT,
    cost_usd    REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (pipeline, id)
);

CREATE INDEX runs_cron ON runs (pipeline, trigger, trigger_source, scheduled_at);

-- Cron firings that catch-up none passed over (spec section 4.6): with the cron runs, where catch-up resumes.
CREATE TABLE skipped_firings (
    pipeline     TEXT NOT NULL,
    trigger      TEXT NOT NULL,
    scheduled_at TEXT NOT NULL,
    PRIMARY KEY (pipeline, trigger, scheduled_at)
);

CREATE TABLE items (
    pipeline     TEXT NOT NULL,
    id           TEXT NOT NULL,
    run_id       TEXT NOT NULL,
    stream       TEXT NOT NULL,
    step         TEXT NOT NULL,
    key          TEXT NOT NULL,
    parents      TEXT NOT NULL, -- JSON array of item ids
    generation   INTEGER NOT NULL,
    ts           TEXT NOT NULL,
    payload      TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    PRIMARY KEY (pipeline, id),
    FOREIGN KEY (pipeline, run_id) REFERENCES runs (pipeline, id)
);
CREATE INDEX items_stream ON items (pipeline, stream);
CREATE INDEX items_run ON items (pipeline, run_id);

-- Lease orders deliveries by rowid: creation order.
CREATE TABLE deliveries (
    pipeline         TEXT NOT NULL,
    id               TEXT NOT NULL,
    run_id           TEXT NOT NULL,
    step             TEXT NOT NULL,
    item_id          TEXT NOT NULL,
    generation       INTEGER NOT NULL,
    state            TEXT NOT NULL,
    attempt          INTEGER NOT NULL,
    lease_owner      TEXT NOT NULL DEFAULT '',
    fencing_token    INTEGER NOT NULL DEFAULT 0,
    lease_expires_at TEXT NOT NULL DEFAULT '',
    next_attempt_at  TEXT NOT NULL,
    error_class      TEXT NOT NULL DEFAULT '',
    error_message    TEXT NOT NULL DEFAULT '',
    cost_usd         REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (pipeline, id),
    UNIQUE (pipeline, step, item_id, generation),
    FOREIGN KEY (pipeline, run_id) REFERENCES runs (pipeline, id),
    FOREIGN KEY (pipeline, item_id) REFERENCES items (pipeline, id)
);
CREATE INDEX deliveries_due ON deliveries (state, next_attempt_at);
CREATE INDEX deliveries_run ON deliveries (pipeline, run_id, state);
CREATE INDEX deliveries_lease ON deliveries (state, lease_expires_at);

CREATE TABLE dead_letters (
    pipeline        TEXT NOT NULL,
    id              TEXT NOT NULL, -- the dead-letter item
    run_id          TEXT NOT NULL,
    origin_delivery TEXT NOT NULL,
    origin_step     TEXT NOT NULL,
    kind            TEXT NOT NULL, -- dead | unrouted
    state           TEXT NOT NULL, -- open | replayed | discarded
    stream          TEXT NOT NULL,
    error           TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    PRIMARY KEY (pipeline, id),
    FOREIGN KEY (pipeline, id) REFERENCES items (pipeline, id)
);
CREATE INDEX dead_letters_run ON dead_letters (pipeline, run_id, state);

-- seq is the rowid: the single writer appends in commit order, and a unit rolled back to its savepoint gives its
-- numbers back, so the sequence has no gaps.
CREATE TABLE events (
    seq      INTEGER PRIMARY KEY,
    ts       TEXT NOT NULL,
    pipeline TEXT NOT NULL,
    run_id   TEXT NOT NULL,
    kind     TEXT NOT NULL,
    data     TEXT NOT NULL
);
