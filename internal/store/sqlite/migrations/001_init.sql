-- The log store (spec section 9.1), SQLite's DDL. The Postgres store's migrations/001_init.sql holds the same logical
-- schema: the same tables, columns and keys, which the shared units (store/sqlstore) read and write alike.
--
-- Times are UTC text, 2006-01-02T15:04:05.000Z, so they compare as strings; NULL is an instant that has not come
-- (a lease not held, a delivery not finished, a review without a deadline). The pipeline id leads every key (spec
-- section 4.10): ids are deterministic per pipeline, and two pipelines may produce the same run, item or delivery
-- id. rowid, SQLite's implicit key, is creation order: reads page by it and Lease takes deliveries in its order.

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
    message       TEXT NOT NULL DEFAULT '', -- apply --message
    state         TEXT NOT NULL -- active | superseded
);

CREATE TABLE pipelines (
    id              TEXT PRIMARY KEY,
    active_revision INTEGER NOT NULL REFERENCES revisions (id),
    state           TEXT NOT NULL, -- active | paused | disabled
    vars            TEXT NOT NULL DEFAULT '{}', -- the variables of the runs its imports open, resolved at apply
    var_sources     TEXT NOT NULL DEFAULT '{}'
);

-- The streams each pipeline's active revision imports (spec section 4.12): the Done unit copies a producer's new
-- item on producer_stream to each consumer's stream. ApplyRevision rewrites a consumer's rows.
CREATE TABLE stream_imports (
    consumer        TEXT NOT NULL,
    stream          TEXT NOT NULL, -- the consumer's name for it
    producer        TEXT NOT NULL,
    producer_stream TEXT NOT NULL,
    PRIMARY KEY (consumer, stream)
);
CREATE INDEX stream_imports_producer ON stream_imports (producer, producer_stream);

-- cause is empty for a pause a user asked for, else what paused the step (budget.run, budget.day).
CREATE TABLE steps_state (
    pipeline TEXT NOT NULL,
    step     TEXT NOT NULL,
    paused   BOOLEAN NOT NULL,
    cause    TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (pipeline, step)
);

-- budget_reached_usd is the run budget it was stopped at (spec section 7.5), 0 while it has not been.
CREATE TABLE runs (
    pipeline           TEXT NOT NULL,
    id                 TEXT NOT NULL,
    revision           INTEGER NOT NULL REFERENCES revisions (id),
    trigger            TEXT NOT NULL,
    trigger_key        TEXT NOT NULL,
    trigger_source     TEXT NOT NULL, -- cron | manual | import
    scheduled_at       TEXT,          -- the scheduled time of a cron firing
    kind               TEXT NOT NULL, -- trigger | replay | import
    generation         INTEGER NOT NULL,
    vars               TEXT NOT NULL,
    var_sources        TEXT NOT NULL,
    state              TEXT NOT NULL,
    started_at         TEXT NOT NULL,
    settled_at         TEXT,
    cost_usd           REAL NOT NULL DEFAULT 0,
    budget_reached_usd REAL NOT NULL DEFAULT 0,
    producer           TEXT NOT NULL DEFAULT '', -- an import run's producer pipeline, its trigger_key the producer run
    consumers          INTEGER NOT NULL DEFAULT 0,  -- how many import runs it opened in other pipelines
    PRIMARY KEY (pipeline, id)
);
CREATE INDEX runs_producer ON runs (producer, trigger_key) WHERE producer <> '';
CREATE INDEX runs_cron ON runs (pipeline, trigger, trigger_source, scheduled_at);

-- Cron firings that catch-up none passed over (spec section 4.6): with the cron runs, where catch-up resumes.
CREATE TABLE skipped_firings (
    pipeline     TEXT NOT NULL,
    trigger      TEXT NOT NULL,
    scheduled_at TEXT NOT NULL,
    PRIMARY KEY (pipeline, trigger, scheduled_at)
);

-- Replay runs (spec section 5.7) take the next generation of their pipeline; trigger runs keep generation 0.
CREATE TABLE generations (
    pipeline TEXT PRIMARY KEY,
    next     INTEGER NOT NULL -- the generation the next replay run takes
);

-- An item lists the artifacts it carries (spec section 9.2): a JSON array of {name, sha256, size, type}.
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
    artifacts    TEXT NOT NULL DEFAULT '[]',
    -- the pipeline holding the item's parents when it is another one: a copy published through an import
    parent_pipeline TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (pipeline, id),
    FOREIGN KEY (pipeline, run_id) REFERENCES runs (pipeline, id)
);
CREATE INDEX items_copies ON items (parent_pipeline) WHERE parent_pipeline <> '';
CREATE INDEX items_stream ON items (pipeline, stream);
CREATE INDEX items_run ON items (pipeline, run_id);

-- attempt_base is the attempt number before a dead-letter replay: attempt numbers are never reused (attempt
-- artifacts are keyed by them), and the retry budget counts attempt - attempt_base. reserved_usd is the budget
-- reservation of a leased agent delivery (spec section 5.9). Timings: created_at (reset by a dead-letter replay),
-- leased_at (the latest attempt's lease), finished_at (terminal), and the time its attempts spent due but not leased
-- (wait_ms, backoff excluded) and leased (work_ms).
CREATE TABLE deliveries (
    pipeline         TEXT NOT NULL,
    id               TEXT NOT NULL,
    run_id           TEXT NOT NULL,
    step             TEXT NOT NULL,
    item_id          TEXT NOT NULL,
    generation       INTEGER NOT NULL,
    state            TEXT NOT NULL,
    attempt          INTEGER NOT NULL,
    attempt_base     INTEGER NOT NULL DEFAULT 0,
    lease_owner      TEXT NOT NULL DEFAULT '',
    fencing_token    INTEGER NOT NULL DEFAULT 0,
    lease_expires_at TEXT,
    next_attempt_at  TEXT NOT NULL,
    error_class      TEXT NOT NULL DEFAULT '',
    error_message    TEXT NOT NULL DEFAULT '',
    filter_reason    TEXT NOT NULL DEFAULT '',
    cost_usd         REAL NOT NULL DEFAULT 0,
    reserved_usd     REAL NOT NULL DEFAULT 0,
    created_at       TEXT NOT NULL,
    leased_at        TEXT,
    finished_at      TEXT,
    wait_ms          INTEGER NOT NULL DEFAULT 0,
    work_ms          INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (pipeline, id),
    UNIQUE (pipeline, step, item_id, generation),
    FOREIGN KEY (pipeline, run_id) REFERENCES runs (pipeline, id),
    FOREIGN KEY (pipeline, item_id) REFERENCES items (pipeline, id)
);
CREATE INDEX deliveries_due ON deliveries (state, next_attempt_at);
CREATE INDEX deliveries_run ON deliveries (pipeline, run_id, state);
CREATE INDEX deliveries_lease ON deliveries (state, lease_expires_at);
CREATE INDEX deliveries_item ON deliveries (pipeline, item_id);

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

-- seq is the rowid: the writer appends in commit order, numbering each event after the last, so the sequence has no
-- gaps.
CREATE TABLE events (
    seq      INTEGER PRIMARY KEY,
    ts       TEXT NOT NULL,
    pipeline TEXT NOT NULL,
    run_id   TEXT NOT NULL,
    kind     TEXT NOT NULL,
    data     TEXT NOT NULL
);
-- The insights projector reads a pipeline's events of some kinds over a window; a delivery's attempts are its run's.
CREATE INDEX events_pipeline ON events (pipeline, kind, ts);
CREATE INDEX events_run ON events (pipeline, run_id, seq);

-- The dedupe builtin keeps the keys it let through, per pipeline, step and generation, so a replay run (a new
-- generation) dedupes only within itself.
CREATE TABLE dedupe_keys (
    pipeline   TEXT NOT NULL,
    step       TEXT NOT NULL,
    generation INTEGER NOT NULL,
    key        TEXT NOT NULL,
    first_item TEXT NOT NULL, -- the item that passed when the key was last let through
    seen_at    TEXT NOT NULL,
    PRIMARY KEY (pipeline, step, generation, key)
);

-- Artifacts (spec sections 6.3 and 9.2) live content-addressed in the artifact store, outside the database; each
-- counts the items and attempts referencing it, so retention can delete a file once nothing needs it. A count moves
-- only when an item is inserted: a retry republishing an existing item counts nothing twice.
CREATE TABLE artifacts (
    pipeline   TEXT NOT NULL,
    sha256     TEXT NOT NULL,
    size       INTEGER NOT NULL,
    type       TEXT NOT NULL,
    refcount   INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (pipeline, sha256)
);

-- An attempt may keep files of its own, apart from its outputs, such as an agent's transcript (spec section 7.2).
CREATE TABLE attempt_artifacts (
    pipeline    TEXT NOT NULL,
    delivery_id TEXT NOT NULL,
    attempt     INTEGER NOT NULL,
    name        TEXT NOT NULL,
    sha256      TEXT NOT NULL,
    size        INTEGER NOT NULL,
    type        TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    PRIMARY KEY (pipeline, delivery_id, attempt, name)
);

-- Costs charged to a pipeline's UTC day (spec section 7.5), and the day budget it was stopped at, 0 while it has not
-- been.
CREATE TABLE budget_days (
    pipeline    TEXT NOT NULL,
    day         TEXT NOT NULL, -- 2006-01-02, UTC
    spent_usd   REAL NOT NULL DEFAULT 0,
    reached_usd REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (pipeline, day)
);

-- Eval scores (spec section 7.4), by action and its digest.
CREATE TABLE eval_results (
    action        TEXT NOT NULL, -- the manifest's name
    action_digest TEXT NOT NULL,
    case_name     TEXT NOT NULL,
    score         REAL NOT NULL, -- 0 to 1
    at            TEXT NOT NULL
);
CREATE INDEX eval_results_case ON eval_results (action, case_name, at);

-- Metrics are a projection of events (D10): the rollup unit reads the events after its cursor and adds each
-- measurement to the rollup of its minute, moving the cursor in the same transaction.
CREATE TABLE projections (
    name TEXT PRIMARY KEY, -- rollup
    seq  INTEGER NOT NULL  -- the last event the projection read
);

CREATE TABLE metric_rollups (
    pipeline    TEXT NOT NULL,
    name        TEXT NOT NULL,
    labels_json TEXT NOT NULL, -- a JSON object with sorted keys, {} without labels
    minute      TEXT NOT NULL, -- the minute's start, UTC
    count       INTEGER NOT NULL,
    sum         REAL NOT NULL,
    min         REAL NOT NULL,
    max         REAL NOT NULL,
    PRIMARY KEY (pipeline, name, labels_json, minute)
);

-- id is the insight's fingerprint (spec section 11.1), so a recurring problem updates its row.
CREATE TABLE insights (
    pipeline      TEXT NOT NULL,
    id            TEXT NOT NULL,
    kind          TEXT NOT NULL,
    scope         TEXT NOT NULL, -- a step, or pipeline
    window_start  TEXT NOT NULL,
    window_end    TEXT NOT NULL,
    severity      TEXT NOT NULL, -- info | warning | error
    evidence_json TEXT NOT NULL,
    state         TEXT NOT NULL, -- open | acknowledged | resolved | muted
    opened_at     TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    PRIMARY KEY (pipeline, id)
);

-- Human review (spec section 8): a delivery of a review step parks awaitingReview with a review row; a decision, the
-- autoApprove rule or onTimeout ends it. The row keeps what deciding it needs from the step's config (spec_json:
-- inbox, timeout, onTimeout, editable pointers, reason codes, output schemas) and the item's display rendering, so a
-- review waiting for days needs neither its revision's package nor a template engine. version changes when the
-- review is escalated or parked again: a decision names the version it saw.
CREATE TABLE reviews (
    pipeline    TEXT NOT NULL,
    delivery_id TEXT NOT NULL,
    run_id      TEXT NOT NULL,
    step        TEXT NOT NULL,
    item_id     TEXT NOT NULL,
    inbox       TEXT NOT NULL,
    state       TEXT NOT NULL, -- open | decided | timedOut | cancelled
    version     INTEGER NOT NULL,
    spec_json   TEXT NOT NULL,
    display     TEXT NOT NULL,
    parked_at   TEXT NOT NULL,
    deadline    TEXT,                     -- NULL without a timeout
    decision    TEXT NOT NULL DEFAULT '', -- approve | reject
    reviewer    TEXT NOT NULL DEFAULT '', -- a person, rule (autoApprove) or timeout (onTimeout)
    reason      TEXT NOT NULL DEFAULT '',
    reason_text TEXT NOT NULL DEFAULT '',
    patch       TEXT NOT NULL DEFAULT '[]', -- the edit's diff, a JSON Patch (RFC 6902)
    decided_at  TEXT,
    PRIMARY KEY (pipeline, delivery_id),
    FOREIGN KEY (pipeline, delivery_id) REFERENCES deliveries (pipeline, id)
);
CREATE INDEX reviews_open ON reviews (state, inbox);
CREATE INDEX reviews_deadline ON reviews (state, deadline);
CREATE INDEX reviews_run ON reviews (pipeline, run_id, state);

-- Sampled auto-approvals: the item passed at once; the sample only measures the rule and never holds its run.
CREATE TABLE review_samples (
    pipeline    TEXT NOT NULL,
    delivery_id TEXT NOT NULL,
    run_id      TEXT NOT NULL,
    step        TEXT NOT NULL,
    item_id     TEXT NOT NULL,
    inbox       TEXT NOT NULL,
    state       TEXT NOT NULL, -- open | decided | unreviewed
    display     TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    deadline    TEXT,                     -- NULL when the step has no timeout
    decision    TEXT NOT NULL DEFAULT '', -- agree | disagree: with the rule
    reviewer    TEXT NOT NULL DEFAULT '',
    reason      TEXT NOT NULL DEFAULT '',
    decided_at  TEXT,
    PRIMARY KEY (pipeline, delivery_id),
    FOREIGN KEY (pipeline, delivery_id) REFERENCES deliveries (pipeline, id)
);
CREATE INDEX review_samples_open ON review_samples (state, inbox);
CREATE INDEX review_samples_deadline ON review_samples (state, deadline);
