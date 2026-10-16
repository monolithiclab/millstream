-- The log store (spec section 9.1), Postgres's DDL: the logical schema of the SQLite store's migrations/001_init.sql
-- (same tables, columns and keys, which the shared units in store/sqlstore read and write alike), in Postgres's
-- types: timestamptz for instants (NULL for one that has not come), bigint for counts and ids, double precision for
-- costs and scores, date for a budget's day, boolean for a pause, bytea for a bundle, and jsonb for the one JSON
-- column the units query (an item's parents). The other JSON documents are text: the units store them canonical and
-- read them back byte for byte.
--
-- rowid is creation order, as SQLite's implicit key of the same name is, under the same name so the units' SQL is
-- shared: reads page by it and Lease takes deliveries in its order. The single writer inserts in commit order; an
-- insert rolled back leaves a gap in the sequence, which no reader depends on. What must not have gaps, or must be
-- numbered alike after a crash, is numbered by the writer instead: events' seq and revisions' id.

CREATE TABLE packages (
    hash       text PRIMARY KEY,
    bundle     bytea NOT NULL,
    created_at timestamptz NOT NULL
);

CREATE TABLE revisions (
    id            bigint PRIMARY KEY, -- assigned by the writer, the last one plus one, as on SQLite
    pipeline      text NOT NULL,
    package_hash  text NOT NULL REFERENCES packages (hash),
    pipeline_json text NOT NULL,
    applied_at    timestamptz NOT NULL,
    message       text NOT NULL DEFAULT '', -- apply --message
    state         text NOT NULL, -- active | superseded
    vars          text NOT NULL DEFAULT '{}', -- the variables of the runs its imports open, resolved at apply
    var_sources   text NOT NULL DEFAULT '{}'
);

CREATE TABLE pipelines (
    id              text PRIMARY KEY,
    active_revision bigint NOT NULL REFERENCES revisions (id),
    state           text NOT NULL -- active | paused | disabled
);

-- The streams each pipeline's active revision imports (spec section 4.12): the Done unit copies a producer's new
-- item on producer_stream to each consumer's stream. ApplyRevision and Rollback rewrite a consumer's rows.
CREATE TABLE stream_imports (
    consumer        text NOT NULL,
    stream          text NOT NULL, -- the consumer's name for it
    producer        text NOT NULL,
    producer_stream text NOT NULL,
    PRIMARY KEY (consumer, stream)
);
CREATE INDEX stream_imports_producer ON stream_imports (producer, producer_stream);

-- cause is empty for a pause a user asked for, else what paused the step (budget.run, budget.day).
CREATE TABLE steps_state (
    pipeline text NOT NULL,
    step     text NOT NULL,
    paused   boolean NOT NULL,
    cause    text NOT NULL DEFAULT '',
    PRIMARY KEY (pipeline, step)
);

-- budget_reached_usd is the run budget it was stopped at (spec section 7.5), 0 while it has not been.
CREATE TABLE runs (
    rowid              bigint GENERATED ALWAYS AS IDENTITY,
    pipeline           text NOT NULL,
    id                 text NOT NULL,
    revision           bigint NOT NULL REFERENCES revisions (id),
    trigger            text NOT NULL,
    trigger_key        text NOT NULL,
    trigger_source     text NOT NULL, -- cron | manual | webhook | import
    scheduled_at       timestamptz,   -- the scheduled time of a cron firing
    kind               text NOT NULL, -- trigger | replay | import
    generation         bigint NOT NULL,
    vars               text NOT NULL,
    var_sources        text NOT NULL,
    state              text NOT NULL,
    started_at         timestamptz NOT NULL,
    settled_at         timestamptz,
    cost_usd           double precision NOT NULL DEFAULT 0,
    budget_reached_usd double precision NOT NULL DEFAULT 0,
    producer           text NOT NULL DEFAULT '', -- an import run's producer pipeline, its trigger_key the producer run
    consumers          bigint NOT NULL DEFAULT 0,  -- how many import runs it opened in other pipelines
    PRIMARY KEY (pipeline, id)
);
CREATE INDEX runs_producer ON runs (producer, trigger_key) WHERE producer <> '';
CREATE INDEX runs_cron ON runs (pipeline, trigger, trigger_source, scheduled_at);
CREATE INDEX runs_started ON runs (started_at, rowid);

-- Cron firings that catch-up none passed over (spec section 4.6): with the cron runs, where catch-up resumes.
CREATE TABLE skipped_firings (
    pipeline     text NOT NULL,
    trigger      text NOT NULL,
    scheduled_at timestamptz NOT NULL,
    PRIMARY KEY (pipeline, trigger, scheduled_at)
);

-- Replay runs (spec section 5.7) take the next generation of their pipeline; trigger runs keep generation 0.
CREATE TABLE generations (
    pipeline text PRIMARY KEY,
    next     bigint NOT NULL -- the generation the next replay run takes
);

-- An item lists the artifacts it carries (spec section 9.2): a JSON array of {name, sha256, size, type}.
CREATE TABLE items (
    rowid        bigint GENERATED ALWAYS AS IDENTITY,
    pipeline     text NOT NULL,
    id           text NOT NULL,
    run_id       text NOT NULL,
    stream       text NOT NULL,
    step         text NOT NULL,
    key          text NOT NULL,
    parents      jsonb NOT NULL, -- JSON array of item ids
    generation   bigint NOT NULL,
    ts           timestamptz NOT NULL,
    payload      text NOT NULL,
    payload_hash text NOT NULL,
    artifacts    text NOT NULL DEFAULT '[]',
    -- the pipeline holding the item's parents when it is another one: a copy published through an import
    parent_pipeline text NOT NULL DEFAULT '',
    PRIMARY KEY (pipeline, id),
    FOREIGN KEY (pipeline, run_id) REFERENCES runs (pipeline, id)
);
CREATE INDEX items_copies ON items (parent_pipeline) WHERE parent_pipeline <> '';
CREATE INDEX items_stream ON items (pipeline, stream);
CREATE INDEX items_run ON items (pipeline, run_id);
CREATE INDEX items_rowid ON items (rowid);

-- attempt_base is the attempt number before a dead-letter replay: attempt numbers are never reused (attempt
-- artifacts are keyed by them), and the retry budget counts attempt - attempt_base. reserved_usd is the budget
-- reservation of a leased agent delivery (spec section 5.9). Timings: created_at (reset by a dead-letter replay),
-- leased_at (the latest attempt's lease), finished_at (terminal), and the time its attempts spent due but not leased
-- (wait_ms, backoff excluded) and leased (work_ms).
CREATE TABLE deliveries (
    rowid            bigint GENERATED ALWAYS AS IDENTITY,
    pipeline         text NOT NULL,
    id               text NOT NULL,
    run_id           text NOT NULL,
    step             text NOT NULL,
    item_id          text NOT NULL,
    generation       bigint NOT NULL,
    state            text NOT NULL,
    attempt          bigint NOT NULL,
    attempt_base     bigint NOT NULL DEFAULT 0,
    lease_owner      text NOT NULL DEFAULT '',
    fencing_token    bigint NOT NULL DEFAULT 0,
    lease_expires_at timestamptz,
    next_attempt_at  timestamptz NOT NULL,
    error_class      text NOT NULL DEFAULT '',
    error_message    text NOT NULL DEFAULT '',
    filter_reason    text NOT NULL DEFAULT '',
    cost_usd         double precision NOT NULL DEFAULT 0,
    reserved_usd     double precision NOT NULL DEFAULT 0,
    created_at       timestamptz NOT NULL,
    leased_at        timestamptz,
    finished_at      timestamptz,
    wait_ms          bigint NOT NULL DEFAULT 0,
    work_ms          bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (pipeline, id),
    UNIQUE (pipeline, step, item_id, generation),
    FOREIGN KEY (pipeline, run_id) REFERENCES runs (pipeline, id),
    FOREIGN KEY (pipeline, item_id) REFERENCES items (pipeline, id)
);
CREATE INDEX deliveries_due ON deliveries (state, next_attempt_at);
CREATE INDEX deliveries_run ON deliveries (pipeline, run_id, state);
CREATE INDEX deliveries_lease ON deliveries (state, lease_expires_at);
CREATE INDEX deliveries_item ON deliveries (pipeline, item_id);
CREATE INDEX deliveries_queue ON deliveries (pipeline, step, state, rowid);

CREATE TABLE dead_letters (
    rowid           bigint GENERATED ALWAYS AS IDENTITY,
    pipeline        text NOT NULL,
    id              text NOT NULL, -- the dead-letter item
    run_id          text NOT NULL,
    origin_delivery text NOT NULL,
    origin_step     text NOT NULL,
    kind            text NOT NULL, -- dead | unrouted
    state           text NOT NULL, -- open | replayed | discarded
    stream          text NOT NULL,
    error           text NOT NULL,
    created_at      timestamptz NOT NULL,
    PRIMARY KEY (pipeline, id),
    FOREIGN KEY (pipeline, id) REFERENCES items (pipeline, id)
);
CREATE INDEX dead_letters_run ON dead_letters (pipeline, run_id, state);

-- seq is assigned by the writer, in commit order, each event numbered after the last, so the sequence has no gaps.
CREATE TABLE events (
    seq      bigint PRIMARY KEY,
    ts       timestamptz NOT NULL,
    pipeline text NOT NULL,
    run_id   text NOT NULL,
    kind     text NOT NULL,
    data     text NOT NULL
);
-- The insights projector reads a pipeline's events of some kinds over a window; a delivery's attempts are its run's.
CREATE INDEX events_pipeline ON events (pipeline, kind, ts);
CREATE INDEX events_run ON events (pipeline, run_id, seq);

-- The dedupe builtin keeps the keys it let through, per pipeline, step and generation, so a replay run (a new
-- generation) dedupes only within itself.
CREATE TABLE dedupe_keys (
    pipeline   text NOT NULL,
    step       text NOT NULL,
    generation bigint NOT NULL,
    key        text NOT NULL,
    first_item text NOT NULL, -- the item that passed when the key was last let through
    seen_at    timestamptz NOT NULL,
    PRIMARY KEY (pipeline, step, generation, key)
);

-- Artifacts (spec sections 6.3 and 9.2) live content-addressed in the artifact store, outside the database; each
-- counts the items and attempts referencing it, so retention can delete a file once nothing needs it. A count moves
-- only when an item is inserted: a retry republishing an existing item counts nothing twice.
CREATE TABLE artifacts (
    pipeline   text NOT NULL,
    sha256     text NOT NULL,
    size       bigint NOT NULL,
    type       text NOT NULL,
    refcount   bigint NOT NULL,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (pipeline, sha256)
);

-- An attempt may keep files of its own, apart from its outputs, such as an agent's transcript (spec section 7.2).
CREATE TABLE attempt_artifacts (
    pipeline    text NOT NULL,
    delivery_id text NOT NULL,
    attempt     bigint NOT NULL,
    name        text NOT NULL,
    sha256      text NOT NULL,
    size        bigint NOT NULL,
    type        text NOT NULL,
    created_at  timestamptz NOT NULL,
    PRIMARY KEY (pipeline, delivery_id, attempt, name)
);

-- Costs charged to a pipeline's UTC day (spec section 7.5), and the day budget it was stopped at, 0 while it has not
-- been.
CREATE TABLE budget_days (
    pipeline    text NOT NULL,
    day         date NOT NULL, -- UTC
    spent_usd   double precision NOT NULL DEFAULT 0,
    reached_usd double precision NOT NULL DEFAULT 0,
    PRIMARY KEY (pipeline, day)
);

-- Eval scores (spec section 7.4), by action and its digest.
CREATE TABLE eval_results (
    rowid         bigint GENERATED ALWAYS AS IDENTITY,
    action        text NOT NULL, -- the manifest's name
    action_digest text NOT NULL,
    case_name     text NOT NULL,
    score         double precision NOT NULL, -- 0 to 1
    at            timestamptz NOT NULL
);
CREATE INDEX eval_results_case ON eval_results (action, case_name, at);

-- Metrics are a projection of events (D10): the rollup unit reads the events after its cursor and adds each
-- measurement to the rollup of its minute, moving the cursor in the same transaction.
CREATE TABLE projections (
    name text PRIMARY KEY, -- rollup
    seq  bigint NOT NULL   -- the last event the projection read
);

CREATE TABLE metric_rollups (
    pipeline    text NOT NULL,
    name        text NOT NULL,
    labels_json text NOT NULL, -- a JSON object with sorted keys, {} without labels
    minute      timestamptz NOT NULL, -- the minute's start
    count       bigint NOT NULL,
    sum         double precision NOT NULL,
    min         double precision NOT NULL,
    max         double precision NOT NULL,
    PRIMARY KEY (pipeline, name, labels_json, minute)
);

-- id is the insight's fingerprint (spec section 11.1), so a recurring problem updates its row.
CREATE TABLE insights (
    pipeline      text NOT NULL,
    id            text NOT NULL,
    kind          text NOT NULL,
    scope         text NOT NULL, -- a step, or pipeline
    window_start  timestamptz NOT NULL,
    window_end    timestamptz NOT NULL,
    severity      text NOT NULL, -- info | warning | error
    evidence_json text NOT NULL,
    state         text NOT NULL, -- open | acknowledged | resolved | muted
    opened_at     timestamptz NOT NULL,
    updated_at    timestamptz NOT NULL,
    PRIMARY KEY (pipeline, id)
);

-- Human review (spec section 8): see the SQLite migration for what a review row keeps and why. version changes when
-- the review is escalated or parked again: a decision names the version it saw.
CREATE TABLE reviews (
    rowid       bigint GENERATED ALWAYS AS IDENTITY,
    pipeline    text NOT NULL,
    delivery_id text NOT NULL,
    run_id      text NOT NULL,
    step        text NOT NULL,
    item_id     text NOT NULL,
    inbox       text NOT NULL,
    state       text NOT NULL, -- open | decided | timedOut | cancelled
    version     bigint NOT NULL,
    spec_json   text NOT NULL,
    display     text NOT NULL,
    parked_at   timestamptz NOT NULL,
    deadline    timestamptz,              -- NULL without a timeout
    decision    text NOT NULL DEFAULT '', -- approve | reject
    reviewer    text NOT NULL DEFAULT '', -- a person, rule (autoApprove) or timeout (onTimeout)
    reason      text NOT NULL DEFAULT '',
    reason_text text NOT NULL DEFAULT '',
    patch       text NOT NULL DEFAULT '[]', -- the edit's diff, a JSON Patch (RFC 6902)
    decided_at  timestamptz,
    PRIMARY KEY (pipeline, delivery_id),
    FOREIGN KEY (pipeline, delivery_id) REFERENCES deliveries (pipeline, id)
);
CREATE INDEX reviews_open ON reviews (state, inbox);
CREATE INDEX reviews_deadline ON reviews (state, deadline);
CREATE INDEX reviews_run ON reviews (pipeline, run_id, state);

-- Sampled auto-approvals: the item passed at once; the sample only measures the rule and never holds its run.
CREATE TABLE review_samples (
    rowid       bigint GENERATED ALWAYS AS IDENTITY,
    pipeline    text NOT NULL,
    delivery_id text NOT NULL,
    run_id      text NOT NULL,
    step        text NOT NULL,
    item_id     text NOT NULL,
    inbox       text NOT NULL,
    state       text NOT NULL, -- open | decided | unreviewed
    display     text NOT NULL,
    created_at  timestamptz NOT NULL,
    deadline    timestamptz,              -- NULL when the step has no timeout
    decision    text NOT NULL DEFAULT '', -- agree | disagree: with the rule
    reviewer    text NOT NULL DEFAULT '',
    reason      text NOT NULL DEFAULT '',
    decided_at  timestamptz,
    PRIMARY KEY (pipeline, delivery_id),
    FOREIGN KEY (pipeline, delivery_id) REFERENCES deliveries (pipeline, id)
);
CREATE INDEX review_samples_open ON review_samples (state, inbox);
CREATE INDEX review_samples_deadline ON review_samples (state, deadline);

-- The deployment's leadership (spec section 5.9), Postgres's alone: SQLite has no standby. Each engine that takes
-- the leadership lock writes its row on its write connection, under the lock, before it writes anything else: who
-- leads, since when, and the epoch, one more at each acquisition. One row per lock key, so two deployments sharing
-- a database keep theirs apart.
CREATE TABLE leadership (
    lock_key  bigint PRIMARY KEY,
    engine_id text NOT NULL,
    version   text NOT NULL,
    address   text NOT NULL, -- where the engine serves its API, for nodes and clients a standby redirects
    epoch     bigint NOT NULL,
    since     timestamptz NOT NULL
);

-- Webhook trigger tokens (spec section 4.6): the SHA-256 of each token, hex, never the token. A token outlives the
-- revisions that declare its trigger, so a rollback finds it again.
CREATE TABLE trigger_tokens (
    pipeline   text NOT NULL,
    trigger    text NOT NULL,
    hash       text NOT NULL,
    created_at timestamptz NOT NULL, -- set again by a rotation
    PRIMARY KEY (pipeline, trigger)
);
