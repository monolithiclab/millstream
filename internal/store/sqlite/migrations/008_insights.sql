-- Migration 8: metric rollups, insights and run reports (spec sections 10.2 and 11.1).
--
-- Delivery timings: created_at (reset by a dead-letter replay), leased_at (the latest attempt's lease),
-- finished_at (terminal), and the time its attempts spent due but not leased (wait_ms, backoff excluded) and leased
-- (work_ms). Latency, the critical path and the slowStep insight come from them.
ALTER TABLE deliveries ADD COLUMN created_at TEXT NOT NULL DEFAULT '';
ALTER TABLE deliveries ADD COLUMN leased_at TEXT NOT NULL DEFAULT '';
ALTER TABLE deliveries ADD COLUMN finished_at TEXT NOT NULL DEFAULT '';
ALTER TABLE deliveries ADD COLUMN wait_ms INTEGER NOT NULL DEFAULT 0;
ALTER TABLE deliveries ADD COLUMN work_ms INTEGER NOT NULL DEFAULT 0;

-- The insights projector reads a pipeline's events of some kinds over a window.
CREATE INDEX events_pipeline ON events (pipeline, kind, ts);

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

-- id is the insight's fingerprint, so a recurring problem updates its row.
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
