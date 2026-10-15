-- Migration 2: the builtins (spec sections 4.7 and 5.11). A filtered delivery records why it was filtered, and the
-- dedupe builtin keeps the keys it let through, per pipeline, step and generation, so a replay run (a new
-- generation) dedupes only within itself.

ALTER TABLE deliveries ADD COLUMN filter_reason TEXT NOT NULL DEFAULT '';

CREATE TABLE dedupe_keys (
    pipeline   TEXT NOT NULL,
    step       TEXT NOT NULL,
    generation INTEGER NOT NULL,
    key        TEXT NOT NULL,
    first_item TEXT NOT NULL, -- the item that passed when the key was last let through
    seen_at    TEXT NOT NULL,
    PRIMARY KEY (pipeline, step, generation, key)
);
