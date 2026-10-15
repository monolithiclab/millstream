-- Migration 5: attempt artifacts (spec sections 6.3 and 7.2). An attempt may keep files of its own, apart from its
-- outputs, such as an agent's transcript; a failed attempt keeps them too. Each counts as one reference in the
-- artifacts table, like an item carrying it.

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
