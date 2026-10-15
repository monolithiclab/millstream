-- Migration 4: artifacts (spec sections 6.3 and 9.2). Files live content-addressed in the artifact store, outside
-- the database; an item lists the artifacts it carries, and each artifact counts the items referencing it, so
-- retention (M4) can delete a file once no item needs it. A count moves only when an item is inserted: a retry
-- republishing an existing item counts nothing twice.

ALTER TABLE items ADD COLUMN artifacts TEXT NOT NULL DEFAULT '[]'; -- JSON array of {name, sha256, size, type}

CREATE TABLE artifacts (
    pipeline   TEXT NOT NULL,
    sha256     TEXT NOT NULL,
    size       INTEGER NOT NULL,
    type       TEXT NOT NULL,
    refcount   INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (pipeline, sha256)
);
