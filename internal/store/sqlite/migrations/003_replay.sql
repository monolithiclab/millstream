-- Migration 3: replay runs (spec section 5.7). Each replay run takes the next generation of its pipeline; trigger
-- runs keep generation 0, so the first replay takes 1.

CREATE TABLE generations (
    pipeline TEXT PRIMARY KEY,
    next     INTEGER NOT NULL -- the generation the next replay run takes
);
