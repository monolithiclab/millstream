-- Migration 9: human review (spec section 8). A delivery of a review step parks awaitingReview with a review row;
-- a decision, the autoApprove rule or onTimeout ends it. The row keeps what deciding it needs from the step's config
-- (spec_json: inbox, timeout, onTimeout, editable pointers, reason codes, output schemas) and the item's display
-- rendering, so a review waiting for days needs neither its revision's package nor a template engine.
-- version changes when the review is escalated or parked again: a decision names the version it saw.
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
    deadline    TEXT NOT NULL, -- '' without a timeout
    decision    TEXT NOT NULL DEFAULT '', -- approve | reject
    reviewer    TEXT NOT NULL DEFAULT '', -- a person, rule (autoApprove) or timeout (onTimeout)
    reason      TEXT NOT NULL DEFAULT '',
    reason_text TEXT NOT NULL DEFAULT '',
    patch       TEXT NOT NULL DEFAULT '[]', -- the edit's diff, a JSON Patch (RFC 6902)
    decided_at  TEXT NOT NULL DEFAULT '',
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
    deadline    TEXT NOT NULL, -- '' when the step has no timeout
    decision    TEXT NOT NULL DEFAULT '', -- agree | disagree: with the rule
    reviewer    TEXT NOT NULL DEFAULT '',
    reason      TEXT NOT NULL DEFAULT '',
    decided_at  TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (pipeline, delivery_id),
    FOREIGN KEY (pipeline, delivery_id) REFERENCES deliveries (pipeline, id)
);
CREATE INDEX review_samples_open ON review_samples (state, inbox);
CREATE INDEX review_samples_deadline ON review_samples (state, deadline);
