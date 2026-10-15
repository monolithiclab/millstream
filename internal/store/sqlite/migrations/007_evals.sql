-- Migration 7: eval results (spec section 7.4). millstream eval --db records each case's score with the action's
-- digest, so the scores of two versions of an action (a prompt, a model) can be compared. Evals score actions, not
-- pipelines: the action's name leads the key.

CREATE TABLE eval_results (
    action        TEXT NOT NULL, -- the manifest's name
    action_digest TEXT NOT NULL,
    case_name     TEXT NOT NULL,
    score         REAL NOT NULL, -- 0 to 1
    at            TEXT NOT NULL
);
CREATE INDEX eval_results_case ON eval_results (action, case_name, at);
