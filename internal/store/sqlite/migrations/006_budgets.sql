-- Migration 6: budgets (spec sections 5.9 and 7.5). Leasing an agent delivery reserves its maxCostUsd on the
-- delivery; completion replaces the reservation with the reported cost, and an expired lease charges it in full.
-- Costs are charged to the pipeline's UTC day in budget_days. When a budget stops a lease, the pipeline's agent
-- steps are paused with the budget as their cause, once per run or per day for a given limit: reached_usd (and the
-- run's budget_reached_usd) records the limit it was reached at, 0 while it has not been.

ALTER TABLE deliveries ADD COLUMN reserved_usd REAL NOT NULL DEFAULT 0;

ALTER TABLE runs ADD COLUMN budget_reached_usd REAL NOT NULL DEFAULT 0;

-- cause is empty for a pause a user asked for, else what paused the step (budget.run, budget.day).
ALTER TABLE steps_state ADD COLUMN cause TEXT NOT NULL DEFAULT '';

CREATE TABLE budget_days (
    pipeline    TEXT NOT NULL,
    day         TEXT NOT NULL, -- 2006-01-02, UTC
    spent_usd   REAL NOT NULL DEFAULT 0,
    reached_usd REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (pipeline, day)
);
