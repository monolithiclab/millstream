package sqlstore

import (
	"fmt"
	"maps"
	"slices"

	"github.com/monolithiclab/millstream/internal/store"
)

// budgetSlack absorbs float rounding, so that three reservations of 0.1 fit a budget of 0.3.
const budgetSlack = 1e-9

// within reports whether a reservation fits a budget given what is already spent and reserved against it. Once the
// budget is reached, even an agent delivery reserving nothing is held back.
func within(spent, reserve, limit float64) bool {
	return spent < limit && spent+reserve <= limit+budgetSlack
}

// budgetAllows returns the budget that holds back the agent delivery l, store.BudgetRun or store.BudgetDay, or ""
// when it may be leased with its reservation: its run's spend and reservations plus its own within budget.runUsd of
// the run's revision, and its pipeline's for the day within budget.dayUsd of the active revision (spec section
// 7.5). The first delivery a budget stops at its current limit pauses the pipeline's agent steps, named by agents,
// with a budgetReached event; paused records the pipelines paused during this Lease, whose agent deliveries are
// held back for the rest of it (as by a day budget: the whole step). An audit (spec section 11.5) is checked against
// the day budget only, and its denial pauses nothing.
func (w *wtx) budgetAllows(l store.Lease, reserve float64, agents map[store.AgentStep]float64,
	paused map[string]bool,
) (string, error) {
	if paused[l.Pipeline] {
		return store.BudgetDay, nil
	}
	rev, err := w.revision(l.Revision)
	if err != nil {
		return "", err
	}
	if limit := rev.p.Budget.RunUSD; limit > 0 && !l.Audit { // an audit is no part of its run (spec section 11.5)
		var spent, reserved, reached float64
		if err := w.queryRow(`SELECT r.cost_usd, COALESCE((SELECT SUM(d.reserved_usd) FROM deliveries d
			WHERE d.pipeline = r.pipeline AND d.run_id = r.id AND d.state = ?), 0), r.budget_reached_usd
			FROM runs r WHERE r.pipeline = ? AND r.id = ?`, store.DeliveryLeased, l.Pipeline, l.RunID).
			Scan(&spent, &reserved, &reached); err != nil {
			return "", notFound(err, "run "+l.RunID)
		}
		if !within(spent+reserved, reserve, limit) {
			if reached < limit {
				if _, err := w.exec(`UPDATE runs SET budget_reached_usd = ? WHERE pipeline = ? AND id = ?`,
					limit, l.Pipeline, l.RunID); err != nil {
					return "", err
				}
				err = w.budgetReached(l.Pipeline, l.RunID, store.BudgetRun, limit, spent, reserved, agents, paused)
			}
			return store.BudgetRun, err
		}
	}
	var active store.RevisionID
	if err := w.queryRow(`SELECT active_revision FROM pipelines WHERE id = ?`, l.Pipeline).Scan(&active); err != nil {
		return "", notFound(err, "pipeline "+l.Pipeline)
	}
	if rev, err = w.revision(active); err != nil {
		return "", err
	}
	if limit := rev.p.Budget.DayUSD; limit > 0 {
		var spent, reserved, reached float64
		if err := w.queryRow(`SELECT
			COALESCE((SELECT spent_usd FROM budget_days WHERE pipeline = ? AND day = ?), 0),
			COALESCE((SELECT SUM(reserved_usd) FROM deliveries WHERE pipeline = ? AND state = ?), 0)
			+ COALESCE((SELECT SUM(reserved_usd) FROM audits WHERE pipeline = ? AND state = ?), 0),
			COALESCE((SELECT reached_usd FROM budget_days WHERE pipeline = ? AND day = ?), 0)`,
			l.Pipeline, w.day(), l.Pipeline, store.DeliveryLeased, l.Pipeline, store.AuditLeased, l.Pipeline, w.day()).
			Scan(&spent, &reserved, &reached); err != nil {
			return "", fmt.Errorf("read day budget: %w", err)
		}
		if !within(spent+reserved, reserve, limit) {
			if reached < limit && !l.Audit { // an audit held back is cancelled, never pauses the pipeline
				if _, err := w.exec(`INSERT INTO budget_days (pipeline, day, reached_usd) VALUES (?, ?, ?)
					ON CONFLICT (pipeline, day) DO UPDATE SET reached_usd = excluded.reached_usd`, l.Pipeline, w.day(), limit); err != nil {
					return "", err
				}
				err = w.budgetReached(l.Pipeline, "", store.BudgetDay, limit, spent, reserved, agents, paused)
			}
			return store.BudgetDay, err
		}
	}
	return "", nil
}

// budgetReached pauses every agent step of the pipeline with the budget as its cause, leaving the steps a user
// paused as they are, and records the budgetReached event.
func (w *wtx) budgetReached(pipelineName, runID, scope string, limit, spent, reserved float64,
	agents map[store.AgentStep]float64, paused map[string]bool,
) error {
	names := map[string]bool{} // agents has a key per revision of a step
	for ref := range agents {
		if ref.Pipeline == pipelineName {
			names[ref.Step] = true
		}
	}
	steps := slices.Sorted(maps.Keys(names))
	for _, step := range steps {
		if _, err := w.exec(`INSERT INTO steps_state (pipeline, step, paused, cause) VALUES (?, ?, TRUE, ?)
			ON CONFLICT (pipeline, step) DO UPDATE SET paused = TRUE, cause = excluded.cause
			WHERE steps_state.paused = FALSE`,
			pipelineName, step, store.PauseCauseBudget+scope); err != nil {
			return err
		}
	}
	paused[pipelineName] = true
	return w.event(pipelineName, runID, store.EventBudgetReached, map[string]any{
		"scope": scope, "limit": limit, "spent": spent, "reserved": reserved, "steps": steps,
	})
}
