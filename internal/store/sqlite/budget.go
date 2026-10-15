package sqlite

import (
	"cmp"
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

// budgetAllows reports whether the agent delivery l may be leased with its reservation: its run's spend and
// reservations plus its own within budget.runUsd of the run's revision, and its pipeline's for the day within
// budget.dayUsd of the active revision (spec section 7.5). The first delivery a budget stops at its current limit
// pauses the pipeline's agent steps, named by agents, with a budgetReached event; paused records the pipelines
// paused during this Lease, whose agent deliveries are held back for the rest of it.
func (w *wtx) budgetAllows(l store.Lease, reserve float64, agents map[store.StepRef]float64,
	paused map[string]bool,
) (bool, error) {
	if paused[l.Pipeline] {
		return false, nil
	}
	rev, err := w.revision(l.Revision)
	if err != nil {
		return false, err
	}
	if limit := rev.p.Budget.RunUSD; limit > 0 {
		var spent, reserved, reached float64
		if err := w.queryRow(`SELECT r.cost_usd, COALESCE((SELECT SUM(d.reserved_usd) FROM deliveries d
			WHERE d.pipeline = r.pipeline AND d.run_id = r.id AND d.state = ?), 0), r.budget_reached_usd
			FROM runs r WHERE r.pipeline = ? AND r.id = ?`, store.DeliveryLeased, l.Pipeline, l.RunID).
			Scan(&spent, &reserved, &reached); err != nil {
			return false, notFound(err, "run "+l.RunID)
		}
		if !within(spent+reserved, reserve, limit) {
			if reached < limit {
				if _, err := w.exec(`UPDATE runs SET budget_reached_usd = ? WHERE pipeline = ? AND id = ?`,
					limit, l.Pipeline, l.RunID); err != nil {
					return false, err
				}
				err = w.budgetReached(l.Pipeline, l.RunID, store.BudgetRun, limit, spent, reserved, agents, paused)
			}
			return false, err
		}
	}
	var active store.RevisionID
	if err := w.queryRow(`SELECT active_revision FROM pipelines WHERE id = ?`, l.Pipeline).Scan(&active); err != nil {
		return false, notFound(err, "pipeline "+l.Pipeline)
	}
	if rev, err = w.revision(active); err != nil {
		return false, err
	}
	if limit := rev.p.Budget.DayUSD; limit > 0 {
		var spent, reserved, reached float64
		if err := w.queryRow(`SELECT
			COALESCE((SELECT spent_usd FROM budget_days WHERE pipeline = ?1 AND day = ?2), 0),
			COALESCE((SELECT SUM(reserved_usd) FROM deliveries WHERE pipeline = ?1 AND state = ?3), 0),
			COALESCE((SELECT reached_usd FROM budget_days WHERE pipeline = ?1 AND day = ?2), 0)`,
			l.Pipeline, w.day(), store.DeliveryLeased).Scan(&spent, &reserved, &reached); err != nil {
			return false, fmt.Errorf("read day budget: %w", err)
		}
		if !within(spent+reserved, reserve, limit) {
			if reached < limit {
				if _, err := w.exec(`INSERT INTO budget_days (pipeline, day, reached_usd) VALUES (?, ?, ?)
					ON CONFLICT DO UPDATE SET reached_usd = excluded.reached_usd`, l.Pipeline, w.day(), limit); err != nil {
					return false, err
				}
				err = w.budgetReached(l.Pipeline, "", store.BudgetDay, limit, spent, reserved, agents, paused)
			}
			return false, err
		}
	}
	return true, nil
}

// budgetReached pauses every agent step of the pipeline with the budget as its cause, leaving the steps a user
// paused as they are, and records the budgetReached event.
func (w *wtx) budgetReached(pipelineName, runID, scope string, limit, spent, reserved float64,
	agents map[store.StepRef]float64, paused map[string]bool,
) error {
	var steps []string
	for _, ref := range slices.SortedFunc(maps.Keys(agents), func(a, b store.StepRef) int {
		return cmp.Or(cmp.Compare(a.Pipeline, b.Pipeline), cmp.Compare(a.Step, b.Step))
	}) {
		if ref.Pipeline != pipelineName {
			continue
		}
		if _, err := w.exec(`INSERT INTO steps_state (pipeline, step, paused, cause) VALUES (?, ?, 1, ?)
			ON CONFLICT DO UPDATE SET paused = 1, cause = excluded.cause WHERE paused = 0`,
			pipelineName, ref.Step, store.PauseCauseBudget+scope); err != nil {
			return err
		}
		steps = append(steps, ref.Step)
	}
	paused[pipelineName] = true
	return w.event(pipelineName, runID, store.EventBudgetReached, map[string]any{
		"scope": scope, "limit": limit, "spent": spent, "reserved": reserved, "steps": steps,
	})
}
