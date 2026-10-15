package pipeline

import (
	"fmt"
	"maps"
	"slices"
	"time"
)

// Insight kinds (spec section 11.1).
const (
	InsightSlowStep       = "slowStep"
	InsightFailureCluster = "failureCluster"
	InsightCost           = "cost"
	InsightReviewPattern  = "reviewPattern"
	InsightWaste          = "waste"
	InsightDrift          = "drift"
	InsightDistillable    = "distillable"
	InsightUnschedulable  = "unschedulable"
)

// InsightKinds are every kind of the spec, which insights.disable may name, including those a later milestone
// computes.
var InsightKinds = []string{InsightSlowStep, InsightFailureCluster, InsightCost, InsightReviewPattern, InsightWaste,
	InsightDrift, InsightDistillable, InsightUnschedulable}

// DefaultInsightsEvery is how often the insights projector runs when insights.every is not set.
const DefaultInsightsEvery = 15 * time.Minute

// InsightsConfig is the pipeline's insights key (spec section 11.1).
type InsightsConfig struct {
	// Enabled is insights.enabled, true by default.
	Enabled bool
	// Every is insights.every: how often the projector runs besides at the end of every run.
	Every time.Duration
	// Disable are the kinds insights.disable turns off.
	Disable []string
}

// Kinds returns the kinds of available the config leaves on: none when insights are off.
func (c InsightsConfig) Kinds(available []string) []string {
	if !c.Enabled {
		return nil
	}
	return slices.DeleteFunc(slices.Clone(available), func(k string) bool { return slices.Contains(c.Disable, k) })
}

// InsightsConfig parses the pipeline's insights key: enabled (a boolean), every (a duration) and disable (a list of
// kinds). A missing key takes its default; an unknown key, a value of the wrong type and an unknown kind are errors.
func (p *Pipeline) InsightsConfig() (InsightsConfig, error) {
	c := InsightsConfig{Enabled: true, Every: DefaultInsightsEvery}
	for _, k := range slices.Sorted(maps.Keys(p.Insights)) {
		v := p.Insights[k]
		switch k {
		case "enabled":
			b, ok := v.(bool)
			if !ok {
				return c, fmt.Errorf("insights.enabled: want a boolean, got %v", v)
			}
			c.Enabled = b
		case "every":
			s, ok := v.(string)
			if !ok {
				return c, fmt.Errorf("insights.every: want a duration such as 15m, got %v", v)
			}
			d, err := ParseDuration(s)
			if err != nil {
				return c, fmt.Errorf("insights.every: %w", err)
			}
			c.Every = d
		case "disable":
			list, ok := v.([]any)
			if !ok {
				return c, fmt.Errorf("insights.disable: want a list of insight kinds, got %v", v)
			}
			for _, e := range list {
				kind, ok := e.(string)
				if !ok || !slices.Contains(InsightKinds, kind) {
					return c, fmt.Errorf("insights.disable: %v is not an insight kind (%v)", e, InsightKinds)
				}
				c.Disable = append(c.Disable, kind)
			}
		default:
			return c, fmt.Errorf("insights.%s: unknown key (enabled, every, disable)", k)
		}
	}
	return c, nil
}
