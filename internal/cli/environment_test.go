package cli

import (
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/backtest"
	"github.com/monolithiclab/millstream/internal/distill"
	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/evidence"
	"github.com/monolithiclab/millstream/internal/insights"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store/postgres"
)

// TestDefaultsMatchConstants: the environment's default tags equal the constants of the packages that fall back on
// them when they are handed a zero value outside the environment (a test's options, a store's own Options). It lives
// here, where every one of those packages is imported already.
func TestDefaultsMatchConstants(t *testing.T) {
	t.Parallel()
	d := pipeline.Defaults()
	im := d.Improve
	for name, eq := range map[string]bool{
		"lessons.maxTokens":             d.Lessons.MaxTokens == pipeline.DefaultLessonsMaxTokens,
		"runtime.container":             d.Runtime.Container == pipeline.RuntimeDocker,
		"engine.lockCheck":              time.Duration(d.Engine.LockCheck) == postgres.DefaultCheckEvery,
		"engine.keepalive":              time.Duration(d.Engine.Keepalive) == postgres.DefaultKeepalive,
		"improve.minSeverity":           im.MinSeverity == pipeline.SeverityWarning,
		"improve.evidence.sample":       im.Evidence.Sample == evidence.DefaultSample,
		"improve.evidence.ttl":          time.Duration(im.Evidence.TTL) == evidence.DefaultTTL,
		"improve.backtest.sample":       im.Backtest.Sample == backtest.DefaultSample,
		"improve.validation.timeout":    time.Duration(im.Validation.Timeout) == engine.DefaultValidationTimeout,
		"improve.distill.minDeliveries": im.Distill.MinDeliveries == insights.DefaultDistillMinDeliveries,
		"improve.distill.minAgreement":  im.Distill.MinAgreement == distill.DefaultMinAgreement,
		"improve.distill.minCoverage":   im.Distill.MinCoverage == distill.DefaultMinCoverage,
		"improve.distill.auditRate":     im.Distill.AuditRate == distill.DefaultAuditRate,
		"improve.filter.maxFalseDrop":   im.Filter.MaxFalseDrop == insights.DefaultFilterMaxFalseDrop,
	} {
		if !eq {
			t.Errorf("%s: the default tag and the constant differ", name)
		}
	}
}
