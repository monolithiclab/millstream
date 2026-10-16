package pipeline

import (
	"strings"
	"testing"
	"time"
)

func TestParseReview(t *testing.T) {
	t.Parallel()
	r, errs := ParseReview("gate", map[string]any{"timeout": "2h", "onTimeout": map[string]any{"escalate": "leads"},
		"display": "{{.item.city}} {{.key}}", "sampleRate": int64(1)})
	if len(errs) > 0 || r.Inbox != "gate" || r.Timeout != 2*time.Hour || r.OnTimeout != OnTimeoutEscalate ||
		r.EscalateTo != "leads" || r.SampleRate != 1 {
		t.Fatalf("review = %+v, %v", r, errs)
	}
	if got := r.Render(map[string]any{"city": "Lyon"}, "k1", "it_1", "gate"); got != "Lyon k1" {
		t.Fatalf("render = %q", got)
	}
	d, errs := ParseReview("gate", map[string]any{"timeout": "1h"})
	if len(errs) > 0 || d.OnTimeout != OnTimeoutReject || d.SampleRate != DefaultSampleRate || d.Render(nil, "", "", "") != "" {
		t.Fatalf("defaults = %+v, %v", d, errs)
	}
	if r, _ := ParseReview("gate", nil); r.OnTimeout != "" || r.Timeout != 0 {
		t.Fatalf("no timeout = %+v", r)
	}
	fails, _ := ParseReview("gate", map[string]any{"display": "{{index .item 5}}"})
	if got := fails.Render(map[string]any{}, "", "", ""); !strings.HasPrefix(got, "display template failed: ") {
		t.Fatalf("failing template = %q", got)
	}
	long, _ := ParseReview("gate", map[string]any{"display": "{{range .item}}{{.}}{{end}}"})
	items := make([]any, 5000)
	for i := range items {
		items[i] = "ééééé"
	}
	if got := long.Render(items, "", "", ""); len(got) > MaxDisplay || len(got) < MaxDisplay-4 {
		t.Fatalf("long render: %d bytes", len(got))
	}
	for _, cfg := range []map[string]any{
		{"onTimeout": 3, "timeout": "1h"}, {"onTimeout": "later", "timeout": "1h"},
		{"onTimeout": map[string]any{"escalate": "x", "also": 1}, "timeout": "1h"}, {"sampleRate": "half"},
		{"editable": "/x"}, {"inbox": 3},
	} {
		if _, errs := ParseReview("gate", cfg); len(errs) == 0 {
			t.Errorf("config %v parsed", cfg)
		}
	}
}
