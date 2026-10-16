package ids

import (
	"testing"
	"time"
)

// The golden values were computed independently (Python hashlib over the NUL-joined fields), so a change to a
// formula fails here rather than silently re-keying stored data.
func TestFormulas(t *testing.T) {
	t.Parallel()
	cest := time.FixedZone("CEST", 2*3600)
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"item keyed", Item("crawl", "listings.raw", "it_abc", 0, "https://example.com/1", 0), "it_dfec9d5a7a2fe5b8e2a0908efe820f1f"},
		{"item unkeyed uses index", Item("crawl", "listings.raw", "it_abc", 0, "", 0), "it_e252f87eb496b3e3c9ec98e0d40cc09f"},
		{"item unkeyed index 1", Item("crawl", "listings.raw", "it_abc", 0, "", 1), "it_ed2dc7f80369ec2d0b25cbbbea3ca99f"},
		{"item generation", Item("crawl", "listings.raw", "it_abc", 2, "", 0), "it_7f82ee7880ea44f827aec4d6db947550"},
		{"trigger item", TriggerItem("daily", "2026-10-14T06:00Z"), "it_bea56cf8f4c9e5aa5300a47d723b1147"},
		{"delivery", Delivery("score", "it_abc", 0), "dl_97b5cc0cd5a9ea320c8e6080daa86944"},
		{"delivery generation", Delivery("score", "it_abc", 3), "dl_db1b24ea30608ee7d9fd6eb68913bd62"},
		{"idempotency", Idempotency("score", "it_abc", 0), "97b5cc0cd5a9ea320c8e6080daa86944"},
		{"dead letter dead", DeadLetter("score.dlq", "dl_abc", "", 0), "it_0e60e27b5c3301512bce44f50c74d4c7"},
		{"dead letter unrouted", DeadLetter("score.dlq", "dl_abc", "it_out", 0), "it_46ab9e836d32ac74c9f3b4219c75218e"},
		{"dead letter dead again", DeadLetter("score.dlq", "dl_abc", "", 1), "it_d7b859ded7330cc766cd7f6c09048949"},
		{"cron key in UTC", CronKey(time.Date(2026, 10, 14, 8, 0, 42, 0, cest)), "2026-10-14T06:00Z"},
		{"cron run", CronRun(time.Date(2026, 10, 14, 8, 0, 42, 0, cest), "daily"), "run_2026-10-14T06:00Z_daily"},
		{"manual run", ManualRun("req42", "rescore"), "run_req42_rescore"},
		{"replay run", ReplayRun("req42"), "run_replay_req42"},
		{"import item", ImportItem("approved", "it_abc"), "it_a0d07f2776e2756802761d3b0d7f6264"},
		{"import run", ImportRun("approved", "run_req42_go"), "run_import_8d6f121b92921195ee9861b7f5f3fbee"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("got %s, want %s", tt.got, tt.want)
			}
		})
	}
}

func TestUpperSnake(t *testing.T) {
	t.Parallel()
	tests := []struct{ in, want string }{
		{"homesiteToken", "HOMESITE_TOKEN"},
		{"min-score", "MIN_SCORE"},
		{"minScore", "MIN_SCORE"},
		{"city", "CITY"},
		{"apiURL", "API_URL"},
		{"HTTPServer", "HTTP_SERVER"},
		{"maxPrice2", "MAX_PRICE2"},
		{"v2Token", "V2_TOKEN"},
		{"min_margin_pct", "MIN_MARGIN_PCT"},
		{"a.b c", "A_B_C"},
		{"--lead--trail--", "LEAD_TRAIL"},
		{"", ""},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			if got := UpperSnake(tt.in); got != tt.want {
				t.Errorf("UpperSnake(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
