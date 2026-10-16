package pipeline

import (
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"
	"text/template"
	"time"

	"github.com/monolithiclab/millstream/internal/expr"
)

// Review timeout actions (spec section 8): what onTimeout does to a review left undecided past its timeout.
const (
	OnTimeoutReject   = "reject"
	OnTimeoutApprove  = "approve"
	OnTimeoutEscalate = "escalate"
)

// Sample rates of a review step's auto-approvals (spec section 8).
const (
	DefaultSampleRate = 0.05
	MinSampleRate     = 0.01
)

// Review is a review step's config (spec section 8), parsed by ParseReview.
type Review struct {
	// Inbox is the inbox its items wait in; the step's name when the config sets none.
	Inbox string
	// Timeout is how long an item may wait, zero for ever.
	Timeout time.Duration
	// OnTimeout is what a timeout applies: reject (the default with a timeout), approve or escalate, which moves the
	// item to EscalateTo with a fresh deadline.
	OnTimeout  string
	EscalateTo string
	// Editable are the JSON pointers a reviewer may change before approving.
	Editable []string
	// Notify is the webhook URL told of every item parked, empty for none; NotifySecret, set instead, names the
	// pipeline secret holding it, so the URL stays out of the pipeline file.
	Notify       string
	NotifySecret string
	// Display renders an item for the reviewer; nil without a display template.
	Display *template.Template
	// Reasons are the reason codes offered; a rejection then needs one.
	Reasons []string
	// AutoApprove, when set, approves the items it holds for at once, recorded as approved by rule.
	AutoApprove *expr.ItemExpr
	// SampleRate is the share of auto-approved items also given to a person as a review sample.
	SampleRate float64
}

// ReviewError is a review config error, located by its config key.
type ReviewError struct {
	Key     string
	Message string
}

func (e ReviewError) Error() string { return e.Key + ": " + e.Message }

// ParseReview parses the config of review step step and returns every error it finds, each located by its key. The
// config is not interpolated: autoApprove is a CEL expression and display a Go text/template, rendered over the item
// (Review.Render).
func ParseReview(step string, cfg map[string]any) (*Review, []ReviewError) {
	r := &Review{Inbox: step, SampleRate: DefaultSampleRate, Editable: []string{}, Reasons: []string{}}
	var errs []ReviewError
	fail := func(key, format string, args ...any) {
		errs = append(errs, ReviewError{Key: key, Message: fmt.Sprintf(format, args...)})
	}
	str := func(key string) (string, bool) {
		v, set := cfg[key]
		if !set {
			return "", false
		}
		s, ok := v.(string)
		if !ok || s == "" {
			fail(key, "must be a non-empty string")
			return "", false
		}
		return s, true
	}
	list := func(key string, each func(i int, s string)) {
		v, set := cfg[key]
		if !set {
			return
		}
		l, ok := v.([]any)
		if !ok {
			fail(key, "must be a list of strings")
			return
		}
		for i, e := range l {
			s, ok := e.(string)
			if !ok || s == "" {
				fail(fmt.Sprintf("%s[%d]", key, i), "must be a non-empty string")
				continue
			}
			each(i, s)
		}
	}
	for _, k := range slices.Sorted(maps.Keys(cfg)) {
		if !slices.Contains(builtinKeys[BuiltinReview], k) {
			fail(k, "unknown key for the review builtin")
		}
	}
	if s, ok := str("inbox"); ok {
		if !validInbox(s) {
			fail("inbox", "an inbox name is letters, digits, dots, dashes and underscores")
		} else {
			r.Inbox = s
		}
	}
	if s, ok := str("timeout"); ok {
		d, err := ParseDuration(s)
		if err != nil {
			fail("timeout", "%v", err)
		}
		r.Timeout = d
	}
	errs = append(errs, r.parseOnTimeout(cfg)...)
	if r.OnTimeout == "" && r.Timeout > 0 {
		r.OnTimeout = OnTimeoutReject
	}
	if _, set := cfg["onTimeout"]; set && r.Timeout == 0 {
		fail("onTimeout", "needs a timeout")
	}
	list("editable", func(i int, s string) {
		if !strings.HasPrefix(s, "/") {
			fail(fmt.Sprintf("editable[%d]", i), "must be a JSON pointer such as /notes")
			return
		}
		r.Editable = append(r.Editable, s)
	})
	r.parseNotify(cfg, fail)
	if s, ok := str("display"); ok {
		t, err := template.New("display").Option("missingkey=zero").Parse(s)
		if err != nil {
			fail("display", "%v", err)
		}
		r.Display = t
	}
	seen := map[string]bool{}
	list("reasons", func(i int, s string) {
		if seen[s] {
			fail(fmt.Sprintf("reasons[%d]", i), "reason code %s is listed twice", s)
			return
		}
		seen[s] = true
		r.Reasons = append(r.Reasons, s)
	})
	if s, ok := str("autoApprove"); ok {
		e, err := expr.CompileItem(s)
		if err != nil {
			fail("autoApprove", "%v", err)
		}
		r.AutoApprove = e
	}
	if v, set := cfg["sampleRate"]; set {
		f, ok := number(v)
		switch {
		case !ok || f > 1:
			fail("sampleRate", "must be a number from %g to 1", MinSampleRate)
		case f < MinSampleRate:
			fail("sampleRate", "must be at least %g, so the rule is always measured", MinSampleRate)
		default:
			r.SampleRate = f
		}
	}
	return r, errs
}

// parseNotify reads notify: an http or https URL, or the mapping { secret: <name> }.
func (r *Review) parseNotify(cfg map[string]any, fail func(key, format string, args ...any)) {
	v, set := cfg["notify"]
	if !set {
		return
	}
	switch t := v.(type) {
	case string:
		if httpURL(t) {
			r.Notify = t
			return
		}
	case map[string]any:
		if name, ok := t["secret"].(string); ok && len(t) == 1 && name != "" {
			r.NotifySecret = name
			return
		}
	}
	fail("notify", "must be an http or https URL or { secret: <name> }")
}

// parseOnTimeout reads onTimeout: reject, approve, or a mapping { escalate: <inbox> }.
func (r *Review) parseOnTimeout(cfg map[string]any) []ReviewError {
	v, set := cfg["onTimeout"]
	if !set {
		return nil
	}
	bad := []ReviewError{{Key: "onTimeout", Message: "must be reject, approve or { escalate: <inbox> }"}}
	switch t := v.(type) {
	case string:
		if t != OnTimeoutReject && t != OnTimeoutApprove {
			return bad
		}
		r.OnTimeout = t
	case map[string]any:
		inbox, ok := t[OnTimeoutEscalate].(string)
		if len(t) != 1 || !ok || !validInbox(inbox) {
			return bad
		}
		r.OnTimeout, r.EscalateTo = OnTimeoutEscalate, inbox
	default:
		return bad
	}
	return nil
}

// validInbox reports whether s can name an inbox: it appears in inbox links, so it is limited to URL-safe
// characters.
func validInbox(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for _, c := range s {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && !strings.ContainsRune("._-", c) {
			return false
		}
	}
	return true
}

// httpURL reports whether s is an absolute http or https URL.
func httpURL(s string) bool {
	u, err := url.Parse(s)
	return err == nil && (u.Scheme == "https" || u.Scheme == "http") && u.Host != ""
}

// number returns a config number, which decoding makes an int64 or a float64.
func number(v any) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}

// MaxDisplay is the most bytes of an item's display rendering kept.
const MaxDisplay = 16 << 10

// Render renders the item for the reviewer with the display template, whose dot holds item (the payload), key, id
// and step: "" without a template. A template that fails for the item renders as the error, for the reviewer to see.
func (r *Review) Render(payload any, key, id, step string) string {
	if r.Display == nil {
		return ""
	}
	var b strings.Builder
	err := r.Display.Execute(&limitWriter{b: &b, n: MaxDisplay},
		map[string]any{"item": payload, "key": key, "id": id, "step": step})
	if err != nil && b.Len() < MaxDisplay {
		return "display template failed: " + err.Error()
	}
	return strings.ToValidUTF8(b.String(), "")
}

// limitWriter keeps the first n bytes written and drops the rest, so a template looping over a large item stays
// bounded.
type limitWriter struct {
	b *strings.Builder
	n int
}

func (w *limitWriter) Write(p []byte) (int, error) {
	if room := w.n - w.b.Len(); room > 0 {
		w.b.Write(p[:min(len(p), room)])
	}
	return len(p), nil
}
