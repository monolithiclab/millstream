package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/artifacts"
)

func TestPipelines(t *testing.T) {
	f := readFixture(t)
	ps := get[[]Pipeline](t, f.srv, "/api/v1/pipelines")
	if len(ps) != 2 || ps[1].Name != "nightly" || ps[1].LastRun != nil {
		t.Fatalf("pipelines = %+v", ps)
	}
	p := ps[0]
	if p.Name != "listings" || p.State != "active" || p.Revision != 1 || len(p.Steps) != 3 || p.OpenDeadLetters != 1 ||
		p.OpenReviews != 3 || p.LastRun == nil || p.LastRun.ID != "r4" || len(p.Triggers) != 2 {
		t.Errorf("pipeline = %+v", p)
	}
	if s := p.Steps[1]; s.Name != "crawl" || s.Builtin || s.DLQ != "crawl.dlq" || s.Concurrency != 1 {
		t.Errorf("crawl = %+v", s)
	}
	if s := p.Steps[2]; s.Name != "score" || !slices.Equal(s.Out, []string{"listings.good", "listings.rest"}) {
		t.Errorf("score = %+v", s)
	}
	if tr := p.Triggers[1]; tr.Kind != "cron" || tr.Schedule != "0 6 * * *" || tr.Stream != "trigger.daily" {
		t.Errorf("daily = %+v", tr)
	}
	if one := get[Pipeline](t, f.srv, "/api/v1/pipelines/listings"); one.Name != "listings" || one.Description == "" {
		t.Errorf("pipeline = %+v", one)
	}
	wantError(t, do(t, f.srv, request{target: "/api/v1/pipelines/nope"}), http.StatusNotFound, codeNotFound)
}

func TestRevisions(t *testing.T) {
	f := readFixture(t)
	revs := get[[]Revision](t, f.srv, "/api/v1/pipelines/listings/revisions")
	if len(revs) != 1 || !revs[0].Active || revs[0].PackageHash != "pkg1" || revs[0].Graph != nil {
		t.Fatalf("revisions = %+v", revs)
	}
	rev := get[Revision](t, f.srv, "/api/v1/pipelines/listings/revisions/1")
	if rev.Graph == nil || len(rev.Graph.Steps) != 3 || !slices.Contains(rev.Graph.Streams, "check.dlq") {
		t.Errorf("revision = %+v", rev)
	}
	for _, tt := range []struct {
		target, code string
		status       int
	}{
		{"/api/v1/pipelines/listings/revisions/x", codeInvalid, http.StatusBadRequest},
		{"/api/v1/pipelines/listings/revisions/9", codeNotFound, http.StatusNotFound},
		{"/api/v1/pipelines/nope/revisions", codeNotFound, http.StatusNotFound},
		{"/api/v1/pipelines/nope/revisions/1", codeNotFound, http.StatusNotFound},
	} {
		wantError(t, do(t, f.srv, request{target: tt.target}), tt.status, tt.code)
	}
}

func TestStreams(t *testing.T) {
	f := readFixture(t)
	streams := get[[]Stream](t, f.srv, "/api/v1/pipelines/listings/streams")
	byName := map[string]Stream{}
	for _, s := range streams {
		byName[s.Name] = s
	}
	if raw := byName["listings.raw"]; raw.Items != 6 || !slices.Equal(raw.Producers, []string{"crawl"}) ||
		!slices.Equal(raw.Consumers, []string{"score"}) {
		t.Errorf("listings.raw = %+v", raw)
	}
	if tr := byName["trigger.adhoc"]; tr.Items != 4 || !slices.Equal(tr.Producers, []string{"trigger.adhoc"}) {
		t.Errorf("trigger.adhoc = %+v", tr)
	}
	if dlq := byName["score.dlq"]; dlq.Items != 1 {
		t.Errorf("score.dlq = %+v", dlq)
	}
	wantError(t, do(t, f.srv, request{target: "/api/v1/pipelines/nope/streams"}), http.StatusNotFound, codeNotFound)
}

func TestRuns(t *testing.T) {
	f := readFixture(t)
	page := get[Page[Run]](t, f.srv, "/api/v1/pipelines/listings/runs?limit=3")
	if len(page.Items) != 3 || page.Items[0].ID != "r4" || page.Next == "" {
		t.Fatalf("first page = %+v", page)
	}
	next := get[Page[Run]](t, f.srv, "/api/v1/pipelines/listings/runs?limit=3&cursor="+page.Next)
	if len(next.Items) != 1 || next.Items[0].ID != "r1" || next.Next != "" {
		t.Fatalf("second page = %+v", next)
	}
	if w := get[Page[Run]](t, f.srv, "/api/v1/pipelines/listings/runs?state=succeeded"); len(w.Items) != 2 {
		t.Errorf("succeeded runs = %+v", w)
	}
	run := get[Run](t, f.srv, "/api/v1/pipelines/listings/runs/r1")
	if run.State != "completedWithErrors" && run.State != "waitingReview" || run.Items["listings.raw"] != 2 ||
		run.OpenDeadLetters == nil || *run.OpenDeadLetters != 1 || run.Vars["minScore"] != float64(70) {
		t.Errorf("run = %+v", run)
	}
	rep := get[map[string]any](t, f.srv, "/api/v1/pipelines/listings/runs/r1/report")
	if rep["run"] != "r1" {
		t.Errorf("report = %v", rep)
	}
	ds := get[[]Delivery](t, f.srv, "/api/v1/pipelines/listings/runs/r1/deliveries")
	if len(ds) != 4 {
		t.Errorf("r1 deliveries = %+v", ds)
	}
	if dead := get[[]Delivery](t, f.srv, "/api/v1/pipelines/listings/runs/r1/deliveries?state=dead&step=score"); len(dead) != 1 ||
		dead[0].ErrorClass != "nonRetryable" {
		t.Errorf("dead deliveries = %+v", dead)
	}
	for _, tt := range []struct {
		target, code string
		status       int
	}{
		{"/api/v1/pipelines/listings/runs?limit=0", codeInvalid, http.StatusBadRequest},
		{"/api/v1/pipelines/listings/runs?limit=x", codeInvalid, http.StatusBadRequest},
		{"/api/v1/pipelines/listings/runs?cursor=" + encodeCursor("items", 1), codeInvalid, http.StatusBadRequest},
		{"/api/v1/pipelines/listings/runs?cursor=%21", codeInvalid, http.StatusBadRequest},
		{"/api/v1/pipelines/nope/runs", codeNotFound, http.StatusNotFound},
		{"/api/v1/pipelines/listings/runs/nope", codeNotFound, http.StatusNotFound},
		{"/api/v1/pipelines/listings/runs/nope/report", codeNotFound, http.StatusNotFound},
		{"/api/v1/pipelines/listings/runs/nope/deliveries", codeNotFound, http.StatusNotFound},
	} {
		rec := do(t, f.srv, request{target: tt.target})
		if tt.status != 0 {
			wantError(t, rec, tt.status, tt.code)
		}
	}
}

func TestItems(t *testing.T) {
	f := readFixture(t)
	all := get[Page[Item]](t, f.srv, "/api/v1/pipelines/listings/items?stream=listings.raw")
	if len(all.Items) != 6 || all.Next != "" {
		t.Fatalf("raw items = %+v", all)
	}
	first := get[Page[Item]](t, f.srv, "/api/v1/pipelines/listings/items?stream=listings.raw&limit=5")
	if len(first.Items) != 5 || first.Next == "" {
		t.Fatalf("first page = %+v", first)
	}
	rest := get[Page[Item]](t, f.srv, "/api/v1/pipelines/listings/items?stream=listings.raw&limit=5&cursor="+first.Next)
	if len(rest.Items) != 1 || rest.Items[0].Key != "f" || rest.Next != "" {
		t.Fatalf("second page = %+v", rest)
	}
	where := url.QueryEscape("item.price >= 100 && vars.minScore == 70 && run.id == 'r1'")
	sel := get[Page[Item]](t, f.srv, "/api/v1/pipelines/listings/items?stream=listings.raw&where="+where)
	if len(sel.Items) != 1 || sel.Items[0].Key != "a" || len(sel.Items[0].Artifacts) != 1 {
		t.Errorf("where = %+v", sel)
	}
	paged := get[Page[Item]](t, f.srv, "/api/v1/pipelines/listings/items?limit=1&where="+url.QueryEscape("has(item.price)"))
	if len(paged.Items) != 1 || paged.Next == "" {
		t.Errorf("filtered page = %+v", paged)
	}
	it := get[Item](t, f.srv, "/api/v1/pipelines/listings/items/"+f.good.ID)
	if it.Key != "a" || it.Stream != "listings.raw" || string(it.Payload) != `{"price":100}` {
		t.Errorf("item = %+v", it)
	}
	ds := get[[]Delivery](t, f.srv, "/api/v1/pipelines/listings/items/"+f.good.ID+"/deliveries")
	if len(ds) != 1 || ds[0].Step != "score" || ds[0].State != "done" {
		t.Errorf("item deliveries = %+v", ds)
	}
	for _, tt := range []struct {
		target, code string
		status       int
	}{
		{"/api/v1/pipelines/listings/items?where=" + url.QueryEscape("item."), codeInvalid, http.StatusBadRequest},
		{"/api/v1/pipelines/listings/items?where=" + url.QueryEscape("item.nope == 1"), codeInvalid, http.StatusBadRequest},
		{"/api/v1/pipelines/listings/items?limit=501", codeInvalid, http.StatusBadRequest},
		{"/api/v1/pipelines/listings/items?cursor=" + encodeCursor("items", 1, 2), codeInvalid, http.StatusBadRequest},
		{"/api/v1/pipelines/nope/items", codeNotFound, http.StatusNotFound},
		{"/api/v1/pipelines/listings/items/nope", codeNotFound, http.StatusNotFound},
		{"/api/v1/pipelines/listings/items/nope/deliveries", codeNotFound, http.StatusNotFound},
		{"/api/v1/pipelines/listings/items/nope/lineage", codeNotFound, http.StatusNotFound},
		{"/api/v1/pipelines/listings/items/" + f.good.ID + "/lineage?depth=0", codeInvalid, http.StatusBadRequest},
	} {
		wantError(t, do(t, f.srv, request{target: tt.target}), tt.status, tt.code)
	}
}

func TestLineage(t *testing.T) {
	f := readFixture(t)
	l := get[Lineage](t, f.srv, "/api/v1/pipelines/listings/items/"+f.good.ID+"/lineage")
	if len(l.Up) != 1 || l.Up[0].ID != f.trigger.ID || l.Truncated {
		t.Errorf("up = %+v", l)
	}
	// Down: the scored listing on listings.good, then nothing (the review is open).
	if len(l.Down) != 1 || l.Down[0].Stream != "listings.good" {
		t.Errorf("down = %+v", l.Down)
	}
	root := get[Lineage](t, f.srv, "/api/v1/pipelines/listings/items/"+f.trigger.ID+"/lineage?depth=1")
	if len(root.Up) != 0 || len(root.Down) != 2 || !root.Truncated {
		t.Errorf("trigger lineage at depth 1 = %+v", root)
	}
	deep := get[Lineage](t, f.srv, "/api/v1/pipelines/listings/items/"+f.trigger.ID+"/lineage?depth=5")
	if len(deep.Down) != 4 || deep.Truncated { // a, b, the scored a, b's dead letter
		t.Errorf("trigger lineage = %+v", deep)
	}
}

func TestDeliveries(t *testing.T) {
	f := readFixture(t)
	d := get[Delivery](t, f.srv, "/api/v1/pipelines/listings/deliveries/"+f.crawlDelivery)
	if d.State != "done" || len(d.Attempts) != 1 {
		t.Fatalf("crawl delivery = %+v", d)
	}
	a := d.Attempts[0]
	if a.Outcome != "done" || a.Owner != "embedded-1" || a.CostUSD != 0.25 || len(a.Artifacts) != 2 ||
		a.Artifacts[1].Name != "transcript.jsonl" || a.LeasedAt == nil || a.EndedAt == nil {
		t.Errorf("attempt = %+v", a)
	}
	dead := get[Delivery](t, f.srv, "/api/v1/pipelines/listings/deliveries/"+f.deadDelivery)
	if len(dead.Attempts) != 1 || dead.Attempts[0].Outcome != "dead" || dead.Attempts[0].ErrorClass != "nonRetryable" {
		t.Errorf("dead delivery = %+v", dead)
	}
	parked := get[Delivery](t, f.srv, "/api/v1/pipelines/listings/deliveries/"+f.reviewDelivery)
	if parked.State != "awaitingReview" || parked.Attempts[0].Outcome != "awaitingReview" {
		t.Errorf("parked delivery = %+v", parked)
	}
	wantError(t, do(t, f.srv, request{target: "/api/v1/pipelines/listings/deliveries/nope"}), http.StatusNotFound,
		codeNotFound)
}

func TestArtifacts(t *testing.T) {
	f := readFixture(t)
	transcript := "/api/v1/pipelines/listings/deliveries/" + f.crawlDelivery + "/attempts/1/artifacts/transcript.jsonl"
	rec := do(t, f.srv, request{target: transcript})
	h := rec.Header()
	if rec.Code != http.StatusOK || rec.Body.String() != `{"role":"user"}`+"\n" ||
		h.Get("Content-Type") != "text/plain; charset=utf-8" || !strings.HasPrefix(h.Get("Content-Disposition"), "inline") ||
		h.Get("Content-Security-Policy") == "" || h.Get("X-Content-Type-Options") != "nosniff" {
		t.Errorf("transcript: %d %v %q", rec.Code, h, rec.Body.String())
	}
	// What a browser would run is downloaded, never shown.
	rec = do(t, f.srv, request{target: strings.Replace(transcript, "transcript.jsonl", "page.html", 1)})
	if rec.Code != http.StatusOK || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/html") ||
		rec.Header().Get("Content-Disposition") != "attachment; filename=page.html" {
		t.Errorf("page: %d %v", rec.Code, rec.Header())
	}
	photo := "/api/v1/pipelines/listings/items/" + f.good.ID + "/artifacts/photo.png"
	rec = do(t, f.srv, request{target: photo})
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "image/png" ||
		rec.Header().Get("Content-Disposition") != `inline; filename=photo.png` {
		t.Errorf("photo: %d %v", rec.Code, rec.Header())
	}
	noStore, err := New(Options{Store: f.st})
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		h            http.Handler
		target, code string
		status       int
	}{
		{f.srv, "/api/v1/pipelines/listings/items/" + f.good.ID + "/artifacts/nope.png", codeNotFound, 404},
		{f.srv, "/api/v1/pipelines/listings/items/nope/artifacts/photo.png", codeNotFound, 404},
		{f.srv, strings.Replace(transcript, "/1/", "/2/", 1), codeNotFound, 404},
		{f.srv, strings.Replace(transcript, "/1/", "/x/", 1), codeInvalid, 400},
		{f.srv, "/api/v1/pipelines/listings/deliveries/nope/attempts/1/artifacts/a", codeNotFound, 404},
		{noStore, photo, codeUnavailable, 503},
	} {
		wantError(t, do(t, tt.h, request{target: tt.target}), tt.status, tt.code)
	}
	// An artifact the item names whose content is gone from the store.
	gone := must(New(Options{Store: f.st, Artifacts: artifacts.New(t.TempDir())}))
	wantError(t, do(t, gone, request{target: photo}), http.StatusNotFound, codeNotFound)
}

func TestDeadLetters(t *testing.T) {
	f := readFixture(t)
	groups := get[[]DeadLetterGroup](t, f.srv, "/api/v1/pipelines/listings/dead-letters")
	if len(groups) != 1 {
		t.Fatalf("groups = %+v", groups)
	}
	g := groups[0]
	if g.Step != "score" || g.Kind != "dead" || g.Count != 1 || g.Error != "nonRetryable: price <n> is below the floor" ||
		len(g.DeadLetters) != 1 || g.DeadLetters[0].RunID != "r1" {
		t.Errorf("group = %+v", g)
	}
	if all := get[[]DeadLetterGroup](t, f.srv, "/api/v1/pipelines/listings/dead-letters?state=all"); len(all) != 1 {
		t.Errorf("all = %+v", all)
	}
	if none := get[[]DeadLetterGroup](t, f.srv, "/api/v1/pipelines/listings/dead-letters?state=replayed"); len(none) != 0 {
		t.Errorf("replayed = %+v", none)
	}
	wantError(t, do(t, f.srv, request{target: "/api/v1/pipelines/listings/dead-letters?state=x"}), 400, codeInvalid)
	wantError(t, do(t, f.srv, request{target: "/api/v1/pipelines/nope/dead-letters"}), 404, codeNotFound)
}

func TestLessons(t *testing.T) {
	f := readFixture(t)
	if ls := get[[]StepLessons](t, f.srv, "/api/v1/pipelines/nightly/lessons"); len(ls) != 0 {
		t.Errorf("lessons of a package without a bundle = %+v", ls)
	}
	ls := get[[]StepLessons](t, f.srv, "/api/v1/pipelines/listings/lessons")
	if len(ls) != 1 || ls[0].Step != "score" || len(ls[0].Rules) != 1 || ls[0].Rules[0].Source != "review/dl_1" ||
		len(ls[0].Examples) != 1 {
		t.Errorf("lessons = %+v", ls)
	}
	wantError(t, do(t, f.srv, request{target: "/api/v1/pipelines/nope/lessons"}), 404, codeNotFound)
}

func TestInbox(t *testing.T) {
	f := readFixture(t)
	page := get[Page[Review]](t, f.srv, "/api/v1/inbox?inbox=analysts&limit=2")
	if len(page.Items) != 2 || page.Next == "" {
		t.Fatalf("inbox = %+v", page)
	}
	rest := get[Page[Review]](t, f.srv, "/api/v1/inbox?inbox=analysts&limit=2&cursor="+page.Next)
	if len(rest.Items) != 1 || rest.Next != "" || rest.Items[0].DeliveryID == page.Items[1].DeliveryID {
		t.Fatalf("second page = %+v", rest)
	}
	r := page.Items[0]
	if r.DeliveryID != f.reviewDelivery || r.Version != 1 || r.Display != "Listing a" ||
		!slices.Equal(r.Editable, []string{"/score"}) || string(r.Item.Payload) != `{"price":100,"score":90}` {
		t.Errorf("review = %+v", r)
	}
	if decided := get[Page[Review]](t, f.srv, "/api/v1/inbox?state=decided&limit=2"); len(decided.Items) != 2 ||
		decided.Items[0].Reviewer != "rule" {
		t.Errorf("decided = %+v", decided)
	}
	one := get[Review](t, f.srv, "/api/v1/pipelines/listings/reviews/"+f.reviewDelivery)
	if one.State != "open" {
		t.Errorf("review = %+v", one)
	}
	samples := get[Page[Sample]](t, f.srv, "/api/v1/inbox/samples?pipeline=listings&limit=1")
	if len(samples.Items) != 1 || samples.Items[0].DeliveryID != f.sampleDelivery || samples.Items[0].State != "open" ||
		samples.Next == "" {
		t.Errorf("samples = %+v", samples)
	}
	if more := get[Page[Sample]](t, f.srv, "/api/v1/inbox/samples?limit=1&cursor="+samples.Next); len(more.Items) != 1 ||
		more.Next != "" {
		t.Errorf("more samples = %+v", more)
	}
	if s := get[Sample](t, f.srv, "/api/v1/pipelines/listings/samples/"+f.sampleDelivery); s.Inbox != "analysts" {
		t.Errorf("sample = %+v", s)
	}
	for _, tt := range []struct {
		target, code string
		status       int
	}{
		{"/api/v1/inbox?limit=-1", codeInvalid, 400},
		{"/api/v1/inbox?cursor=x", codeInvalid, 400},
		{"/api/v1/inbox/samples?limit=0", codeInvalid, 400},
		{"/api/v1/inbox/samples?cursor=" + encodeCursor("reviews", 1), codeInvalid, 400},
		{"/api/v1/pipelines/listings/reviews/nope", codeNotFound, 404},
		{"/api/v1/pipelines/listings/samples/nope", codeNotFound, 404},
	} {
		wantError(t, do(t, f.srv, request{target: tt.target}), tt.status, tt.code)
	}
}

func TestInsights(t *testing.T) {
	f := readFixture(t)
	ins := get[[]Insight](t, f.srv, "/api/v1/insights?pipeline=listings&state=open")
	if len(ins) != 1 || ins[0].ID != "in_1" || ins[0].Summary == "" || ins[0].Scope != "score" {
		t.Errorf("insights = %+v", ins)
	}
	if none := get[[]Insight](t, f.srv, "/api/v1/insights?state=muted"); len(none) != 0 {
		t.Errorf("muted = %+v", none)
	}
}

func TestMetrics(t *testing.T) {
	f := readFixture(t)
	names := get[[]string](t, f.srv, "/api/v1/metrics/names?since=1h")
	if !slices.Contains(names, "items_in") {
		t.Fatalf("names = %v", names)
	}
	series := get[[]MetricSeries](t, f.srv, "/api/v1/metrics?name=items_in&groupBy=step&from=2026-10-15T05:00:00Z")
	groups := map[string]MetricSeries{}
	for _, s := range series {
		groups[s.Group] = s
	}
	if len(groups["crawl"].Points) == 0 || len(groups["score"].Points) == 0 {
		t.Fatalf("series = %+v", series)
	}
	total := get[[]MetricSeries](t, f.srv, "/api/v1/metrics?name=items_in&pipeline=listings&since=1h")
	if len(total) != 1 || total[0].Group != "items_in" {
		t.Errorf("ungrouped = %+v", total)
	}
	if early := get[[]MetricSeries](t, f.srv, "/api/v1/metrics?name=items_in&from=2026-10-15T04:00:00Z&to=2026-10-15T05:00:00Z"); len(early) != 0 {
		t.Errorf("before the runs = %+v", early)
	}
	for _, target := range []string{
		"/api/v1/metrics", "/api/v1/metrics?name=x&since=forever", "/api/v1/metrics?name=x&from=yesterday",
		"/api/v1/metrics?name=x&from=2026-10-15T06:00:00Z&to=2026-10-15T05:00:00Z", "/api/v1/metrics/names?to=x",
	} {
		wantError(t, do(t, f.srv, request{target: target}), 400, codeInvalid)
	}
}

func TestNodes(t *testing.T) {
	f := readFixture(t)
	n := get[Nodes](t, f.srv, "/api/v1/nodes")
	if n.Engine.State != "active" || n.Engine.Version != "v-test" || n.Engine.NotificationsFailed != 2 ||
		len(n.Nodes) != 1 || n.Nodes[0].Leases != 0 ||
		n.Nodes[0].Labels == nil {
		t.Errorf("nodes = %+v", n)
	}
}

func TestOpenAPIServed(t *testing.T) {
	f := readFixture(t)
	rec := do(t, f.srv, request{target: "/api/v1/openapi.yaml"})
	if rec.Code != http.StatusOK || !strings.HasPrefix(rec.Body.String(), "openapi: 3.1.0") {
		t.Errorf("openapi.yaml: %d", rec.Code)
	}
}

// Every path the API serves is in the document, with the methods it answers, and the document has no operation the
// API does not serve.
func TestRoutesDocumented(t *testing.T) {
	c := theContract()
	paths, _ := c.doc["paths"].(map[string]any)
	served := map[string]bool{}
	for _, rt := range routes {
		op := rt.method + " " + rt.path
		served[op] = true
		if _, ok := c.lookup("paths", rt.path, strings.ToLower(rt.method), "responses"); !ok {
			t.Errorf("%s is not documented", op)
		}
	}
	for p, item := range paths {
		for m := range item.(map[string]any) {
			if m == "parameters" {
				continue
			}
			if op := strings.ToUpper(m) + " " + p; !served[op] {
				t.Errorf("%s is documented but not served", op)
			}
		}
	}
}

// Paths the API does not have, and methods a path does not answer, get JSON errors; other paths are not the API's.
func TestUnrouted(t *testing.T) {
	f := readFixture(t)
	wantError(t, do(t, f.srv, request{target: "/api/v1/nope"}), http.StatusNotFound, codeNotFound)
	wantError(t, do(t, f.srv, request{target: "/api/v2/pipelines"}), http.StatusNotFound, codeNotFound)
	rec := do(t, f.srv, request{method: http.MethodDelete, target: "/api/v1/pipelines/listings/runs"})
	wantError(t, rec, http.StatusMethodNotAllowed, codeMethodNotAllowed)
	if got := rec.Header().Get("Allow"); got != "GET, POST" {
		t.Errorf("Allow = %q", got)
	}
	ui := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ui")) })
	h := Handler(f.srv, ui)
	for _, tt := range []struct{ host, target, want string }{
		{"127.0.0.1:7070", "/inbox", "ui"},
		{"localhost:7070", "/api/v1/pipelines/nope", `"notFound"`},
		{"[::1]:7070", "/inbox", "ui"},
		{"localhost", "/inbox", "ui"},
		// A name that resolves here (DNS rebinding), or any other host, is refused.
		{"evil.example:7070", "/api/v1/pipelines", `"forbidden"`},
		{"192.168.1.2:7070", "/inbox", `"forbidden"`},
	} {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, tt.target, nil)
		r.Host = tt.host
		h.ServeHTTP(rec, r)
		if !strings.Contains(rec.Body.String(), tt.want) {
			t.Errorf("GET %s on %s = %q, want %q", tt.target, tt.host, rec.Body.String(), tt.want)
		}
	}
	// A POST without a JSON content type, which a page elsewhere can send without asking, changes nothing.
	for _, ct := range []string{"", "text/plain", "application/x-www-form-urlencoded"} {
		rec := do(t, f.srv, request{method: http.MethodPost, target: "/api/v1/pipelines/listings/pause",
			header: map[string]string{"Content-Type": ct}})
		wantError(t, rec, http.StatusUnsupportedMediaType, codeUnsupportedMediaType)
	}
}

func Test_errorCode(t *testing.T) {
	for _, tt := range []struct {
		err    error
		status int
		code   string
	}{
		{errUnavailable, 503, codeUnavailable},
		{&http.MaxBytesError{Limit: 1}, 413, codeTooLarge},
		{json.Unmarshal([]byte("x"), new(int)), 500, codeInternal},
	} {
		if status, code := errorCode(tt.err); status != tt.status || code != tt.code {
			t.Errorf("errorCode(%v) = %d %s, want %d %s", tt.err, status, code, tt.status, tt.code)
		}
	}
}

// recordingTB records the failures the contract reports, to check that it reports them.
type recordingTB struct {
	testing.TB
	errs []string
}

func (r *recordingTB) Helper() {}

func (r *recordingTB) Errorf(format string, args ...any) {
	r.errs = append(r.errs, fmt.Sprintf(format, args...))
}

// The contract check fails a response its schema refuses, an undocumented status and an undocumented type.
func TestContractCatches(t *testing.T) {
	for _, tt := range []struct {
		status      int
		contentType string
		body        string
	}{
		{200, "application/json", `{"count":"one"}`},
		{200, "application/json", `{"count":1,"extra":true}`},
		{418, "application/json", `{}`},
		{200, "text/plain", `1`},
		{200, "application/json", `not json`},
	} {
		rec := httptest.NewRecorder()
		rec.Header().Set("Content-Type", tt.contentType)
		rec.WriteHeader(tt.status)
		rec.Body.WriteString(tt.body)
		r := &recordingTB{TB: t}
		theContract().validate(r, http.MethodPost, "/pipelines/{pipeline}/dead-letters/discard", rec)
		if len(r.errs) == 0 {
			t.Errorf("%d %s %s: no failure reported", tt.status, tt.contentType, tt.body)
		}
	}
}
