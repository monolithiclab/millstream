package engine

import (
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/store"
)

const varsPipeline = `
pipeline: listings
vars: { minScore: 70, city: Paris, maxPrice: null }
secrets: [apiToken]
triggers:
  adhoc: { manual: {} }
steps:
  crawl:
    action: ./actions/crawl
    in: trigger.adhoc
    config:
      max: "${vars.maxPrice}"
      token: "${secrets.apiToken}"
      label: "${vars.city} run ${run.trigger}"
`

func TestFireStoresVarsAndSources(t *testing.T) {
	t.Parallel()
	environ := map[string]string{"MILLSTREAM_VAR_CITY": "Lyon", "MILLSTREAM_SECRET_API_TOKEN": "s3cr3t-value"}
	v := newEnv(t, Options{
		Flags: map[string]string{"maxPrice": "300000", "notDeclared": "x"},
		Env:   func(k string) (string, bool) { s, ok := environ[k]; return s, ok },
	})
	v.apply(newPackage(t, varsPipeline, "crawl"))

	id := v.fire("listings", "adhoc", "")
	if !regexp.MustCompile(`^run_\d{8}T\d{6}Z-[0-9a-f]{8}_adhoc$`).MatchString(id) {
		t.Fatalf("run id %q: want run_<request id>_adhoc", id)
	}
	r := v.run("listings", id)
	if r.Vars["minScore"] != int64(70) || r.Vars["city"] != "Lyon" || r.Vars["maxPrice"] != int64(300000) {
		t.Fatalf("vars = %v", r.Vars)
	}
	want := map[string]string{"minScore": "default", "city": "env", "maxPrice": "flag"}
	for k, s := range want {
		if r.VarSources[k] != s {
			t.Fatalf("sources = %v, want %v", r.VarSources, want)
		}
	}

	again, created, err := v.e.Fire(v.ctx, "listings", "adhoc", strings.TrimSuffix(strings.TrimPrefix(id, "run_"),
		"_adhoc"), nil)
	if err != nil || created || again != id {
		t.Fatalf("refire = %q, %v, %v; want the same run, not created", again, created, err)
	}

	// The secret resolves at dispatch and is stored nowhere.
	as, err := v.e.Pull(v.ctx, "node-a", 1)
	if err != nil || len(as) != 1 {
		t.Fatalf("Pull = %v, %v", as, err)
	}
	if as[0].Spec.Config["token"] != "s3cr3t-value" {
		t.Fatalf("config = %v", as[0].Spec.Config)
	}
	for _, ev := range v.events("") {
		if strings.Contains(string(ev.Data), "s3cr3t-value") {
			t.Fatalf("event %s holds the secret: %s", ev.Kind, ev.Data)
		}
	}
	if strings.Contains(v.logs.String(), "s3cr3t-value") {
		t.Fatal("logs hold the secret")
	}
}

func TestFireErrors(t *testing.T) {
	t.Parallel()
	full := map[string]string{"MILLSTREAM_VAR_MAX_PRICE": "1", "MILLSTREAM_SECRET_API_TOKEN": "t"}
	tests := []struct {
		name     string
		env      map[string]string
		pipeline string
		trigger  string
		want     string
		wantErr  error
	}{
		{"required variable", map[string]string{"MILLSTREAM_SECRET_API_TOKEN": "t"}, "listings", "adhoc",
			"variable maxPrice is required", nil},
		{"missing secret", map[string]string{"MILLSTREAM_VAR_MAX_PRICE": "1"}, "listings", "adhoc",
			"secret apiToken is not set", nil},
		{"unknown trigger", full, "listings", "weekly", `has no trigger "weekly"`, store.ErrInvalid},
		{"unknown pipeline", full, "nope", "adhoc", "pipeline nope", store.ErrNotFound},
	}
	// One engine for every case: none fires, and each sets the environment it fires in.
	var environ map[string]string
	v := newEnv(t, Options{Env: func(k string) (string, bool) { s, ok := environ[k]; return s, ok }})
	v.apply(newPackage(t, varsPipeline, "crawl"))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			environ = tt.env
			_, _, err := v.e.Fire(v.ctx, tt.pipeline, tt.trigger, "", nil)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if rs, err := v.st.Runs(v.ctx, store.RunFilter{}); err != nil || len(rs) != 0 {
				t.Fatalf("runs = %v, %v; want none", rs, err)
			}
		})
	}
}

func TestNew(t *testing.T) {
	t.Parallel()
	if _, err := New(Options{}); err == nil {
		t.Fatal("New without a store succeeded")
	}
	v := newEnv(t, Options{})
	if v.e.LeaseTTL() != DefaultLeaseTTL || v.e.opts.TickInterval != DefaultTickInterval {
		t.Fatalf("defaults: ttl %s tick %s", v.e.LeaseTTL(), v.e.opts.TickInterval)
	}
	if _, ok := v.e.opts.Env("PATH"); ok {
		t.Fatal("the default environment is not empty")
	}
}
