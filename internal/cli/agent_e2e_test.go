package cli

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

// Test_runAgentRecordReplay runs an agent step through millstream run --once, unsandboxed as the environment file
// allows: first recording against a fake OpenAI-compatible server, then replaying with the server gone. Each run
// keeps the attempt's transcript.
func Test_runAgentRecordReplay(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer sk-test-0123456789" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"` + "```json\\n{\\\"score\\\": 61}\\n```" +
			`"}}],"usage":{"prompt_tokens":40,"completion_tokens":9}}`))
	}))
	pkg := runPackage(t, "agent-run")
	manifest := "name: lab/score\nkind: agent\noutput: { type: object, required: [score] }\n" +
		"runtime: { agent: { harness: api, provider: openai, baseUrl: \"" + srv.URL + "\", model: mistral-large, " +
		"apiKeySecret: apiKey, network: [api.mistral.ai] } }\n"
	if err := os.WriteFile(filepath.Join(pkg, "actions", "score", "action.yaml"), []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	envFile := filepath.Join(t.TempDir(), "millstream.yaml")
	if err := os.WriteFile(envFile, []byte("agents: { allowUnsandboxed: true }\n"+
		"prices: { mistral-large: { inPerMtok: 2, outPerMtok: 6 } }\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	recordings := filepath.Join(t.TempDir(), "recordings")
	env := map[string]string{"MILLSTREAM_SECRET_API_KEY": "sk-test-0123456789",
		"MILLSTREAM_HARNESS_MODE": "record", "MILLSTREAM_HARNESS_DIR": recordings}
	want := "listings run_r1_adhoc succeeded\n  scored 1\n  trigger.adhoc 1\n"
	for i, mode := range []string{"record", "replay"} {
		if mode == "replay" {
			srv.Close()
		}
		env["MILLSTREAM_HARNESS_MODE"] = mode
		db := migratedDB(t)
		code, out, errOut := runCLICtx(t, context.Background(), env, "--db", db, "--env-file", envFile, "run", "--once",
			"--request-id", "r1", pkg)
		if code != 0 || out != want || !strings.Contains(errOut, "agent running without a sandbox") {
			t.Fatalf("%s: code %d\nstdout:\n%s\nstderr:\n%s", mode, code, out, errOut)
		}
		if n := calls.Load(); n != 1 {
			t.Fatalf("%s: the model was called %d times", mode, n)
		}
		st, err := sqlite.Open(db, sqlite.Options{})
		if err != nil {
			t.Fatal(err)
		}
		ds, err := st.Deliveries(t.Context(), store.DeliveryFilter{Pipeline: "listings"})
		if err != nil || len(ds) != 1 {
			t.Fatalf("deliveries %+v, %v", ds, err)
		}
		arts, err := st.AttemptArtifacts(t.Context(), "listings", ds[0].ID)
		_ = st.Close()
		if err != nil || len(arts) != 1 || arts[0].Name != "transcript.json" || arts[0].Attempt != 1 {
			t.Fatalf("run %d: attempt artifacts %+v, %v", i, arts, err)
		}
		transcript, err := os.ReadFile(filepath.Join(filepath.Dir(db), "artifacts", "listings", arts[0].SHA256))
		if err != nil || strings.Contains(string(transcript), "sk-test-0123456789") ||
			!strings.Contains(string(transcript), `"costUsd": 0.000134`) {
			t.Fatalf("transcript %s, %v", transcript, err)
		}
	}
}
