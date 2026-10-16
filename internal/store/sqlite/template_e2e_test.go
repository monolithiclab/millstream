//go:build e2e

package sqlite

import "testing"

// The embedded template image has the schema and recorded versions the migrations build now.
func TestTemplateImageSchema(t *testing.T) {
	tmpl, err := memoryTemplate()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := describe(t, tmpl), describe(t, migrated(t)); got != want {
		t.Fatalf("template.db is stale, rewrite it: go test ./internal/store/sqlite -run TestTemplateImage -update"+
			"\ngot:  %s\nwant: %s", got, want)
	}
}
