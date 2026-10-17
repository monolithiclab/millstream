package pipeline

import (
	"strings"
	"testing"
)

// Pipeline names are lowercase words joined by single dashes; the first manual trigger by name is the default one.
func TestValidNameAndFirstManualTrigger(t *testing.T) {
	for name, want := range map[string]bool{"listings": true, "m1-fixture": true, "Listings": false, "a--b": false,
		"-a": false, "": false} {
		if ValidName(name) != want {
			t.Errorf("ValidName(%q) = %v", name, !want)
		}
	}
	p := &Pipeline{Triggers: map[string]Trigger{"daily": {Cron: &CronSpec{}}, "go": {Manual: &ManualSpec{}},
		"adhoc": {Manual: &ManualSpec{}}}}
	if name, ok := p.FirstManualTrigger(); !ok || name != "adhoc" {
		t.Errorf("FirstManualTrigger = %q, %v", name, ok)
	}
	if _, ok := (&Pipeline{}).FirstManualTrigger(); ok {
		t.Error("FirstManualTrigger of a pipeline without one: ok")
	}
}

// A trigger's input schema comes back as a JSON document; a trigger without one, or none, is nil.
func TestTriggerSchema(t *testing.T) {
	pkg := &Package{Dir: ".", Pipeline: &Pipeline{Triggers: map[string]Trigger{
		"hook": {InputSchema: map[string]any{"type": "object"}}, "go": {Manual: &ManualSpec{}},
	}}}
	if doc, err := pkg.TriggerSchema("hook"); err != nil || strings.TrimSpace(string(doc)) != `{"type":"object"}` {
		t.Errorf("TriggerSchema(hook) = %s, %v", doc, err)
	}
	for _, name := range []string{"go", "nope"} {
		if doc, err := pkg.TriggerSchema(name); err != nil || doc != nil {
			t.Errorf("TriggerSchema(%s) = %s, %v", name, doc, err)
		}
	}
}
