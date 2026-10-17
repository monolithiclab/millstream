package expr

import "testing"

// An eval assertion sees output, input and vars; a non-boolean result, a runtime error and a compile error fail.
func TestAssertion(t *testing.T) {
	a, err := CompileAssertion("output.score >= vars.min && input.id == output.id")
	if err != nil {
		t.Fatal(err)
	}
	ok, err := a.Eval(map[string]any{"score": int64(80), "id": "L1"}, map[string]any{"id": "L1"},
		map[string]any{"min": int64(70)})
	if err != nil || !ok {
		t.Fatalf("Eval = %v, %v", ok, err)
	}
	if ok, err := a.Eval(map[string]any{"score": int64(60), "id": "L1"}, map[string]any{"id": "L1"},
		map[string]any{"min": int64(70)}); err != nil || ok {
		t.Fatalf("Eval of a failing case = %v, %v", ok, err)
	}
	if _, err := a.Eval(map[string]any{}, map[string]any{}, nil); err == nil {
		t.Fatal("Eval over a missing field: no error")
	}
	if _, err := CompileAssertion("output.score +"); err == nil {
		t.Fatal("CompileAssertion of a broken expression: no error")
	}
}
