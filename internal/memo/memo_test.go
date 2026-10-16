package memo

import "testing"

// A value is built once per key, and a full cache starts over.
func TestCache(t *testing.T) {
	t.Parallel()
	c := New[string, int](2)
	builds := 0
	build := func(v int) func() int { return func() int { builds++; return v } }
	if c.Get("a", build(1)) != 1 || c.Get("a", build(9)) != 1 || builds != 1 {
		t.Fatalf("a built %d times", builds)
	}
	c.Get("b", build(2))
	if n := c.Len(); n != 2 {
		t.Fatalf("%d values, want 2", n)
	}
	if c.Get("c", build(3)) != 3 || c.Len() != 1 {
		t.Fatalf("a full cache holds %d values after a miss, want 1", c.Len())
	}
	if c.Get("a", build(4)) != 4 {
		t.Fatal("a survived the reset")
	}
}
