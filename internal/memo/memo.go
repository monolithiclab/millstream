// Package memo is a bounded, concurrency-safe cache of computed values: what a process computes again and again
// from the same input, such as a compiled expression or schema, it computes once.
package memo

import "sync"

// Cache holds up to limit values by key. A full cache starts over: the keys are a revision's sources, which a
// long-lived process changes rarely, so a simpler eviction policy would gain nothing.
type Cache[K comparable, V any] struct {
	mu    sync.Mutex
	m     map[K]V
	limit int
}

// New returns a cache of up to limit values.
func New[K comparable, V any](limit int) *Cache[K, V] {
	return &Cache[K, V]{m: map[K]V{}, limit: limit}
}

// Get returns the value of k, computing it with build, unlocked, when the cache does not hold it. Two callers
// missing the same key at once may both build it: the values are equivalent, and the last one is kept.
func (c *Cache[K, V]) Get(k K, build func() V) V {
	c.mu.Lock()
	v, ok := c.m[k]
	c.mu.Unlock()
	if ok {
		return v
	}
	v = build()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.m) >= c.limit {
		clear(c.m)
	}
	c.m[k] = v
	return v
}

// Len returns how many values the cache holds.
func (c *Cache[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.m)
}
