package lru_test

import (
	"testing"

	"github.com/Lenecplusultra/distributed-kv-store/internal/lru"
)

func TestTouchNewKey(t *testing.T) {
	c := lru.New(3)
	evicted, ok := c.Touch("a")
	if ok {
		t.Fatalf("expected no eviction, got %q", evicted)
	}
	if c.Len() != 1 {
		t.Fatalf("expected len 1, got %d", c.Len())
	}
}

func TestNoEvictionUnderCapacity(t *testing.T) {
	c := lru.New(3)
	c.Touch("a")
	c.Touch("b")
	c.Touch("c")
	if c.Len() != 3 {
		t.Fatalf("expected len 3, got %d", c.Len())
	}
}

func TestEvictsLRUKeyWhenFull(t *testing.T) {
	c := lru.New(3)
	c.Touch("a") // oldest
	c.Touch("b")
	c.Touch("c")

	evicted, ok := c.Touch("d") // should evict "a"
	if !ok {
		t.Fatal("expected eviction")
	}
	if evicted != "a" {
		t.Fatalf("expected 'a' evicted, got %q", evicted)
	}
	if c.Len() != 3 {
		t.Fatalf("expected len 3 after eviction, got %d", c.Len())
	}
}

func TestTouchExistingKeyUpdatesOrder(t *testing.T) {
	c := lru.New(3)
	c.Touch("a") // oldest
	c.Touch("b")
	c.Touch("c")
	c.Touch("a") // "a" is now most recent — "b" becomes oldest

	evicted, ok := c.Touch("d") // should evict "b", not "a"
	if !ok {
		t.Fatal("expected eviction")
	}
	if evicted != "b" {
		t.Fatalf("expected 'b' evicted after touching 'a', got %q", evicted)
	}
}

func TestTouchExistingKeyNoEviction(t *testing.T) {
	c := lru.New(3)
	c.Touch("a")
	c.Touch("b")
	c.Touch("c")

	evicted, ok := c.Touch("a") // existing key — never triggers eviction
	if ok {
		t.Fatalf("touching existing key should not evict, got %q", evicted)
	}
	if c.Len() != 3 {
		t.Fatalf("expected len 3, got %d", c.Len())
	}
}

func TestRemove(t *testing.T) {
	// capacity=3: add a(oldest), b, remove a → 1 key remains
	// add c → 2 keys, add d → 3 keys (full, no eviction yet)
	// add e → 4 keys would exceed capacity → evicts "b" (now oldest)
	c := lru.New(3)
	c.Touch("a")
	c.Touch("b")
	c.Remove("a") // explicitly remove "a" — "b" is now oldest

	c.Touch("c")
	c.Touch("d") // full at 3: b, c, d

	evicted, ok := c.Touch("e") // should evict "b"
	if !ok {
		t.Fatal("expected eviction")
	}
	if evicted != "b" {
		t.Fatalf("expected 'b' evicted, got %q", evicted)
	}
}

func TestRemoveNonexistentKey(t *testing.T) {
	c := lru.New(3)
	c.Remove("ghost") // should not panic
}

func TestCapacityOne(t *testing.T) {
	c := lru.New(1)
	c.Touch("a")

	evicted, ok := c.Touch("b")
	if !ok || evicted != "a" {
		t.Fatalf("expected 'a' evicted, got %q ok=%v", evicted, ok)
	}
	if c.Len() != 1 {
		t.Fatalf("expected len 1, got %d", c.Len())
	}
}

func TestSequentialEvictions(t *testing.T) {
	c := lru.New(2)
	c.Touch("a")
	c.Touch("b")

	e1, _ := c.Touch("c") // evicts "a"
	e2, _ := c.Touch("d") // evicts "b"
	e3, _ := c.Touch("e") // evicts "c"

	if e1 != "a" {
		t.Fatalf("expected 'a', got %q", e1)
	}
	if e2 != "b" {
		t.Fatalf("expected 'b', got %q", e2)
	}
	if e3 != "c" {
		t.Fatalf("expected 'c', got %q", e3)
	}
}
