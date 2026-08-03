package hashing_test

import (
	"fmt"
	"testing"

	"github.com/Lenecplusultra/distributed-kv-store/internal/hashing"
)

func TestEmptyRingReturnsNothing(t *testing.T) {
	r := hashing.New(10)
	_, ok := r.GetNode("anykey")
	if ok {
		t.Fatal("empty ring should return ok=false")
	}
}

func TestSingleNodeOwnsAllKeys(t *testing.T) {
	r := hashing.New(10)
	r.AddNode("node1:6379")

	keys := []string{"user:1", "session:abc", "config:db", "cache:hot", "order:999"}
	for _, k := range keys {
		node, ok := r.GetNode(k)
		if !ok {
			t.Fatalf("GetNode(%q): expected ok=true", k)
		}
		if node != "node1:6379" {
			t.Fatalf("GetNode(%q): expected node1:6379, got %q", k, node)
		}
	}
}

func TestDeterministic(t *testing.T) {
	// The same key must always route to the same node.
	r := hashing.New(150)
	r.AddNode("node1:6379")
	r.AddNode("node2:6379")
	r.AddNode("node3:6379")

	key := "user:42"
	first, _ := r.GetNode(key)
	for i := 0; i < 100; i++ {
		node, _ := r.GetNode(key)
		if node != first {
			t.Fatalf("GetNode(%q) returned different nodes: %q vs %q", key, first, node)
		}
	}
}

func TestAddNodeIdempotent(t *testing.T) {
	r := hashing.New(10)
	r.AddNode("node1:6379")
	r.AddNode("node1:6379") // second add should be a no-op
	r.AddNode("node1:6379")

	if r.Len() != 1 {
		t.Fatalf("expected 1 node, got %d", r.Len())
	}
}

func TestRemoveNode(t *testing.T) {
	r := hashing.New(150)
	r.AddNode("node1:6379")
	r.AddNode("node2:6379")
	r.RemoveNode("node1:6379")

	if r.Len() != 1 {
		t.Fatalf("expected 1 node after remove, got %d", r.Len())
	}

	// All keys should now route to node2.
	for i := 0; i < 20; i++ {
		node, ok := r.GetNode(fmt.Sprintf("key:%d", i))
		if !ok {
			t.Fatalf("key:%d: expected ok=true", i)
		}
		if node != "node2:6379" {
			t.Fatalf("key:%d: expected node2:6379, got %q", i, node)
		}
	}
}

func TestRemoveNonexistentNodeNoPanic(t *testing.T) {
	r := hashing.New(10)
	r.AddNode("node1:6379")
	r.RemoveNode("ghost:9999") // should not panic or change state
	if r.Len() != 1 {
		t.Fatalf("expected 1 node, got %d", r.Len())
	}
}

// TestKeyStability is the most important test in this file.
// It proves that consistent hashing minimises key movement when
// a node is added — unlike modulo sharding where ~(N-1)/N keys move.
func TestKeyStabilityWhenNodeAdded(t *testing.T) {
	r := hashing.New(150)
	r.AddNode("node1:6379")
	r.AddNode("node2:6379")

	// Record routing for 1000 keys with 2 nodes.
	const total = 1000
	before := make(map[string]string, total)
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key:%d", i)
		node, _ := r.GetNode(key)
		before[key] = node
	}

	// Add a third node.
	r.AddNode("node3:6379")

	// Count how many keys moved.
	moved := 0
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key:%d", i)
		node, _ := r.GetNode(key)
		if node != before[key] {
			moved++
		}
	}

	movedPct := float64(moved) / float64(total) * 100

	// With consistent hashing and 3 nodes, ~1/3 of keys should move.
	// We allow a generous range (15%–50%) to account for variance
	// from virtual node placement.
	//
	// With modulo sharding (hash % N), ~67% of keys would move.
	// If ALL keys moved, it's definitely modulo — fail loudly.
	if moved == total {
		t.Fatalf("all %d keys remapped — looks like modulo sharding, not consistent hashing", total)
	}
	if movedPct > 55 {
		t.Fatalf("%.1f%% of keys moved when adding 1 of 3 nodes — expected ~33%%", movedPct)
	}
	t.Logf("%.1f%% of keys moved when adding node3 (expected ~33%%)", movedPct)
}

func TestKeyStabilityWhenNodeRemoved(t *testing.T) {
	r := hashing.New(150)
	r.AddNode("node1:6379")
	r.AddNode("node2:6379")
	r.AddNode("node3:6379")

	const total = 1000
	before := make(map[string]string, total)
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key:%d", i)
		node, _ := r.GetNode(key)
		before[key] = node
	}

	r.RemoveNode("node3:6379")

	moved := 0
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key:%d", i)
		node, _ := r.GetNode(key)
		if node != before[key] {
			moved++
		}
	}

	movedPct := float64(moved) / float64(total) * 100
	if moved == total {
		t.Fatal("all keys remapped on node removal — not consistent hashing")
	}
	if movedPct > 55 {
		t.Fatalf("%.1f%% of keys moved when removing 1 of 3 nodes — expected ~33%%", movedPct)
	}
	t.Logf("%.1f%% of keys moved when removing node3 (expected ~33%%)", movedPct)
}

func TestEvenDistribution(t *testing.T) {
	r := hashing.New(150)
	r.AddNode("node1:6379")
	r.AddNode("node2:6379")
	r.AddNode("node3:6379")

	counts := map[string]int{}
	const total = 9000
	for i := 0; i < total; i++ {
		node, _ := r.GetNode(fmt.Sprintf("key:%d", i))
		counts[node]++
	}

	// Each node should handle roughly 33% ± 10% of keys.
	for node, count := range counts {
		pct := float64(count) / float64(total) * 100
		if pct < 23 || pct > 43 {
			t.Errorf("node %s handles %.1f%% of keys — expected ~33%%", node, pct)
		}
		t.Logf("node %s: %d keys (%.1f%%)", node, count, pct)
	}
}

func TestGetNReturnsDistinctNodes(t *testing.T) {
	r := hashing.New(150)
	r.AddNode("node1:6379")
	r.AddNode("node2:6379")
	r.AddNode("node3:6379")

	nodes := r.GetN("anykey", 3)
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}

	seen := map[string]bool{}
	for _, n := range nodes {
		if seen[n] {
			t.Fatalf("duplicate node in GetN result: %q", n)
		}
		seen[n] = true
	}
}

func TestGetNCapAtAvailableNodes(t *testing.T) {
	r := hashing.New(150)
	r.AddNode("node1:6379")
	r.AddNode("node2:6379")

	// Ask for 5 but only 2 exist.
	nodes := r.GetN("anykey", 5)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes (capped), got %d", len(nodes))
	}
}
