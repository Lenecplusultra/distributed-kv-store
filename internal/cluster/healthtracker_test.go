package cluster

import (
	"testing"
	"time"
)

// Note: testing internal (unexported) types directly from within the package.

func newTracker() *HealthTracker {
	return NewHealthTracker(3)
}

func TestRegisterIsAlive(t *testing.T) {
	h := newTracker()
	h.register("node1:6379")
	if !h.IsAlive("node1:6379") {
		t.Fatal("newly registered node should be alive")
	}
}

func TestIsAliveUnknownNode(t *testing.T) {
	h := newTracker()
	if h.IsAlive("ghost:9999") {
		t.Fatal("unknown node should not be alive")
	}
}

func TestBeatResetsConsecutiveMisses(t *testing.T) {
	h := newTracker()
	h.register("node1:6379")

	// Two misses — not yet dead.
	h.Miss("node1:6379")
	h.Miss("node1:6379")

	// Beat should reset miss count and node stays alive.
	revived := h.Beat("node1:6379")
	if revived {
		t.Fatal("node was alive, Beat should not report revival")
	}
	if !h.IsAlive("node1:6379") {
		t.Fatal("node should still be alive after Beat")
	}

	// Verify miss count was reset — needs 3 more misses to die.
	h.Miss("node1:6379")
	h.Miss("node1:6379")
	if !h.IsAlive("node1:6379") {
		t.Fatal("node should survive 2 more misses after beat reset")
	}
}

func TestMissThresholdDeclaresDead(t *testing.T) {
	h := newTracker()
	h.register("node1:6379")

	// Two misses — still alive, justDied should be false.
	if died := h.Miss("node1:6379"); died {
		t.Fatal("miss 1: should not die yet")
	}
	if died := h.Miss("node1:6379"); died {
		t.Fatal("miss 2: should not die yet")
	}
	if !h.IsAlive("node1:6379") {
		t.Fatal("node should survive 2 misses")
	}

	// Third miss — crosses threshold.
	justDied := h.Miss("node1:6379")
	if !justDied {
		t.Fatal("miss 3: expected justDied=true at threshold")
	}
	if h.IsAlive("node1:6379") {
		t.Fatal("node should be dead after 3 consecutive misses")
	}
}

func TestMissOnDeadNodeReturnsFalse(t *testing.T) {
	h := newTracker()
	h.register("node1:6379")

	// Kill it.
	h.Miss("node1:6379")
	h.Miss("node1:6379")
	h.Miss("node1:6379")

	// Further misses should not report justDied again.
	if died := h.Miss("node1:6379"); died {
		t.Fatal("additional miss on dead node should return false")
	}
}

func TestBeatRevivesDeadNode(t *testing.T) {
	h := newTracker()
	h.register("node1:6379")

	// Kill it.
	h.Miss("node1:6379")
	h.Miss("node1:6379")
	h.Miss("node1:6379")
	if h.IsAlive("node1:6379") {
		t.Fatal("node should be dead")
	}

	// Node comes back — Beat should report revival.
	revived := h.Beat("node1:6379")
	if !revived {
		t.Fatal("Beat on dead node should return revived=true")
	}
	if !h.IsAlive("node1:6379") {
		t.Fatal("node should be alive after Beat")
	}
}

func TestBeatOnUnknownNodeReturnsFalse(t *testing.T) {
	h := newTracker()
	revived := h.Beat("ghost:9999")
	if revived {
		t.Fatal("Beat on unknown node should return false")
	}
}

func TestDeregisterRemovesNode(t *testing.T) {
	h := newTracker()
	h.register("node1:6379")
	h.deregister("node1:6379")

	if h.IsAlive("node1:6379") {
		t.Fatal("deregistered node should not appear alive")
	}
	nodes := h.AllNodes()
	for _, n := range nodes {
		if n == "node1:6379" {
			t.Fatal("deregistered node should not appear in AllNodes")
		}
	}
}

func TestAllNodesIncludesDeadNodes(t *testing.T) {
	h := newTracker()
	h.register("node1:6379")
	h.register("node2:6379")

	// Kill node1.
	h.Miss("node1:6379")
	h.Miss("node1:6379")
	h.Miss("node1:6379")

	nodes := h.AllNodes()
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes (alive + dead), got %d", len(nodes))
	}
}

func TestStatusSnapshot(t *testing.T) {
	h := newTracker()
	h.register("node1:6379")
	h.Miss("node1:6379")
	h.Miss("node1:6379")

	status := h.Status()
	s, ok := status["node1:6379"]
	if !ok {
		t.Fatal("node1 should appear in Status")
	}
	if !s.Alive {
		t.Fatal("node1 should still be alive after 2 misses")
	}
	if s.ConsecutiveMisses != 2 {
		t.Fatalf("expected 2 consecutive misses, got %d", s.ConsecutiveMisses)
	}
	if s.LastSeen.IsZero() {
		t.Fatal("LastSeen should not be zero")
	}
}

func TestLastSeenUpdatedOnBeat(t *testing.T) {
	h := newTracker()
	h.register("node1:6379")

	before := time.Now()
	time.Sleep(10 * time.Millisecond)
	h.Beat("node1:6379")

	status := h.Status()
	if !status["node1:6379"].LastSeen.After(before) {
		t.Fatal("LastSeen should be updated after Beat")
	}
}
