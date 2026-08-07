package cluster_test

import (
	"context"
	"testing"
	"time"

	"github.com/Lenecplusultra/distributed-kv-store/internal/cluster"
	"github.com/Lenecplusultra/distributed-kv-store/internal/storage"
	"github.com/Lenecplusultra/distributed-kv-store/internal/tcp"
)

// ── Routing tests ─────────────────────────────────────────────────────────────

func TestEmptyClusterRouteReturnsFalse(t *testing.T) {
	c := cluster.New()
	_, ok := c.Route("anykey")
	if ok {
		t.Fatal("empty cluster should return ok=false")
	}
}

func TestAddNodeAndRoute(t *testing.T) {
	c := cluster.New()
	c.AddNode("node1:6379")
	addr, ok := c.Route("user:42")
	if !ok {
		t.Fatal("expected ok=true after adding node")
	}
	if addr != "node1:6379" {
		t.Fatalf("expected node1:6379, got %q", addr)
	}
}

func TestRemoveNodeRedistributes(t *testing.T) {
	c := cluster.New()
	c.AddNode("node1:6379")
	c.AddNode("node2:6379")
	c.RemoveNode("node1:6379")

	if c.Len() != 1 {
		t.Fatalf("expected 1 node, got %d", c.Len())
	}
	addr, ok := c.Route("anykey")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if addr != "node2:6379" {
		t.Fatalf("expected node2:6379, got %q", addr)
	}
}

func TestNewFromAddrs(t *testing.T) {
	c, err := cluster.NewFromAddrs([]string{"node1:6379", "node2:6379", "node3:6379"})
	if err != nil {
		t.Fatalf("NewFromAddrs: %v", err)
	}
	if c.Len() != 3 {
		t.Fatalf("expected 3 nodes, got %d", c.Len())
	}
}

func TestNewFromAddrsIgnoresBlanks(t *testing.T) {
	c, err := cluster.NewFromAddrs([]string{"node1:6379", "", "  ", "node2:6379"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.Len() != 2 {
		t.Fatalf("expected 2 nodes, got %d", c.Len())
	}
}

func TestRouteDeterministic(t *testing.T) {
	c := cluster.New()
	c.AddNode("node1:6379")
	c.AddNode("node2:6379")
	c.AddNode("node3:6379")

	first, _ := c.Route("user:123")
	for i := 0; i < 50; i++ {
		addr, _ := c.Route("user:123")
		if addr != first {
			t.Fatalf("Route not deterministic: %q then %q", first, addr)
		}
	}
}

func TestRouteN(t *testing.T) {
	c := cluster.New()
	c.AddNode("node1:6379")
	c.AddNode("node2:6379")
	c.AddNode("node3:6379")

	nodes := c.RouteN("user:42", 2)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}
	if nodes[0] == nodes[1] {
		t.Fatalf("RouteN returned duplicate: %q", nodes[0])
	}
}

func TestIsEmpty(t *testing.T) {
	c := cluster.New()
	if !c.IsEmpty() {
		t.Fatal("new cluster should be empty")
	}
	c.AddNode("node1:6379")
	if c.IsEmpty() {
		t.Fatal("cluster with a node should not be empty")
	}
}

// ── Failure detection tests ───────────────────────────────────────────────────

func TestFailureDetectorRemovesDeadNode(t *testing.T) {
	store := storage.New()
	srv := tcp.New("127.0.0.1:18001", store, nil, nil)
	go srv.Start()
	time.Sleep(40 * time.Millisecond)

	c, _ := cluster.NewFromAddrs([]string{"127.0.0.1:18001"})
	if c.Len() != 1 {
		t.Fatal("expected 1 node")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.StartFailureDetector(ctx, 100*time.Millisecond)

	srv.Shutdown()

	// 3 misses × 100ms + buffer = ~600ms
	time.Sleep(700 * time.Millisecond)

	if c.Len() != 0 {
		t.Fatalf("expected dead node removed, got %d nodes", c.Len())
	}
}

func TestFailureDetectorRevivesNode(t *testing.T) {
	store := storage.New()
	srv := tcp.New("127.0.0.1:18002", store, nil, nil)
	go srv.Start()
	time.Sleep(40 * time.Millisecond)

	c, _ := cluster.NewFromAddrs([]string{"127.0.0.1:18002"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.StartFailureDetector(ctx, 100*time.Millisecond)

	srv.Shutdown()
	time.Sleep(700 * time.Millisecond)
	if c.Len() != 0 {
		t.Fatal("expected node removed after failure")
	}

	// Restart on same address.
	store2 := storage.New()
	srv2 := tcp.New("127.0.0.1:18002", store2, nil, nil)
	go srv2.Start()
	defer srv2.Shutdown()

	time.Sleep(400 * time.Millisecond)
	if c.Len() != 1 {
		t.Fatalf("expected revived node in ring, got %d", c.Len())
	}
}

func TestSingleMissDoesNotKillNode(t *testing.T) {
	// No server at this address — probes will fail.
	c := cluster.New()
	c.AddNode("127.0.0.1:18003")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.StartFailureDetector(ctx, 100*time.Millisecond)

	// One probe cycle — 1 miss, still alive (threshold is 3).
	time.Sleep(150 * time.Millisecond)

	status := c.Health()
	s, ok := status["127.0.0.1:18003"]
	if !ok {
		t.Fatal("node should still be tracked")
	}
	if !s.Alive {
		t.Fatal("node should still be alive after 1 miss")
	}
}
