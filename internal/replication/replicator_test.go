package replication_test

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Lenecplusultra/distributed-kv-store/internal/replication"
	"github.com/Lenecplusultra/distributed-kv-store/internal/storage"
	"github.com/Lenecplusultra/distributed-kv-store/internal/tcp"
)

// startReplica launches a real TCP server with no WAL and no replicator —
// exactly what a replica node looks like in Phase 5.
func startReplica(t *testing.T, port string) *storage.Store {
	t.Helper()
	addr := "127.0.0.1:" + port
	store := storage.New()
	srv := tcp.New(addr, store, nil, nil)
	go srv.Start()
	time.Sleep(40 * time.Millisecond)
	t.Cleanup(func() { srv.Shutdown() })
	return store
}

// query sends one command to addr and returns the trimmed response.
func query(t *testing.T, addr, cmd string) string {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("connect to %s: %v", addr, err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "%s\n", cmd)
	resp, _ := bufio.NewReader(conn).ReadString('\n')
	return strings.TrimRight(resp, "\n")
}

func TestReplicateSetToLiveServer(t *testing.T) {
	startReplica(t, "17001")

	r := replication.New([]string{"127.0.0.1:17001"})
	r.Replicate("SET city Atlanta")

	// Replication is async — give it time to complete.
	time.Sleep(200 * time.Millisecond)

	if resp := query(t, "127.0.0.1:17001", "GET city"); resp != "+Atlanta" {
		t.Fatalf("expected +Atlanta on replica, got %q", resp)
	}
}

func TestReplicateDelToLiveServer(t *testing.T) {
	startReplica(t, "17002")

	r := replication.New([]string{"127.0.0.1:17002"})

	// First replicate a SET, then a DEL.
	r.Replicate("SET city Atlanta")
	time.Sleep(100 * time.Millisecond)

	r.Replicate("DEL city")
	time.Sleep(200 * time.Millisecond)

	resp := query(t, "127.0.0.1:17002", "GET city")
	if !strings.HasPrefix(resp, "-ERR") {
		t.Fatalf("expected -ERR after DEL replication, got %q", resp)
	}
}

func TestReplicateWithEXAT(t *testing.T) {
	// EXAT preserves the absolute expiry timestamp across replication.
	// We set a 1-second TTL and verify it expires on the replica too.
	startReplica(t, "17003")

	r := replication.New([]string{"127.0.0.1:17003"})

	expiresAt := time.Now().Add(time.Second)
	cmd := fmt.Sprintf("SET token abc EXAT %d", expiresAt.UnixNano())
	r.Replicate(cmd)
	time.Sleep(100 * time.Millisecond)

	// Key should be alive immediately after replication.
	if resp := query(t, "127.0.0.1:17003", "GET token"); resp != "+abc" {
		t.Fatalf("expected +abc immediately after replication, got %q", resp)
	}

	// Wait for expiry.
	time.Sleep(1100 * time.Millisecond)

	resp := query(t, "127.0.0.1:17003", "GET token")
	if !strings.HasPrefix(resp, "-ERR") {
		t.Fatalf("expected -ERR after TTL expiry on replica, got %q", resp)
	}
}

func TestReplicateToMultipleReplicas(t *testing.T) {
	startReplica(t, "17004")
	startReplica(t, "17005")

	r := replication.New([]string{"127.0.0.1:17004", "127.0.0.1:17005"})
	r.Replicate("SET name tex")
	time.Sleep(200 * time.Millisecond)

	for _, port := range []string{"17004", "17005"} {
		resp := query(t, "127.0.0.1:"+port, "GET name")
		if resp != "+tex" {
			t.Fatalf("replica %s: expected +tex, got %q", port, resp)
		}
	}
}

func TestReplicateToDeadServerDoesNotBlock(t *testing.T) {
	// Replicating to a non-existent address should not hang the test.
	// The retry logic should give up gracefully.
	r := replication.New([]string{"127.0.0.1:19999"})

	done := make(chan struct{})
	go func() {
		r.Replicate("SET key value")
		close(done)
	}()

	// Replicate returns immediately (async) — should not block.
	select {
	case <-done:
		// Good — returned immediately.
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Replicate blocked when it should have returned immediately")
	}
}

func TestNoReplicasIsNoop(t *testing.T) {
	r := replication.New([]string{})
	// Should not panic or block.
	r.Replicate("SET key value")
}
