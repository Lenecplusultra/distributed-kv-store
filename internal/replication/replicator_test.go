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

// recordingReplica is a fake replica that records every command it receives,
// in arrival order, and can be made to fail on demand.
//
// A real tcp.Server can't be used for the ordering tests: it applies writes
// to a store, so only the final value is observable and an out-of-order
// delivery that happens to end on the right value would pass. Recording the
// sequence is what actually proves ordering.
type recordingReplica struct {
	addr     string
	ln       net.Listener
	mu       chan struct{} // used as a 1-slot mutex
	received []string
	failing  bool
	delay    time.Duration
}

func newRecordingReplica(t *testing.T, port string) *recordingReplica {
	t.Helper()
	addr := "127.0.0.1:" + port
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen on %s: %v", addr, err)
	}

	r := &recordingReplica{addr: addr, ln: ln, mu: make(chan struct{}, 1)}
	r.mu <- struct{}{}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go r.handle(conn)
		}
	}()

	t.Cleanup(func() { ln.Close() })
	return r
}

func (r *recordingReplica) lock()   { <-r.mu }
func (r *recordingReplica) unlock() { r.mu <- struct{}{} }

func (r *recordingReplica) handle(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimSpace(line)

		r.lock()
		failing := r.failing
		delay := r.delay
		isCmd := line != "" && !strings.HasPrefix(line, "HELLO") && line != "PING"
		if isCmd && !failing {
			r.received = append(r.received, line)
		}
		r.unlock()

		if delay > 0 {
			time.Sleep(delay)
		}

		switch {
		case line == "PING":
			fmt.Fprint(conn, "+PONG\n")
		case strings.HasPrefix(line, "HELLO"):
			fmt.Fprint(conn, "+OK\n")
		case failing:
			fmt.Fprint(conn, "-ERR simulated failure\n")
		default:
			fmt.Fprint(conn, "+OK\n")
		}
	}
}

func (r *recordingReplica) setFailing(v bool) {
	r.lock()
	r.failing = v
	r.unlock()
}

func (r *recordingReplica) setDelay(d time.Duration) {
	r.lock()
	r.delay = d
	r.unlock()
}

func (r *recordingReplica) snapshot() []string {
	r.lock()
	defer r.unlock()
	out := make([]string, len(r.received))
	copy(out, r.received)
	return out
}

// waitFor polls until cond is true or the timeout expires.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// ── Ordering ──────────────────────────────────────────────────────────────

func TestDeliveryPreservesOrder(t *testing.T) {
	rep := newRecordingReplica(t, "17101")

	r := replication.New([]string{rep.addr})
	defer r.Close()

	const n = 50
	for i := 0; i < n; i++ {
		r.Replicate(fmt.Sprintf("SET k %d", i))
	}

	if !waitFor(t, 5*time.Second, func() bool { return len(rep.snapshot()) == n }) {
		t.Fatalf("only %d of %d writes delivered", len(rep.snapshot()), n)
	}

	got := rep.snapshot()
	for i, cmd := range got {
		want := fmt.Sprintf("SET k %d", i)
		if cmd != want {
			t.Fatalf("position %d: got %q, want %q — delivery out of order", i, cmd, want)
		}
	}
}

// TestRetryDoesNotAllowOvertaking is the regression test for the Phase 8a
// bug. A slow first write must not be passed by a fast second one.
func TestRetryDoesNotAllowOvertaking(t *testing.T) {
	rep := newRecordingReplica(t, "17102")

	r := replication.New([]string{rep.addr})
	defer r.Close()

	// Make the first delivery slow, then speed up.
	rep.setDelay(150 * time.Millisecond)
	r.Replicate("SET k first")

	time.Sleep(20 * time.Millisecond) // ensure the first is in flight
	rep.setDelay(0)
	r.Replicate("SET k second")

	if !waitFor(t, 5*time.Second, func() bool { return len(rep.snapshot()) == 2 }) {
		t.Fatalf("expected 2 writes, got %v", rep.snapshot())
	}

	got := rep.snapshot()
	if got[0] != "SET k first" || got[1] != "SET k second" {
		t.Fatalf("out of order: %v", got)
	}
}

// ── Dead replica handling ─────────────────────────────────────────────────

func TestReplicaDeclaredDeadStopsBlocking(t *testing.T) {
	// No listener at this address — every delivery fails.
	r := replication.New([]string{"127.0.0.1:17103"})
	defer r.Close()

	// Enough writes to exceed the retry budget several times over.
	for i := 0; i < 5; i++ {
		r.Replicate(fmt.Sprintf("SET k %d", i))
	}

	dead := waitFor(t, 10*time.Second, func() bool {
		st, ok := r.Health()["127.0.0.1:17103"]
		return ok && !st.Alive
	})
	if !dead {
		t.Fatal("replica should have been declared dead after repeated failures")
	}

	// Once dead, Replicate must not block even under sustained load.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 5000; i++ {
			r.Replicate(fmt.Sprintf("SET flood %d", i))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Replicate blocked on a dead replica — queue is not draining")
	}
}

func TestDeadReplicaRevives(t *testing.T) {
	rep := newRecordingReplica(t, "17104")
	rep.setFailing(true)

	r := replication.New([]string{rep.addr})
	defer r.Close()

	for i := 0; i < 5; i++ {
		r.Replicate(fmt.Sprintf("SET k %d", i))
	}

	dead := waitFor(t, 10*time.Second, func() bool {
		st := r.Health()[rep.addr]
		return !st.Alive
	})
	if !dead {
		t.Fatal("replica should have been declared dead")
	}

	// Recover. The probe ticker should notice within ~1s.
	rep.setFailing(false)

	alive := waitFor(t, 5*time.Second, func() bool {
		st := r.Health()[rep.addr]
		return st.Alive
	})
	if !alive {
		t.Fatal("replica should have revived after recovering")
	}

	// Writes flow again.
	r.Replicate("SET after revival")
	if !waitFor(t, 5*time.Second, func() bool {
		for _, c := range rep.snapshot() {
			if c == "SET after revival" {
				return true
			}
		}
		return false
	}) {
		t.Fatal("write after revival was not delivered")
	}
}

func TestHealthyReplicaStaysAlive(t *testing.T) {
	rep := newRecordingReplica(t, "17105")

	r := replication.New([]string{rep.addr})
	defer r.Close()

	for i := 0; i < 20; i++ {
		r.Replicate(fmt.Sprintf("SET k %d", i))
	}

	if !waitFor(t, 5*time.Second, func() bool { return len(rep.snapshot()) == 20 }) {
		t.Fatalf("expected 20 writes, got %d", len(rep.snapshot()))
	}
	if st := r.Health()[rep.addr]; !st.Alive {
		t.Fatal("healthy replica should not be marked dead")
	}
}

// ── Multiple replicas ─────────────────────────────────────────────────────

func TestOneDeadReplicaDoesNotStopAnother(t *testing.T) {
	good := newRecordingReplica(t, "17106")

	// Second address has no listener at all.
	r := replication.New([]string{good.addr, "127.0.0.1:17107"})
	defer r.Close()

	for i := 0; i < 10; i++ {
		r.Replicate(fmt.Sprintf("SET k %d", i))
	}

	if !waitFor(t, 10*time.Second, func() bool { return len(good.snapshot()) == 10 }) {
		t.Fatalf("healthy replica got %d of 10 writes — a dead peer is holding it up",
			len(good.snapshot()))
	}

	got := good.snapshot()
	for i, cmd := range got {
		if want := fmt.Sprintf("SET k %d", i); cmd != want {
			t.Fatalf("position %d: got %q, want %q", i, cmd, want)
		}
	}
}

// ── Lifecycle ─────────────────────────────────────────────────────────────

func TestCloseStopsWorkers(t *testing.T) {
	rep := newRecordingReplica(t, "17108")

	r := replication.New([]string{rep.addr})
	r.Replicate("SET k v")

	done := make(chan struct{})
	go func() {
		r.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return — worker goroutines are still running")
	}

	// Replicate after Close must not block or panic.
	r.Replicate("SET after close")
}

// ── Real server integration ───────────────────────────────────────────────

func TestReplicateToRealServer(t *testing.T) {
	store := storage.New()
	srv := tcp.New("127.0.0.1:17109", store, nil, nil)
	go srv.Start()
	time.Sleep(40 * time.Millisecond)
	t.Cleanup(srv.Shutdown)

	r := replication.New([]string{"127.0.0.1:17109"})
	defer r.Close()

	r.Replicate("SET city Atlanta")

	if !waitFor(t, 5*time.Second, func() bool {
		v, ok := store.Get("city")
		return ok && v == "Atlanta"
	}) {
		t.Fatal("write did not reach the replica store")
	}

	r.Replicate("DEL city")
	if !waitFor(t, 5*time.Second, func() bool {
		_, ok := store.Get("city")
		return !ok
	}) {
		t.Fatal("delete did not reach the replica store")
	}
}
