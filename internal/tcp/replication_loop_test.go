package tcp_test

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Lenecplusultra/distributed-kv-store/internal/replication"
	"github.com/Lenecplusultra/distributed-kv-store/internal/storage"
	"github.com/Lenecplusultra/distributed-kv-store/internal/tcp"
)

// A replica receives writes as ordinary SET commands. Before this fix,
// dispatch re-replicated every SET it accepted whenever a replicator was
// configured, so any cycle in the replication graph became an infinite
// loop: A forwards to B, B forwards back to A, and the cluster saturates
// itself within seconds.
//
// That restricted every deployment to a star topology with exactly one node
// configured to replicate. These tests cover the rule that lifts the
// restriction: a write arriving from a replica connection is persisted and
// applied locally, but never forwarded.

// countingSink is a fake node that records every command it receives.
type countingSink struct {
	ln net.Listener

	mu       sync.Mutex
	received []string
}

func newCountingSink(t *testing.T) *countingSink {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &countingSink{ln: ln}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go s.handle(conn)
		}
	}()

	t.Cleanup(func() { ln.Close() })
	return s
}

func (s *countingSink) addr() string { return s.ln.Addr().String() }

func (s *countingSink) handle(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimSpace(line)

		if strings.HasPrefix(line, "HELLO") {
			fmt.Fprint(conn, "+OK\n")
			continue
		}
		if line == "PING" {
			fmt.Fprint(conn, "+PONG\n")
			continue
		}

		s.mu.Lock()
		s.received = append(s.received, line)
		s.mu.Unlock()
		fmt.Fprint(conn, "+OK\n")
	}
}

func (s *countingSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.received)
}

func (s *countingSink) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.received))
	copy(out, s.received)
	return out
}

// replicatingServer starts a server that forwards its writes to sink.
func replicatingServer(t *testing.T, port string, sink *countingSink) (addr string, store *storage.Store) {
	t.Helper()

	addr = "127.0.0.1:" + port
	store = storage.New()
	r := replication.New([]string{sink.addr()})
	srv := tcp.New(addr, store, nil, r)

	go srv.Start()
	time.Sleep(50 * time.Millisecond)

	t.Cleanup(func() {
		srv.Shutdown()
		r.Close()
	})
	return addr, store
}

// send issues commands on one connection and returns the responses.
func send(t *testing.T, addr string, cmds ...string) []string {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	out := make([]string, 0, len(cmds))
	for _, cmd := range cmds {
		fmt.Fprintf(conn, "%s\n", cmd)
		resp, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read after %q: %v", cmd, err)
		}
		out = append(out, strings.TrimSpace(resp))
	}
	return out
}

func TestClientWriteIsReplicated(t *testing.T) {
	sink := newCountingSink(t)
	addr, _ := replicatingServer(t, "17301", sink)

	if resp := send(t, addr, "SET k v")[0]; resp != "+OK" {
		t.Fatalf("SET: %s", resp)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && sink.count() == 0 {
		time.Sleep(10 * time.Millisecond)
	}

	if got := sink.snapshot(); len(got) != 1 || got[0] != "SET k v" {
		t.Fatalf("expected the write to be forwarded once, got %v", got)
	}
}

// TestReplicaWriteIsNotReReplicated is the regression test. Against the
// previous behaviour the sink receives the write and the loop begins.
func TestReplicaWriteIsNotReReplicated(t *testing.T) {
	sink := newCountingSink(t)
	addr, store := replicatingServer(t, "17302", sink)

	// Identify as replication traffic, then write — exactly what an upstream
	// primary does.
	resps := send(t, addr, "HELLO replica:primary", "SET k v")
	if resps[0] != "+OK" {
		t.Fatalf("HELLO: %s", resps[0])
	}
	if resps[1] != "+OK" {
		t.Fatalf("SET: %s", resps[1])
	}

	// The write must be applied locally — a replica keeps its own copy.
	if v, ok := store.Get("k"); !ok || v != "v" {
		t.Fatalf("replica did not apply the write locally: %q ok=%v", v, ok)
	}

	// But it must not be forwarded onward.
	time.Sleep(500 * time.Millisecond)
	if got := sink.count(); got != 0 {
		t.Fatalf("replica re-replicated %d writes; any cycle in the topology "+
			"would loop forever: %v", got, sink.snapshot())
	}
}

func TestReplicaDeleteIsNotReReplicated(t *testing.T) {
	sink := newCountingSink(t)
	addr, store := replicatingServer(t, "17303", sink)

	send(t, addr, "HELLO replica:primary", "SET k v", "DEL k")

	if _, ok := store.Get("k"); ok {
		t.Fatal("replica did not apply the delete locally")
	}

	time.Sleep(500 * time.Millisecond)
	if got := sink.count(); got != 0 {
		t.Fatalf("replica re-replicated %d commands: %v", got, sink.snapshot())
	}
}

// TestIdentityIsPerConnection checks that the exemption follows the
// connection rather than leaking across connections.
func TestIdentityIsPerConnection(t *testing.T) {
	sink := newCountingSink(t)
	addr, _ := replicatingServer(t, "17304", sink)

	// Connection A identifies as a replica; its write is not forwarded.
	send(t, addr, "HELLO replica:primary", "SET a 1")

	time.Sleep(300 * time.Millisecond)
	if got := sink.count(); got != 0 {
		t.Fatalf("replica write was forwarded: %v", sink.snapshot())
	}

	// Connection B is an ordinary client; its write must be forwarded.
	send(t, addr, "SET b 2")

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && sink.count() == 0 {
		time.Sleep(10 * time.Millisecond)
	}

	got := sink.snapshot()
	if len(got) != 1 || got[0] != "SET b 2" {
		t.Fatalf("expected only the client write forwarded, got %v", got)
	}
}

// TestTwoNodesPointingAtEachOtherDoNotLoop is the end-to-end proof that a
// cycle is now safe. Before the fix this saturates both nodes immediately.
func TestTwoNodesPointingAtEachOtherDoNotLoop(t *testing.T) {
	addrA := "127.0.0.1:17305"
	addrB := "127.0.0.1:17306"

	storeA := storage.New()
	storeB := storage.New()

	repA := replication.New([]string{addrB})
	repB := replication.New([]string{addrA})

	srvA := tcp.New(addrA, storeA, nil, repA)
	srvB := tcp.New(addrB, storeB, nil, repB)

	go srvA.Start()
	go srvB.Start()
	time.Sleep(60 * time.Millisecond)

	t.Cleanup(func() {
		srvA.Shutdown()
		srvB.Shutdown()
		repA.Close()
		repB.Close()
	})

	// One client write into A.
	if resp := send(t, addrA, "SET shared value")[0]; resp != "+OK" {
		t.Fatalf("SET: %s", resp)
	}

	// It should reach B exactly once and stop there.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := storeB.Get("shared"); ok && v == "value" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if v, ok := storeB.Get("shared"); !ok || v != "value" {
		t.Fatalf("write did not reach B: %q ok=%v", v, ok)
	}

	// Let any loop run for a while. If one exists, both nodes will be busy
	// forwarding the same key back and forth; the values stay identical so
	// the only observable signal is that the test hangs or the CPU pegs.
	// A bounded sleep plus a subsequent successful write is enough to show
	// the servers are still responsive rather than saturated.
	time.Sleep(1 * time.Second)

	if resp := send(t, addrA, "PING")[0]; resp != "+PONG" {
		t.Fatalf("node A unresponsive after replication settled: %s", resp)
	}
	if resp := send(t, addrB, "PING")[0]; resp != "+PONG" {
		t.Fatalf("node B unresponsive after replication settled: %s", resp)
	}
}
