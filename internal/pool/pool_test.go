package pool_test

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Lenecplusultra/distributed-kv-store/internal/pool"
)

// echoServer is a minimal node: it answers PING with +PONG, echoes anything
// else back as +<cmd>, and counts how many connections it has accepted.
//
// The connection count is what makes pooling observable — a working pool
// sends N commands over 1 connection, an unpooled client opens N.
type echoServer struct {
	ln net.Listener

	mu       sync.Mutex
	accepted int
	conns    []net.Conn
}

func newEchoServer(t *testing.T) *echoServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0") // port 0 = let the OS pick
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	s := &echoServer{ln: ln}
	go s.acceptLoop()
	t.Cleanup(s.close)
	return s
}

func (s *echoServer) addr() string { return s.ln.Addr().String() }

func (s *echoServer) acceptLoop() {
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.accepted++
		s.conns = append(s.conns, c)
		s.mu.Unlock()
		go s.handle(c)
	}
}

func (s *echoServer) handle(c net.Conn) {
	defer c.Close()
	reader := bufio.NewReader(c)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimSpace(line)
		if line == "PING" {
			fmt.Fprint(c, "+PONG\n")
			continue
		}
		fmt.Fprintf(c, "+%s\n", line)
	}
}

func (s *echoServer) acceptedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accepted
}

// dropAll closes every connection the server has accepted, simulating a
// restart or an idle timeout on the server side.
func (s *echoServer) dropAll() {
	s.mu.Lock()
	conns := s.conns
	s.conns = nil
	s.mu.Unlock()

	for _, c := range conns {
		c.Close()
	}
}

func (s *echoServer) close() {
	s.ln.Close()
	s.dropAll()
}

// ── Basic behaviour ───────────────────────────────────────────────────────

func TestDoReturnsResponse(t *testing.T) {
	srv := newEchoServer(t)
	p := pool.New()
	defer p.Close()

	resp, err := p.Do(srv.addr(), "SET k v")
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if strings.TrimSpace(resp) != "+SET k v" {
		t.Fatalf("got %q", resp)
	}
}

func TestConnectionIsReused(t *testing.T) {
	srv := newEchoServer(t)
	p := pool.New()
	defer p.Close()

	for i := 0; i < 20; i++ {
		if _, err := p.Do(srv.addr(), fmt.Sprintf("SET k %d", i)); err != nil {
			t.Fatalf("command %d: %v", i, err)
		}
	}

	// The whole point: 20 sequential commands, 1 connection.
	if got := srv.acceptedCount(); got != 1 {
		t.Fatalf("server accepted %d connections for 20 sequential commands, want 1", got)
	}
	if p.IdleCount(srv.addr()) != 1 {
		t.Fatalf("expected 1 idle connection, got %d", p.IdleCount(srv.addr()))
	}
}

func TestResponsesMatchRequestsUnderSequentialUse(t *testing.T) {
	srv := newEchoServer(t)
	p := pool.New()
	defer p.Close()

	// A desynced reader would return the previous command's response.
	for i := 0; i < 30; i++ {
		cmd := fmt.Sprintf("GET key%d", i)
		resp, err := p.Do(srv.addr(), cmd)
		if err != nil {
			t.Fatalf("command %d: %v", i, err)
		}
		if want := "+" + cmd; strings.TrimSpace(resp) != want {
			t.Fatalf("response desync at %d: got %q, want %q", i, strings.TrimSpace(resp), want)
		}
	}
}

// ── Staleness and retry ───────────────────────────────────────────────────

// TestStaleConnectionIsRetried is the core test for the design decision:
// use optimistically, retry once on failure, rather than validating on
// checkout.
func TestStaleConnectionIsRetried(t *testing.T) {
	srv := newEchoServer(t)
	p := pool.New()
	defer p.Close()

	// Establish and pool a connection.
	if _, err := p.Do(srv.addr(), "SET k v"); err != nil {
		t.Fatalf("first command: %v", err)
	}
	if srv.acceptedCount() != 1 {
		t.Fatalf("expected 1 connection, got %d", srv.acceptedCount())
	}

	// Kill it server-side. The pool still holds it and has no idea.
	srv.dropAll()
	time.Sleep(50 * time.Millisecond)

	// This must transparently succeed on a retry.
	resp, err := p.Do(srv.addr(), "GET k")
	if err != nil {
		t.Fatalf("stale connection was not retried: %v", err)
	}
	if strings.TrimSpace(resp) != "+GET k" {
		t.Fatalf("got %q", resp)
	}
	if got := srv.acceptedCount(); got != 2 {
		t.Fatalf("expected a second connection after retry, got %d total", got)
	}
}

func TestDeadNodeIsNotRetriedForever(t *testing.T) {
	srv := newEchoServer(t)
	addr := srv.addr()

	p := pool.New()
	defer p.Close()

	if _, err := p.Do(addr, "SET k v"); err != nil {
		t.Fatalf("first command: %v", err)
	}

	// Take the node down entirely.
	srv.close()
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	_, err := p.Do(addr, "GET k")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error against a dead node")
	}
	// One retry, not a loop. Generous bound, but a retry storm would blow it.
	if elapsed > 8*time.Second {
		t.Fatalf("took %v — suggests more than a single retry", elapsed)
	}
}

func TestProtocolErrorIsNotRetried(t *testing.T) {
	// A server that always returns a protocol-level error.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	var mu sync.Mutex
	accepted := 0

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			accepted++
			mu.Unlock()

			go func(c net.Conn) {
				defer c.Close()
				reader := bufio.NewReader(c)
				for {
					if _, err := reader.ReadString('\n'); err != nil {
						return
					}
					fmt.Fprint(c, "-ERR key not found\n")
				}
			}(c)
		}
	}()

	p := pool.New()
	defer p.Close()

	for i := 0; i < 5; i++ {
		resp, err := p.Do(ln.Addr().String(), "GET ghost")
		if err != nil {
			t.Fatalf("protocol error should not surface as a transport error: %v", err)
		}
		if !strings.HasPrefix(resp, "-ERR") {
			t.Fatalf("got %q, want the error response passed through", resp)
		}
	}

	mu.Lock()
	got := accepted
	mu.Unlock()

	// An error response is an answer — the connection stays healthy and pooled.
	if got != 1 {
		t.Fatalf("server accepted %d connections; error responses should not trigger reconnects", got)
	}
}

// ── Concurrency ───────────────────────────────────────────────────────────

func TestConcurrentCallersDoNotShareAConnection(t *testing.T) {
	srv := newEchoServer(t)
	p := pool.New()
	defer p.Close()

	const workers = 20
	const perWorker = 25

	var wg sync.WaitGroup
	errs := make(chan error, workers*perWorker)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				cmd := fmt.Sprintf("GET w%d-i%d", w, i)
				resp, err := p.Do(srv.addr(), cmd)
				if err != nil {
					errs <- fmt.Errorf("worker %d: %w", w, err)
					return
				}
				// If two callers shared a socket, this is where the
				// mismatched reply would show up.
				if want := "+" + cmd; strings.TrimSpace(resp) != want {
					errs <- fmt.Errorf("response mismatch: got %q, want %q",
						strings.TrimSpace(resp), want)
					return
				}
			}
		}(w)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestIdlePoolIsCapped(t *testing.T) {
	srv := newEchoServer(t)
	p := pool.New()
	defer p.Close()

	const workers = 40
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Do(srv.addr(), "PING")
		}()
	}
	wg.Wait()

	if got := p.IdleCount(srv.addr()); got > pool.DefaultMaxIdlePerAddr {
		t.Fatalf("idle count %d exceeds cap %d", got, pool.DefaultMaxIdlePerAddr)
	}
}

// ── Lifecycle ─────────────────────────────────────────────────────────────

func TestCloseDiscardsIdleConnections(t *testing.T) {
	srv := newEchoServer(t)
	p := pool.New()

	if _, err := p.Do(srv.addr(), "PING"); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if p.IdleCount(srv.addr()) != 1 {
		t.Fatalf("expected 1 idle connection")
	}

	p.Close()

	if got := p.IdleCount(srv.addr()); got != 0 {
		t.Fatalf("expected 0 idle after Close, got %d", got)
	}
	if _, err := p.Do(srv.addr(), "PING"); err == nil {
		t.Fatal("Do on a closed pool should fail")
	}
}
