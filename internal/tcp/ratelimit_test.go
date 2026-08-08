package tcp_test

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Lenecplusultra/distributed-kv-store/internal/ratelimiter"
	"github.com/Lenecplusultra/distributed-kv-store/internal/storage"
	"github.com/Lenecplusultra/distributed-kv-store/internal/tcp"
)

// limitedServer starts a server with the given limiter (nil for unlimited)
// and returns a dial helper that opens an independent connection.
//
// Connections are separate here because rate-limit identity is
// per-connection state — a shared connection would make the HELLO tests
// meaningless.
func limitedServer(t *testing.T, port string, l *ratelimiter.Limiter) (dial func() (send func(string) string, closeConn func()), cleanup func()) {
	t.Helper()
	addr := "127.0.0.1:" + port
	store := storage.New()
	srv := tcp.New(addr, store, nil, nil, tcp.WithLimiter(l))

	go srv.Start()
	time.Sleep(40 * time.Millisecond) // let listener bind

	dial = func() (func(string) string, func()) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("could not connect to test server: %v", err)
		}
		reader := bufio.NewReader(conn)
		send := func(cmd string) string {
			fmt.Fprintf(conn, "%s\n", cmd)
			resp, _ := reader.ReadString('\n')
			return strings.TrimRight(resp, "\n")
		}
		return send, func() { conn.Close() }
	}

	return dial, func() { srv.Shutdown() }
}

func TestNilLimiterAdmitsUnboundedTraffic(t *testing.T) {
	dial, cleanup := limitedServer(t, "16410", nil)
	defer cleanup()

	send, closeConn := dial()
	defer closeConn()

	for i := 0; i < 50; i++ {
		if got := send("PING"); got != "+PONG" {
			t.Fatalf("request %d: got %q, want +PONG with limiting disabled", i+1, got)
		}
	}
}

func TestRateLimitRejectsAfterBurst(t *testing.T) {
	// 1 token/sec, burst 3. The refill over the test's few milliseconds is
	// far below one token, so exactly 3 requests should be admitted.
	dial, cleanup := limitedServer(t, "16411", ratelimiter.New(1, 3))
	defer cleanup()

	send, closeConn := dial()
	defer closeConn()

	for i := 0; i < 3; i++ {
		if got := send("PING"); got != "+PONG" {
			t.Fatalf("burst request %d: got %q, want +PONG", i+1, got)
		}
	}

	got := send("PING")
	if !strings.Contains(got, "rate limit exceeded") {
		t.Fatalf("request past burst: got %q, want rate limit error", got)
	}
}

func TestHelloMovesConnectionToOwnBucket(t *testing.T) {
	dial, cleanup := limitedServer(t, "16412", ratelimiter.New(1, 2))
	defer cleanup()

	// First connection exhausts the shared IP-keyed bucket.
	sendA, closeA := dial()
	defer closeA()
	for i := 0; i < 2; i++ {
		if got := sendA("PING"); got != "+PONG" {
			t.Fatalf("conn A request %d: got %q", i+1, got)
		}
	}
	if got := sendA("PING"); !strings.Contains(got, "rate limit exceeded") {
		t.Fatalf("conn A should be throttled, got %q", got)
	}

	// Second connection shares the same IP, so it inherits the exhausted
	// bucket until it identifies itself.
	sendB, closeB := dial()
	defer closeB()
	if got := sendB("PING"); !strings.Contains(got, "rate limit exceeded") {
		t.Fatalf("conn B should share A's IP bucket, got %q", got)
	}

	// After HELLO it has its own identity and a fresh bucket.
	if got := sendB("HELLO alice"); got != "+OK" {
		t.Fatalf("HELLO: got %q, want +OK", got)
	}
	if got := sendB("PING"); got != "+PONG" {
		t.Fatalf("conn B after HELLO: got %q, want +PONG", got)
	}
}

func TestHelloIsNotItselfRateLimited(t *testing.T) {
	dial, cleanup := limitedServer(t, "16413", ratelimiter.New(1, 1))
	defer cleanup()

	send, closeConn := dial()
	defer closeConn()

	// Burn the single token.
	if got := send("PING"); got != "+PONG" {
		t.Fatalf("first PING: got %q", got)
	}
	if got := send("PING"); !strings.Contains(got, "rate limit exceeded") {
		t.Fatalf("second PING should be throttled, got %q", got)
	}

	// HELLO must still work from an exhausted identity, otherwise a client
	// behind a busy NAT could never escape a shared bucket.
	if got := send("HELLO bob"); got != "+OK" {
		t.Fatalf("HELLO from throttled identity: got %q, want +OK", got)
	}
}

func TestHelloArgumentValidation(t *testing.T) {
	dial, cleanup := limitedServer(t, "16414", nil)
	defer cleanup()

	send, closeConn := dial()
	defer closeConn()

	if got := send("HELLO"); !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("HELLO with no args: got %q, want error", got)
	}
	if got := send("HELLO a b"); !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("HELLO with two args: got %q, want error", got)
	}
}

func TestReplicaIdentityBypassesRateLimit(t *testing.T) {
	// Burst of 1: any non-exempt identity would be throttled immediately.
	dial, cleanup := limitedServer(t, "16415", ratelimiter.New(1, 1))
	defer cleanup()

	send, closeConn := dial()
	defer closeConn()

	if got := send("HELLO replica:primary"); got != "+OK" {
		t.Fatalf("replica HELLO: got %q, want +OK", got)
	}

	// Replication traffic must not be throttled — a dropped replication
	// write is silent divergence, not a retryable client error.
	for i := 0; i < 20; i++ {
		if got := send(fmt.Sprintf("SET k%d v%d", i, i)); got != "+OK" {
			t.Fatalf("replica write %d: got %q, want +OK", i+1, got)
		}
	}
}

func TestSeparateIdentitiesDoNotShareBudget(t *testing.T) {
	dial, cleanup := limitedServer(t, "16416", ratelimiter.New(1, 2))
	defer cleanup()

	sendA, closeA := dial()
	defer closeA()
	sendB, closeB := dial()
	defer closeB()

	sendA("HELLO alice")
	sendB("HELLO bob")

	for i := 0; i < 2; i++ {
		if got := sendA("PING"); got != "+PONG" {
			t.Fatalf("alice request %d: got %q", i+1, got)
		}
	}
	if got := sendA("PING"); !strings.Contains(got, "rate limit exceeded") {
		t.Fatalf("alice should be throttled, got %q", got)
	}

	// Bob is untouched by alice's usage.
	if got := sendB("PING"); got != "+PONG" {
		t.Fatalf("bob throttled by alice's usage: got %q", got)
	}
}
