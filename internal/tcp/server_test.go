package tcp_test

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Lenecplusultra/distributed-kv-store/internal/storage"
	"github.com/Lenecplusultra/distributed-kv-store/internal/tcp"
)

// testServer starts a server on a fixed test port and returns a
// send helper (sends one command, returns trimmed response) and cleanup.
func testServer(t *testing.T, port string) (send func(string) string, cleanup func()) {
	t.Helper()
	addr := "127.0.0.1:" + port
	store := storage.New()
	srv := tcp.New(addr, store, nil)

	go srv.Start()
	time.Sleep(40 * time.Millisecond) // let listener bind

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("could not connect to test server: %v", err)
	}
	reader := bufio.NewReader(conn)

	send = func(cmd string) string {
		fmt.Fprintf(conn, "%s\n", cmd)
		resp, _ := reader.ReadString('\n')
		return strings.TrimRight(resp, "\n")
	}
	cleanup = func() {
		conn.Close()
		srv.Shutdown()
	}
	return
}

func TestPing(t *testing.T) {
	send, cleanup := testServer(t, "16400")
	defer cleanup()
	if got := send("PING"); got != "+PONG" {
		t.Fatalf("PING: expected '+PONG', got %q", got)
	}
}

func TestSetGet(t *testing.T) {
	send, cleanup := testServer(t, "16401")
	defer cleanup()

	if r := send("SET city Atlanta"); r != "+OK" {
		t.Fatalf("SET: expected '+OK', got %q", r)
	}
	if r := send("GET city"); r != "+Atlanta" {
		t.Fatalf("GET: expected '+Atlanta', got %q", r)
	}
}

func TestGetMissing(t *testing.T) {
	send, cleanup := testServer(t, "16402")
	defer cleanup()

	r := send("GET ghost")
	if !strings.HasPrefix(r, "-ERR") {
		t.Fatalf("GET missing: expected '-ERR...', got %q", r)
	}
}

func TestDel(t *testing.T) {
	send, cleanup := testServer(t, "16403")
	defer cleanup()

	send("SET x 1")
	if r := send("DEL x"); r != "+OK" {
		t.Fatalf("DEL: expected '+OK', got %q", r)
	}
	if r := send("GET x"); !strings.HasPrefix(r, "-ERR") {
		t.Fatalf("GET after DEL: expected '-ERR...', got %q", r)
	}
}

func TestSetWithTTL(t *testing.T) {
	send, cleanup := testServer(t, "16404")
	defer cleanup()

	send("SET token abc EX 1")
	if r := send("GET token"); r != "+abc" {
		t.Fatalf("GET before expiry: expected '+abc', got %q", r)
	}
	time.Sleep(1100 * time.Millisecond)
	if r := send("GET token"); !strings.HasPrefix(r, "-ERR") {
		t.Fatalf("GET after expiry: expected '-ERR...', got %q", r)
	}
}

func TestUnknownCommand(t *testing.T) {
	send, cleanup := testServer(t, "16405")
	defer cleanup()

	r := send("FLUSHALL")
	if !strings.HasPrefix(r, "-ERR") {
		t.Fatalf("unknown command: expected '-ERR...', got %q", r)
	}
}
