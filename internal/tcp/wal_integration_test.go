package tcp_test

import (
	"bufio"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Lenecplusultra/distributed-kv-store/internal/storage"
	"github.com/Lenecplusultra/distributed-kv-store/internal/tcp"
	"github.com/Lenecplusultra/distributed-kv-store/internal/wal"
)

// Every other TCP test constructs the server with a nil WAL. That left the
// one path where concurrency meets the WAL — many connection goroutines
// calling Append at once — with no coverage at all, which is how an
// unsynchronized bufio.Writer survived to production and turned the node
// permanently write-rejecting under load.
//
// These tests drive a real WAL through the real server with concurrent
// clients.

// walServer starts a server backed by a real WAL in a temp directory.
func walServer(t *testing.T, port string) (addr, walPath string, shutdown func()) {
	t.Helper()

	walPath = filepath.Join(t.TempDir(), "wal.log")
	w, err := wal.Open(walPath)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	addr = "127.0.0.1:" + port
	store := storage.New()
	srv := tcp.New(addr, store, w, nil)

	go srv.Start()
	time.Sleep(50 * time.Millisecond)

	return addr, walPath, func() {
		srv.Shutdown()
		w.Close()
	}
}

// sendCommands opens one connection and issues cmds sequentially,
// returning every response.
func sendCommands(t *testing.T, addr string, cmds []string) []string {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	out := make([]string, 0, len(cmds))

	for _, cmd := range cmds {
		if _, err := fmt.Fprintf(conn, "%s\n", cmd); err != nil {
			t.Fatalf("write %q: %v", cmd, err)
		}
		resp, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read after %q: %v", cmd, err)
		}
		out = append(out, strings.TrimSpace(resp))
	}
	return out
}

// TestConcurrentWritesThroughRealWAL is the end-to-end regression test.
// Against the unsynchronized WAL this fails with
// "-ERR internal error: persistence failed".
func TestConcurrentWritesThroughRealWAL(t *testing.T) {
	addr, _, shutdown := walServer(t, "17201")
	defer shutdown()

	const clients = 40
	const perClient = 50

	var wg sync.WaitGroup
	failures := make(chan string, clients*perClient)

	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()

			cmds := make([]string, 0, perClient)
			for i := 0; i < perClient; i++ {
				cmds = append(cmds, fmt.Sprintf("SET c%d-k%d value%d", c, i, i))
			}

			for i, resp := range sendCommands(t, addr, cmds) {
				if resp != "+OK" {
					failures <- fmt.Sprintf("client %d write %d: %s", c, i, resp)
					return
				}
			}
		}(c)
	}

	wg.Wait()
	close(failures)

	var msgs []string
	for f := range failures {
		msgs = append(msgs, f)
	}
	if len(msgs) > 0 {
		t.Fatalf("%d concurrent writes failed, first few:\n%s",
			len(msgs), strings.Join(msgs[:min(3, len(msgs))], "\n"))
	}
}

// TestWritesRemainDurableAfterConcurrentLoad confirms the log is not merely
// error-free but actually contains the data, recoverable into a fresh store.
func TestWritesRemainDurableAfterConcurrentLoad(t *testing.T) {
	addr, walPath, shutdown := walServer(t, "17202")

	const clients = 20
	const perClient = 25

	var wg sync.WaitGroup
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			cmds := make([]string, 0, perClient)
			for i := 0; i < perClient; i++ {
				cmds = append(cmds, fmt.Sprintf("SET dur%d_%d v%d", c, i, i))
			}
			sendCommands(t, addr, cmds)
		}(c)
	}
	wg.Wait()

	shutdown() // flushes and closes the WAL

	// Recover into a fresh store, as a restart would.
	w2, err := wal.Open(walPath)
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer w2.Close()

	recovered := storage.New()
	if err := wal.Recover(w2, recovered); err != nil {
		t.Fatalf("recover: %v", err)
	}

	if got, want := recovered.Len(), clients*perClient; got != want {
		t.Fatalf("recovered %d keys, want %d — writes were acknowledged but lost", got, want)
	}
	if v, ok := recovered.Get("dur7_11"); !ok || v != "v11" {
		t.Fatalf("expected dur7_11=v11, got %q ok=%v", v, ok)
	}
}

// TestConcurrentMixedOpsThroughRealWAL exercises SET, GET, and DEL together
// against a real WAL, which is the actual production command mix.
func TestConcurrentMixedOpsThroughRealWAL(t *testing.T) {
	addr, _, shutdown := walServer(t, "17203")
	defer shutdown()

	var wg sync.WaitGroup
	bad := make(chan string, 4000)

	for c := 0; c < 25; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()

			for i := 0; i < 40; i++ {
				key := fmt.Sprintf("m%d-%d", c, i)
				resps := sendCommands(t, addr, []string{
					"SET " + key + " v",
					"GET " + key,
					"DEL " + key,
				})
				if resps[0] != "+OK" {
					bad <- "SET: " + resps[0]
					return
				}
				if resps[1] != "+v" {
					bad <- "GET: " + resps[1]
					return
				}
				if resps[2] != "+OK" {
					bad <- "DEL: " + resps[2]
					return
				}
			}
		}(c)
	}

	wg.Wait()
	close(bad)

	for msg := range bad {
		t.Fatalf("mixed concurrent op failed: %s", msg)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
