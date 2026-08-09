package wal_test

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Lenecplusultra/distributed-kv-store/internal/storage"
	"github.com/Lenecplusultra/distributed-kv-store/internal/wal"
)

// These tests cover the gap that let a real bug ship: Append was called
// concurrently by one goroutine per client connection, but every existing
// test exercised it single-threaded. bufio.Writer is not safe for concurrent
// use — racing writers corrupted its offset, produced io.ErrShortWrite, and
// because bufio latches errors permanently, every subsequent write failed
// for the life of the process.

func tempWAL(t *testing.T) (*wal.WAL, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := wal.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w, path
}

// countLines returns how many lines the WAL file contains on disk.
func countLines(t *testing.T, path string) int {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	n := 0
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) != "" {
			n++
		}
	}
	return n
}

// TestConcurrentAppendsAllSucceed is the direct regression test. Run against
// the unsynchronized version it fails with "wal write: short write".
func TestConcurrentAppendsAllSucceed(t *testing.T) {
	w, path := tempWAL(t)

	const writers = 50
	const perWriter = 200

	var wg sync.WaitGroup
	errs := make(chan error, writers*perWriter)

	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				e := wal.Entry{
					Op:    "SET",
					Key:   fmt.Sprintf("key-%d-%d", g, i),
					Value: "value",
				}
				if err := w.Append(e); err != nil {
					errs <- fmt.Errorf("writer %d entry %d: %w", g, i, err)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent append failed: %v", err)
	}

	// Every append must have produced exactly one line. A torn or
	// interleaved write would show up as a wrong count.
	if got, want := countLines(t, path), writers*perWriter; got != want {
		t.Fatalf("wrote %d lines, want %d — entries were lost or interleaved", got, want)
	}
}

// TestConcurrentAppendsAreNotInterleaved checks content, not just count.
// Two goroutines writing simultaneously must not produce a line containing
// fragments of both.
func TestConcurrentAppendsAreNotInterleaved(t *testing.T) {
	w, path := tempWAL(t)

	const writers = 30
	const perWriter = 100

	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				w.Append(wal.Entry{
					Op:    "SET",
					Key:   fmt.Sprintf("k%d-%d", g, i),
					Value: fmt.Sprintf("v%d-%d", g, i),
				})
			}
		}(g)
	}
	wg.Wait()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	line := 0
	for scanner.Scan() {
		line++
		text := scanner.Text()
		if text == "" {
			continue
		}
		parts := strings.Fields(text)
		// Every well-formed SET line is exactly "SET key value".
		if len(parts) != 3 || parts[0] != "SET" {
			t.Fatalf("line %d is malformed, suggesting interleaved writes: %q", line, text)
		}
		// The key and value indices must match — they came from one call.
		if strings.TrimPrefix(parts[1], "k") != strings.TrimPrefix(parts[2], "v") {
			t.Fatalf("line %d mixes two entries: %q", line, text)
		}
	}
}

// TestConcurrentAppendsSurviveReplay ensures the file a race would have
// corrupted still recovers cleanly into a store.
func TestConcurrentAppendsSurviveReplay(t *testing.T) {
	w, path := tempWAL(t)

	const writers = 20
	const perWriter = 100

	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				w.Append(wal.Entry{
					Op:    "SET",
					Key:   fmt.Sprintf("key%d_%d", g, i),
					Value: "v",
				})
			}
		}(g)
	}
	wg.Wait()
	w.Close()

	// Reopen and recover, as a restart would.
	w2, err := wal.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()

	store := storage.New()
	if err := wal.Recover(w2, store); err != nil {
		t.Fatalf("recover: %v", err)
	}

	if got, want := store.Len(), writers*perWriter; got != want {
		t.Fatalf("recovered %d keys, want %d", got, want)
	}
	if v, ok := store.Get("key7_42"); !ok || v != "v" {
		t.Fatalf("expected key7_42=v, got %q ok=%v", v, ok)
	}
}

// TestMixedConcurrentOpsSucceed exercises SET and DEL together, which is
// what the server actually produces.
func TestMixedConcurrentOpsSucceed(t *testing.T) {
	w, _ := tempWAL(t)

	var wg sync.WaitGroup
	errs := make(chan error, 2000)

	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				var e wal.Entry
				if i%3 == 0 {
					e = wal.Entry{Op: "DEL", Key: fmt.Sprintf("k%d-%d", g, i)}
				} else {
					e = wal.Entry{Op: "SET", Key: fmt.Sprintf("k%d-%d", g, i), Value: "v"}
				}
				if err := w.Append(e); err != nil {
					errs <- err
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("mixed concurrent append failed: %v", err)
	}
}

// TestReplayDuringConcurrentAppends checks that seeking the shared file
// handle for replay does not corrupt in-flight appends.
func TestReplayDuringConcurrentAppends(t *testing.T) {
	w, _ := tempWAL(t)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
				w.Append(wal.Entry{Op: "SET", Key: fmt.Sprintf("k%d", i), Value: "v"})
				i++
			}
		}
	}()

	// Replay repeatedly while writes are in flight.
	for i := 0; i < 10; i++ {
		count := 0
		if err := w.Replay(func(wal.Entry) { count++ }); err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("replay during concurrent appends: %v", err)
		}
	}

	close(stop)
	wg.Wait()

	// Appends must still work after all that seeking.
	if err := w.Append(wal.Entry{Op: "SET", Key: "after", Value: "v"}); err != nil {
		t.Fatalf("append after concurrent replay: %v", err)
	}
}

// TestUnknownOpIsRejectedWithoutPoisoningTheWriter ensures a bad entry does
// not leave the WAL unable to accept good ones afterwards.
func TestUnknownOpIsRejectedWithoutPoisoningTheWriter(t *testing.T) {
	w, path := tempWAL(t)

	if err := w.Append(wal.Entry{Op: "NOPE", Key: "k"}); err == nil {
		t.Fatal("expected an error for an unknown op")
	}

	if err := w.Append(wal.Entry{Op: "SET", Key: "k", Value: "v"}); err != nil {
		t.Fatalf("valid append after a rejected one failed: %v", err)
	}

	if got := countLines(t, path); got != 1 {
		t.Fatalf("expected 1 line on disk, got %d", got)
	}
}
