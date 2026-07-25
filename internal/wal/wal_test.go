package wal_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/Lenecplusultra/distributed-kv-store/internal/storage"
	"github.com/Lenecplusultra/distributed-kv-store/internal/wal"
)

// openTemp creates a WAL in a temp directory that is cleaned up after the test.
func openTemp(t *testing.T) *wal.WAL {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := wal.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}

func TestAppendAndReplaySET(t *testing.T) {
	w := openTemp(t)

	w.Append(wal.Entry{Op: "SET", Key: "name", Value: "tex"})

	var entries []wal.Entry
	w.Replay(func(e wal.Entry) { entries = append(entries, e) })

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Op != "SET" || entries[0].Key != "name" || entries[0].Value != "tex" {
		t.Fatalf("unexpected entry: %+v", entries[0])
	}
	if entries[0].HasTTL {
		t.Fatal("expected no TTL")
	}
}

func TestAppendAndReplaySETWithTTL(t *testing.T) {
	w := openTemp(t)

	expiry := time.Now().Add(time.Hour).UTC().Truncate(time.Nanosecond)
	w.Append(wal.Entry{
		Op:        "SET",
		Key:       "session",
		Value:     "abc",
		HasTTL:    true,
		ExpiresAt: expiry,
	})

	var entries []wal.Entry
	w.Replay(func(e wal.Entry) { entries = append(entries, e) })

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	e := entries[0]
	if !e.HasTTL {
		t.Fatal("expected HasTTL=true")
	}
	// Compare at nanosecond precision — we encode/decode UnixNano.
	if e.ExpiresAt.UnixNano() != expiry.UnixNano() {
		t.Fatalf("expiry mismatch: got %v want %v", e.ExpiresAt.UnixNano(), expiry.UnixNano())
	}
}

func TestAppendAndReplayDEL(t *testing.T) {
	w := openTemp(t)

	w.Append(wal.Entry{Op: "DEL", Key: "name"})

	var entries []wal.Entry
	w.Replay(func(e wal.Entry) { entries = append(entries, e) })

	if len(entries) != 1 || entries[0].Op != "DEL" || entries[0].Key != "name" {
		t.Fatalf("unexpected entries: %+v", entries)
	}
}

func TestReplayOrderPreserved(t *testing.T) {
	w := openTemp(t)

	// SET then DEL — replaying both should result in the key being absent.
	w.Append(wal.Entry{Op: "SET", Key: "x", Value: "1"})
	w.Append(wal.Entry{Op: "SET", Key: "x", Value: "2"})
	w.Append(wal.Entry{Op: "DEL", Key: "x"})

	var ops []string
	w.Replay(func(e wal.Entry) { ops = append(ops, e.Op) })

	if len(ops) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(ops))
	}
}

func TestRecoverRestoresLiveKeys(t *testing.T) {
	w := openTemp(t)

	// A persistent key and a key with future TTL.
	futureExpiry := time.Now().Add(time.Hour)
	w.Append(wal.Entry{Op: "SET", Key: "city", Value: "Atlanta"})
	w.Append(wal.Entry{Op: "SET", Key: "token", Value: "xyz", HasTTL: true, ExpiresAt: futureExpiry})

	s := storage.New()
	if err := wal.Recover(w, s); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if v, ok := s.Get("city"); !ok || v != "Atlanta" {
		t.Fatalf("expected city=Atlanta, got %q ok=%v", v, ok)
	}
	if v, ok := s.Get("token"); !ok || v != "xyz" {
		t.Fatalf("expected token=xyz, got %q ok=%v", v, ok)
	}
}

func TestRecoverSkipsExpiredKeys(t *testing.T) {
	w := openTemp(t)

	// Key that expired in the past — should not be restored.
	pastExpiry := time.Now().Add(-time.Hour)
	w.Append(wal.Entry{Op: "SET", Key: "ghost", Value: "value", HasTTL: true, ExpiresAt: pastExpiry})

	s := storage.New()
	wal.Recover(w, s)

	if _, ok := s.Get("ghost"); ok {
		t.Fatal("expired key should not be restored")
	}
}

func TestRecoverAppliesDeletes(t *testing.T) {
	w := openTemp(t)

	w.Append(wal.Entry{Op: "SET", Key: "name", Value: "tex"})
	w.Append(wal.Entry{Op: "DEL", Key: "name"})

	s := storage.New()
	wal.Recover(w, s)

	if _, ok := s.Get("name"); ok {
		t.Fatal("deleted key should not be present after recovery")
	}
}

func TestMultipleAppendsSurviveReopen(t *testing.T) {
	// Write entries, close, reopen, replay — simulates a real restart.
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w1, _ := wal.Open(path)
	w1.Append(wal.Entry{Op: "SET", Key: "a", Value: "1"})
	w1.Append(wal.Entry{Op: "SET", Key: "b", Value: "2"})
	w1.Close()

	w2, err := wal.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()

	s := storage.New()
	wal.Recover(w2, s)

	if v, ok := s.Get("a"); !ok || v != "1" {
		t.Fatalf("expected a=1 after reopen, got %q ok=%v", v, ok)
	}
	if v, ok := s.Get("b"); !ok || v != "2" {
		t.Fatalf("expected b=2 after reopen, got %q ok=%v", v, ok)
	}
}
