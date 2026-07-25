package storage_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Lenecplusultra/distributed-kv-store/internal/storage"
)

// ── Basic operations ──────────────────────────────────────────────────────────

func TestSetAndGet(t *testing.T) {
	s := storage.New()
	s.Set("name", "tex")

	val, ok := s.Get("name")
	if !ok || val != "tex" {
		t.Fatalf("expected tex, got %q ok=%v", val, ok)
	}
}

func TestOverwrite(t *testing.T) {
	s := storage.New()
	s.Set("key", "first")
	s.Set("key", "second")

	val, ok := s.Get("key")
	if !ok || val != "second" {
		t.Fatalf("expected second, got %q ok=%v", val, ok)
	}
}

func TestGetMissing(t *testing.T) {
	s := storage.New()
	if _, ok := s.Get("ghost"); ok {
		t.Fatal("expected missing key to return false")
	}
}

func TestDelete(t *testing.T) {
	s := storage.New()
	s.Set("key", "value")

	if !s.Delete("key") {
		t.Fatal("expected Delete to return true for existing key")
	}
	if _, ok := s.Get("key"); ok {
		t.Fatal("deleted key should not be retrievable")
	}
}

func TestDeleteMissing(t *testing.T) {
	s := storage.New()
	if s.Delete("ghost") {
		t.Fatal("expected false when deleting nonexistent key")
	}
}

// ── TTL / expiry ──────────────────────────────────────────────────────────────

func TestTTLKeyLivesBeforeExpiry(t *testing.T) {
	s := storage.New()
	s.SetWithTTL("session", "abc", 200*time.Millisecond)

	val, ok := s.Get("session")
	if !ok || val != "abc" {
		t.Fatal("key should be alive before TTL expires")
	}
}

func TestTTLKeyExpires(t *testing.T) {
	s := storage.New()
	s.SetWithTTL("temp", "gone", 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	if _, ok := s.Get("temp"); ok {
		t.Fatal("key should have expired")
	}
}

func TestSetWithExpiryRestoresAbsoluteTimestamp(t *testing.T) {
	// SetWithExpiry is used during WAL replay — verify it honours the
	// pre-computed absolute timestamp, not a fresh duration.
	s := storage.New()
	past := time.Now().Add(-time.Second) // already expired
	s.SetWithExpiry("x", "value", past)

	if _, ok := s.Get("x"); ok {
		t.Fatal("key with past expiry should be immediately invisible")
	}
}

func TestSetWithExpiryFuture(t *testing.T) {
	s := storage.New()
	future := time.Now().Add(time.Hour)
	s.SetWithExpiry("x", "alive", future)

	val, ok := s.Get("x")
	if !ok || val != "alive" {
		t.Fatalf("expected alive, got %q ok=%v", val, ok)
	}
}

func TestTTLOverwrittenBySet(t *testing.T) {
	s := storage.New()
	s.SetWithTTL("key", "expiring", 50*time.Millisecond)
	s.Set("key", "permanent") // overwrite with no-TTL entry

	time.Sleep(100 * time.Millisecond)

	val, ok := s.Get("key")
	if !ok || val != "permanent" {
		t.Fatalf("expected permanent after overwrite, got %q ok=%v", val, ok)
	}
}

// ── Background sweeper ────────────────────────────────────────────────────────

func TestSweeperEvictsExpiredKeys(t *testing.T) {
	s := storage.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.SetWithTTL("ephemeral", "value", 50*time.Millisecond)
	s.Set("permanent", "stays")

	// Sweeper runs every 20ms — fast for testing.
	s.StartSweeper(ctx, 20*time.Millisecond)

	// Wait for TTL to fire and sweeper to run at least once.
	time.Sleep(150 * time.Millisecond)

	// Expired key should be gone from the map, not just invisible.
	if s.Len() != 1 {
		t.Fatalf("expected 1 entry after sweep, got %d (expired key still in map)", s.Len())
	}
	if _, ok := s.Get("permanent"); !ok {
		t.Fatal("permanent key should still exist")
	}
}

func TestSweeperStopsOnContextCancel(t *testing.T) {
	s := storage.New()
	ctx, cancel := context.WithCancel(context.Background())

	s.StartSweeper(ctx, 10*time.Millisecond)
	cancel() // stop the sweeper immediately

	// If the sweeper goroutine leaks, the race detector would catch it
	// after the test exits. This test primarily verifies no panic/deadlock.
	time.Sleep(30 * time.Millisecond)
}

func TestSweeperDoesNotEvictLiveKeys(t *testing.T) {
	s := storage.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.SetWithTTL("future", "value", time.Hour)
	s.Set("nott", "value")

	s.StartSweeper(ctx, 20*time.Millisecond)
	time.Sleep(60 * time.Millisecond)

	if s.Len() != 2 {
		t.Fatalf("sweeper evicted live keys, expected 2 got %d", s.Len())
	}
}

// ── Concurrency ───────────────────────────────────────────────────────────────

func TestConcurrentAccess(t *testing.T) {
	s := storage.New()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			s.Set("key", "value")
		}()
		go func() {
			defer wg.Done()
			s.Get("key")
		}()
	}
	wg.Wait()
}

func TestLen(t *testing.T) {
	s := storage.New()
	if s.Len() != 0 {
		t.Fatal("new store should be empty")
	}
	s.Set("a", "1")
	s.Set("b", "2")
	if s.Len() != 2 {
		t.Fatalf("expected 2, got %d", s.Len())
	}
	s.Delete("a")
	if s.Len() != 1 {
		t.Fatalf("expected 1, got %d", s.Len())
	}
}
