package storage_test

import (
	"sync"
	"testing"
	"time"

	"github.com/Lenecplusultra/distributed-kv-store/internal/storage"
)

func TestSetAndGet(t *testing.T) {
	s := storage.New()
	s.Set("name", "tex")

	val, ok := s.Get("name")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if val != "tex" {
		t.Fatalf("expected 'tex', got %q", val)
	}
}

func TestOverwrite(t *testing.T) {
	s := storage.New()
	s.Set("key", "first")
	s.Set("key", "second")

	val, ok := s.Get("key")
	if !ok || val != "second" {
		t.Fatalf("expected 'second', got %q (ok=%v)", val, ok)
	}
}

func TestGetMissing(t *testing.T) {
	s := storage.New()
	_, ok := s.Get("nonexistent")
	if ok {
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
		t.Fatal("expected deleted key to be gone")
	}
}

func TestDeleteMissing(t *testing.T) {
	s := storage.New()
	if s.Delete("ghost") {
		t.Fatal("expected Delete to return false for nonexistent key")
	}
}

func TestTTLKeyLives(t *testing.T) {
	s := storage.New()
	s.SetWithTTL("session", "abc", 200*time.Millisecond)

	val, ok := s.Get("session")
	if !ok || val != "abc" {
		t.Fatal("expected key to be alive before TTL expires")
	}
}

func TestTTLKeyExpires(t *testing.T) {
	s := storage.New()
	s.SetWithTTL("temp", "gone", 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	if _, ok := s.Get("temp"); ok {
		t.Fatal("expected key to have expired")
	}
}

func TestTTLOverwrittenBySet(t *testing.T) {
	s := storage.New()
	s.SetWithTTL("key", "expiring", 50*time.Millisecond)
	s.Set("key", "permanent") // overwrite before TTL fires

	time.Sleep(100 * time.Millisecond)

	val, ok := s.Get("key")
	if !ok || val != "permanent" {
		t.Fatalf("expected 'permanent' after overwrite, got %q (ok=%v)", val, ok)
	}
}

func TestLen(t *testing.T) {
	s := storage.New()
	if s.Len() != 0 {
		t.Fatal("expected empty store")
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

// TestConcurrentAccess checks for data races — run with: go test -race
func TestConcurrentAccess(t *testing.T) {
	s := storage.New()
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Set("key", "value")
		}()
	}
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Get("key")
		}()
	}
	wg.Wait()
}
