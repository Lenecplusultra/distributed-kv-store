// Package storage provides a concurrent-safe in-memory key-value store
// with TTL expiry, absolute-timestamp eviction, and a background sweeper.
package storage

import (
	"context"
	"sync"
	"time"
)

// Entry holds a value and its optional expiry metadata.
type Entry struct {
	Value     string
	ExpiresAt time.Time // absolute wall-clock time, only valid when HasTTL is true
	HasTTL    bool
}

// Store is a thread-safe in-memory key-value store.
//
// Concurrency model: sync.RWMutex allows many concurrent readers
// but gives writers exclusive access. No other package writes to
// the internal map directly.
type Store struct {
	mu   sync.RWMutex
	data map[string]Entry
}

// New creates and returns an empty Store.
func New() *Store {
	return &Store{data: make(map[string]Entry)}
}

// Set stores a key-value pair with no expiry.
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = Entry{Value: value}
}

// SetWithTTL stores a key that expires after ttl duration from now.
// Internally converts to an absolute timestamp so the expiry is
// wall-clock correct even if the server restarts.
func (s *Store) SetWithTTL(key, value string, ttl time.Duration) {
	s.SetWithExpiry(key, value, time.Now().Add(ttl))
}

// SetWithExpiry stores a key with a pre-computed absolute expiry time.
// Used during WAL replay, where the original expiry timestamp is restored
// directly — not recomputed from a relative duration.
func (s *Store) SetWithExpiry(key, value string, expiresAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = Entry{
		Value:     value,
		ExpiresAt: expiresAt,
		HasTTL:    true,
	}
}

// Get retrieves a value by key.
// Returns ("", false) if the key does not exist or has expired.
// Expiry is checked lazily on read — the background sweeper handles
// bulk cleanup; this handles the single-key fast path.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.data[key]
	if !ok {
		return "", false
	}
	if entry.HasTTL && time.Now().After(entry.ExpiresAt) {
		return "", false
	}
	return entry.Value, true
}

// Delete removes a key. Returns true if the key existed.
func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.data[key]
	if ok {
		delete(s.data, key)
	}
	return ok
}

// Len returns the number of entries currently in the map.
// Includes expired keys not yet swept — use for metrics, not correctness.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// StartSweeper launches a background goroutine that periodically evicts
// expired keys from the map. It runs until ctx is cancelled.
//
// Why a sweeper in addition to lazy expiry?
// Lazy expiry only cleans keys when they are read. Keys that are written
// with a TTL and never read again would stay in memory forever.
// The sweeper catches those.
//
// Why two phases (RLock then WLock)?
// Holding a write lock while scanning the entire map would block all
// readers for the full scan duration. Instead: scan cheaply under a
// read lock, collect the expired keys, then acquire a write lock only
// to delete them. The re-check inside the write lock handles the race
// where a key is refreshed between the two phases.
func (s *Store) StartSweeper(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.sweep()
			case <-ctx.Done():
				return
			}
		}
	}()
}

// sweep performs one eviction pass. Called by the sweeper goroutine.
func (s *Store) sweep() {
	// Phase 1: collect expired keys under read lock — readers not blocked.
	s.mu.RLock()
	var expired []string
	now := time.Now()
	for k, e := range s.data {
		if e.HasTTL && now.After(e.ExpiresAt) {
			expired = append(expired, k)
		}
	}
	s.mu.RUnlock()

	if len(expired) == 0 {
		return
	}

	// Phase 2: delete under write lock — re-verify each key because a
	// concurrent Set may have refreshed it between phase 1 and here.
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range expired {
		if e, ok := s.data[k]; ok && e.HasTTL && time.Now().After(e.ExpiresAt) {
			delete(s.data, k)
		}
	}
}
