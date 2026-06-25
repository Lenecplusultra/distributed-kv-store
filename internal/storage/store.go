// Package storage provides a concurrent-safe in-memory key-value store
// with TTL expiry. Designed to be the single source of truth for all
// key-value data on a node — no other package writes to the map directly.
package storage

import (
	"sync"
	"time"
)

// Entry holds a value and its optional expiry metadata.
type Entry struct {
	Value     string
	ExpiresAt time.Time
	HasTTL    bool
}

// Store is a thread-safe in-memory key-value store.
//
// Concurrency model: sync.RWMutex allows many concurrent reads
// but serializes writes. A write (Set/Delete) blocks until all
// current reads finish, then holds exclusive access.
type Store struct {
	mu   sync.RWMutex
	data map[string]Entry
}

// New creates and returns an empty Store.
func New() *Store {
	return &Store{
		data: make(map[string]Entry),
	}
}

// Set stores a key-value pair with no expiry.
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = Entry{Value: value}
}

// SetWithTTL stores a key-value pair that expires after ttl duration.
// The key will return ("", false) from Get once the TTL has elapsed.
func (s *Store) SetWithTTL(key, value string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = Entry{
		Value:     value,
		ExpiresAt: time.Now().Add(ttl),
		HasTTL:    true,
	}
}

// Get retrieves a value by key.
// Returns ("", false) if the key does not exist or has expired.
// Expiry is checked lazily on read — no background sweeper needed yet.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.data[key]
	if !ok {
		return "", false
	}
	if entry.HasTTL && time.Now().After(entry.ExpiresAt) {
		// Key is logically expired. We return nothing but don't
		// delete here — we hold only an RLock. Cleanup happens
		// either on next write or via a future background sweeper.
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

// Len returns the current number of stored entries (including expired ones
// not yet cleaned up). Useful for metrics.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}
