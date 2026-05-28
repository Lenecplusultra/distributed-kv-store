// Package storage provides a concurrent-safe in-memory key-value store
// with TTL support and LRU eviction (Phase 1 & 3).
package storage

import (
	"sync"
	"time"
)

// Entry holds a stored value and its optional expiry time.
type Entry struct {
	Value     string
	ExpiresAt time.Time
	HasTTL    bool
}

// Store is a thread-safe in-memory key-value store.
type Store struct {
	mu   sync.RWMutex
	data map[string]Entry
}

// New creates an empty Store.
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
func (s *Store) SetWithTTL(key, value string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = Entry{
		Value:     value,
		ExpiresAt: time.Now().Add(ttl),
		HasTTL:    true,
	}
}

// Get retrieves a value by key. Returns ("", false) if missing or expired.
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

// Len returns the number of keys currently stored (including expired, not yet evicted).
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}
