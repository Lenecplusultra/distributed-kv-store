// Package storage provides a concurrent-safe in-memory key-value store
// with TTL expiry, absolute-timestamp eviction, a background sweeper,
// and optional LRU eviction when a capacity limit is set.
package storage

import (
	"context"
	"sync"
	"time"

	"github.com/Lenecplusultra/distributed-kv-store/internal/lru"
)

// Entry holds a value and its optional expiry metadata.
type Entry struct {
	Value     string
	ExpiresAt time.Time
	HasTTL    bool
}

// Store is a thread-safe in-memory key-value store.
//
// If capacity > 0, the store uses LRU eviction to stay within the limit.
// If capacity == 0, the store grows without bound (original behaviour).
//
// Locking model: the RWMutex covers both the data map and the LRU cache.
// The LRU cache is not independently locked — it relies on the store's
// lock. This avoids nested locks and the deadlocks they invite.
type Store struct {
	mu       sync.RWMutex
	data     map[string]Entry
	cache    *lru.Cache // nil when capacity == 0
	capacity int        // 0 = unlimited
}

// New creates an unlimited Store (no eviction).
func New() *Store {
	return &Store{data: make(map[string]Entry)}
}

// NewWithCapacity creates a Store that evicts the least recently used
// key when the number of entries would exceed capacity.
func NewWithCapacity(capacity int) *Store {
	return &Store{
		data:     make(map[string]Entry, capacity),
		cache:    lru.New(capacity),
		capacity: capacity,
	}
}

// Set stores a key-value pair with no expiry.
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = Entry{Value: value}
	s.evictIfNeeded(key)
}

// SetWithTTL stores a key that expires after ttl duration from now.
func (s *Store) SetWithTTL(key, value string, ttl time.Duration) {
	s.SetWithExpiry(key, value, time.Now().Add(ttl))
}

// SetWithExpiry stores a key with a pre-computed absolute expiry time.
func (s *Store) SetWithExpiry(key, value string, expiresAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = Entry{
		Value:     value,
		ExpiresAt: expiresAt,
		HasTTL:    true,
	}
	s.evictIfNeeded(key)
}

// Get retrieves a value by key.
// Returns ("", false) if the key does not exist or has expired.
// A successful Get counts as a recent access for LRU purposes.
func (s *Store) Get(key string) (string, bool) {
	s.mu.Lock() // Write lock — LRU Touch modifies the list.
	defer s.mu.Unlock()

	entry, ok := s.data[key]
	if !ok {
		return "", false
	}
	if entry.HasTTL && time.Now().After(entry.ExpiresAt) {
		return "", false
	}

	if s.cache != nil {
		s.cache.Touch(key) // existing key — no eviction possible
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
		if s.cache != nil {
			s.cache.Remove(key)
		}
	}
	return ok
}

// Len returns the number of entries currently in the map.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// evictIfNeeded tells the LRU cache about the access and removes the
// evicted key from the data map if capacity is exceeded.
// Must be called with s.mu write lock held.
func (s *Store) evictIfNeeded(key string) {
	if s.cache == nil {
		return
	}
	evicted, ok := s.cache.Touch(key)
	if ok {
		// Eviction is a memory decision — not written to WAL.
		// On restart, WAL replay restores it and LRU starts fresh.
		delete(s.data, evicted)
	}
}

// StartSweeper launches a background goroutine that periodically evicts
// expired keys. Runs until ctx is cancelled.
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

// sweep performs one TTL eviction pass.
func (s *Store) sweep() {
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

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range expired {
		if e, ok := s.data[k]; ok && e.HasTTL && time.Now().After(e.ExpiresAt) {
			delete(s.data, k)
			if s.cache != nil {
				s.cache.Remove(k)
			}
		}
	}
}
