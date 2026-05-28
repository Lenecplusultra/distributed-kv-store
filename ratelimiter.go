// Package ratelimiter implements a token-bucket rate limiter (Phase 7).
//
// Each client (identified by IP or key) gets its own bucket.
// Requests consume one token. Tokens refill at a configured rate.
package ratelimiter

import (
	"sync"
	"time"
)

// Bucket holds the token state for a single client.
type Bucket struct {
	tokens    float64
	lastRefil time.Time
	mu        sync.Mutex
}

// Limiter manages per-client token buckets.
type Limiter struct {
	capacity   float64       // max tokens per bucket
	refillRate float64       // tokens added per second
	mu         sync.Mutex
	buckets    map[string]*Bucket
}

// New creates a Limiter with given capacity and refill rate.
func New(capacity float64, refillPerSecond float64) *Limiter {
	return &Limiter{
		capacity:   capacity,
		refillRate: refillPerSecond,
		buckets:    make(map[string]*Bucket),
	}
}

// Allow returns true if the client identified by key is permitted to proceed.
func (l *Limiter) Allow(key string) bool {
	l.mu.Lock()
	b, ok := l.buckets[key]
	if !ok {
		b = &Bucket{tokens: l.capacity, lastRefil: time.Now()}
		l.buckets[key] = b
	}
	l.mu.Unlock()

	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(b.lastRefil).Seconds()
	b.tokens += elapsed * l.refillRate
	if b.tokens > l.capacity {
		b.tokens = l.capacity
	}
	b.lastRefil = now

	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}
