// Package ratelimiter implements per-client token-bucket rate limiting.
//
// Each client identity gets its own bucket. Tokens refill continuously at a
// fixed rate up to a burst capacity. A request consumes one token; if the
// bucket is empty the request is rejected.
//
// # Why token bucket
//
// Fixed windows allow a 2N burst across a window boundary. Sliding-window
// logs are exact but cost O(N) memory per client. Leaky buckets smooth
// output but reject legitimate bursts. Token bucket gives O(1) state per
// client and permits controlled bursts.
//
// Refill is lazy: no timer drives token accrual. Each bucket stores when it
// was last touched, and tokens owed are computed on access as
// elapsed * rate. This is the same technique used for lazy TTL expiry in
// internal/storage — compute on read rather than maintain a clock per entry.
//
// # Identity is the caller's problem
//
// The limiter keys on an opaque string and does not care what it means.
// Extracting that string from a connection (a HELLO handshake identifier,
// the remote IP, or anything else) belongs to the transport layer. Keeping
// identity policy out of accounting means one can change without the other.
//
// Note this is rate limiting the node itself, which is not what Redis does.
// Redis handles overload with connection caps (maxclients) and client-buffer
// eviction (maxmemory-clients), delegating request rate limiting to a proxy
// or the application layer. This server has no proxy hop to put it in — the
// smart-client architecture connects clients straight to nodes — so the
// limiter lives in the node. The closer real-world parallel is Envoy or an
// API gateway, not Redis.
package ratelimiter

import (
	"context"
	"sync"
	"time"
)

// bucket is the per-client token state. Guarded by Limiter.mu.
type bucket struct {
	tokens     float64
	lastRefill time.Time
}

// Limiter holds one token bucket per client identity.
//
// A nil *Limiter is valid and allows every request. This matches the nil-safe
// pattern used for the WAL and Replicator: an unconfigured subsystem degrades
// to a no-op rather than forcing a branch at every call site.
type Limiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket

	rate  float64 // tokens added per second
	burst float64 // maximum tokens a bucket can hold

	// now is injectable so tests can drive time deterministically instead of
	// sleeping. Production always uses time.Now.
	now func() time.Time
}

// New returns a Limiter granting rate tokens per second per client, with a
// bucket capacity of burst tokens.
//
// Panics on nonsensical configuration rather than silently misbehaving —
// the same contract as lru.New. A burst below 1 could never admit a single
// request, and a non-positive rate would mean buckets never refill.
func New(rate, burst float64) *Limiter {
	if rate <= 0 {
		panic("ratelimiter: rate must be positive")
	}
	if burst < 1 {
		panic("ratelimiter: burst must be at least 1")
	}
	return &Limiter{
		buckets: make(map[string]*bucket),
		rate:    rate,
		burst:   burst,
		now:     time.Now,
	}
}

// Allow consumes one token for clientID and reports whether the request may
// proceed. Safe for concurrent use.
//
// A previously unseen client starts with a full bucket, so a fresh client can
// burst immediately. Starting empty would penalise well-behaved clients for
// the crime of being new.
func (l *Limiter) Allow(clientID string) bool {
	if l == nil {
		return true
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()

	b, ok := l.buckets[clientID]
	if !ok {
		// New client: full bucket, minus the token this request consumes.
		l.buckets[clientID] = &bucket{tokens: l.burst - 1, lastRefill: now}
		return true
	}

	l.refillLocked(b, now)

	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// refillLocked credits tokens accrued since the bucket was last touched.
// Caller must hold l.mu.
func (l *Limiter) refillLocked(b *bucket, now time.Time) {
	elapsed := now.Sub(b.lastRefill)
	if elapsed <= 0 {
		// Clock went backwards, or no measurable time has passed. Credit
		// nothing, and leave lastRefill alone so a backwards clock cannot
		// permanently poison the bucket.
		return
	}

	b.tokens += elapsed.Seconds() * l.rate
	if b.tokens > l.burst {
		b.tokens = l.burst
	}
	b.lastRefill = now
}

// Tokens reports the current token count for clientID after refill, for
// observability and tests. An unknown client reports a full bucket, which is
// what it would be given on its next request. Querying does not allocate a
// bucket — otherwise metrics scraping would create state.
func (l *Limiter) Tokens(clientID string) float64 {
	if l == nil {
		return 0
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	b, ok := l.buckets[clientID]
	if !ok {
		return l.burst
	}
	l.refillLocked(b, l.now())
	return b.tokens
}

// Len reports how many buckets are currently tracked. Used by the reaper
// tests and by Phase 8 metrics.
func (l *Limiter) Len() int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}

// StartReaper launches a background goroutine that discards buckets untouched
// for longer than idleTTL, returning when ctx is cancelled.
//
// This exists because the bucket map is keyed on client-supplied identity.
// Without reaping, a client presenting a fresh identity per connection grows
// the map without bound — an unauthenticated memory-exhaustion vector that
// IP-keyed limiting would not have had. A bucket idle for longer than the
// time it takes to refill completely is indistinguishable from a fresh one,
// so dropping it loses no enforcement.
//
// The reaper bounds the memory cost of identity churn. It does not prevent
// the evasion itself; only authentication would.
func (l *Limiter) StartReaper(ctx context.Context, interval, idleTTL time.Duration) {
	if l == nil {
		return
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				l.reap(idleTTL)
			}
		}
	}()
}

// reap removes buckets idle for longer than idleTTL.
func (l *Limiter) reap(idleTTL time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	cutoff := l.now().Add(-idleTTL)
	for id, b := range l.buckets {
		if b.lastRefill.Before(cutoff) {
			delete(l.buckets, id)
		}
	}
}
