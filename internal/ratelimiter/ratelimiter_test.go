package ratelimiter

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakeClock drives limiter time deterministically. Tests never sleep, so they
// stay fast and produce no flakes under -race.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// newTestLimiter builds a limiter wired to a fake clock.
func newTestLimiter(rate, burst float64) (*Limiter, *fakeClock) {
	clk := newFakeClock()
	l := New(rate, burst)
	l.now = clk.Now
	return l, clk
}

func TestNewPanicsOnInvalidConfig(t *testing.T) {
	tests := []struct {
		name  string
		rate  float64
		burst float64
	}{
		{"zero rate", 0, 10},
		{"negative rate", -1, 10},
		{"zero burst", 10, 0},
		{"burst below one", 10, 0.5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic, got none")
				}
			}()
			New(tt.rate, tt.burst)
		})
	}
}

func TestNilLimiterAllowsEverything(t *testing.T) {
	var l *Limiter

	for i := 0; i < 100; i++ {
		if !l.Allow("client") {
			t.Fatal("nil limiter must allow all requests")
		}
	}
	if l.Len() != 0 {
		t.Fatalf("nil limiter Len = %d, want 0", l.Len())
	}
	l.StartReaper(context.Background(), time.Second, time.Second)
}

func TestNewClientStartsFull(t *testing.T) {
	l, _ := newTestLimiter(1, 5)

	// A brand new client should be able to spend its whole burst at once.
	for i := 0; i < 5; i++ {
		if !l.Allow("fresh") {
			t.Fatalf("request %d denied, expected full initial bucket", i+1)
		}
	}
	if l.Allow("fresh") {
		t.Fatal("request 6 allowed, expected bucket exhausted")
	}
}

func TestBurstThenRefill(t *testing.T) {
	l, clk := newTestLimiter(2, 4) // 2 tokens/sec, capacity 4

	for i := 0; i < 4; i++ {
		if !l.Allow("c") {
			t.Fatalf("burst request %d denied", i+1)
		}
	}
	if l.Allow("c") {
		t.Fatal("request past burst allowed")
	}

	// Half a second at 2/sec credits exactly one token.
	clk.Advance(500 * time.Millisecond)
	if !l.Allow("c") {
		t.Fatal("request denied after one token refilled")
	}
	if l.Allow("c") {
		t.Fatal("second request allowed, only one token had accrued")
	}
}

func TestRefillCapsAtBurst(t *testing.T) {
	l, clk := newTestLimiter(10, 3)

	l.Allow("c")               // spend one, 2 remain
	clk.Advance(1 * time.Hour) // would credit 36000 tokens uncapped

	if got := l.Tokens("c"); got != 3 {
		t.Fatalf("tokens = %v, want capped at burst 3", got)
	}

	for i := 0; i < 3; i++ {
		if !l.Allow("c") {
			t.Fatalf("request %d denied after full refill", i+1)
		}
	}
	if l.Allow("c") {
		t.Fatal("bucket held more than burst tokens")
	}
}

func TestClientsAreIndependent(t *testing.T) {
	l, _ := newTestLimiter(1, 2)

	// Exhaust one client entirely.
	for i := 0; i < 2; i++ {
		if !l.Allow("noisy") {
			t.Fatalf("noisy request %d denied", i+1)
		}
	}
	if l.Allow("noisy") {
		t.Fatal("noisy client not throttled")
	}

	// A different identity must be unaffected.
	if !l.Allow("quiet") {
		t.Fatal("quiet client throttled by noisy client's usage")
	}
	if l.Len() != 2 {
		t.Fatalf("Len = %d, want 2 distinct buckets", l.Len())
	}
}

func TestTokensUnknownClientReportsBurst(t *testing.T) {
	l, _ := newTestLimiter(1, 7)

	if got := l.Tokens("never-seen"); got != 7 {
		t.Fatalf("Tokens = %v, want 7", got)
	}
	// Querying must not create a bucket.
	if l.Len() != 0 {
		t.Fatalf("Len = %d, want 0 — Tokens should not allocate", l.Len())
	}
}

func TestClockGoingBackwardsDoesNotCreditTokens(t *testing.T) {
	l, clk := newTestLimiter(1, 3)

	for i := 0; i < 3; i++ {
		l.Allow("c")
	}
	clk.Advance(-10 * time.Second)

	if l.Allow("c") {
		t.Fatal("negative elapsed time credited tokens")
	}
}

func TestReapRemovesIdleBuckets(t *testing.T) {
	l, clk := newTestLimiter(1, 5)

	l.Allow("stale")
	clk.Advance(10 * time.Minute)
	l.Allow("active")

	if l.Len() != 2 {
		t.Fatalf("Len = %d, want 2 before reap", l.Len())
	}

	l.reap(5 * time.Minute)

	if l.Len() != 1 {
		t.Fatalf("Len = %d, want 1 after reap", l.Len())
	}
	if _, ok := l.buckets["active"]; !ok {
		t.Fatal("reaper removed the active bucket")
	}
	if _, ok := l.buckets["stale"]; ok {
		t.Fatal("reaper kept the stale bucket")
	}
}

func TestStartReaperStopsOnContextCancel(t *testing.T) {
	l, _ := newTestLimiter(1, 5)
	ctx, cancel := context.WithCancel(context.Background())

	l.StartReaper(ctx, time.Millisecond, time.Hour)
	cancel()

	// Give the goroutine a moment to observe cancellation. Under -race this
	// also exercises concurrent map access between reaper and Allow.
	time.Sleep(20 * time.Millisecond)
	l.Allow("c")
}

func TestConcurrentAllowIsRaceFree(t *testing.T) {
	l, _ := newTestLimiter(1000, 1000)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				l.Allow("shared")
				l.Allow("private")
				l.Tokens("shared")
			}
		}(i)
	}
	wg.Wait()

	if l.Len() != 2 {
		t.Fatalf("Len = %d, want 2", l.Len())
	}
}

func TestExactlyBurstRequestsAdmittedNoMore(t *testing.T) {
	l, _ := newTestLimiter(1, 10)

	admitted := 0
	for i := 0; i < 25; i++ {
		if l.Allow("c") {
			admitted++
		}
	}
	if admitted != 10 {
		t.Fatalf("admitted = %d, want exactly burst 10", admitted)
	}
}
