// Command server runs a single node of the distributed key-value store.
//
// Configuration is entirely through environment variables:
//
//	ADDR        listen address                  (default :6379)
//	WAL_PATH    write-ahead log file            (default data/wal.log)
//	REPLICAS    comma-separated replica addrs   (default none)
//	RATE_LIMIT  sustained requests/sec/client   (default unset = unlimited)
//	BURST       token bucket capacity           (default = RATE_LIMIT)
//
// Startup order:
//
//	open WAL → recover into store → start TTL sweeper → build replicator
//	→ build rate limiter → start reaper → listen
//
// The WAL is recovered before the listener binds, so no client can read a
// partially recovered store.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Lenecplusultra/distributed-kv-store/internal/ratelimiter"
	"github.com/Lenecplusultra/distributed-kv-store/internal/replication"
	"github.com/Lenecplusultra/distributed-kv-store/internal/storage"
	"github.com/Lenecplusultra/distributed-kv-store/internal/tcp"
	"github.com/Lenecplusultra/distributed-kv-store/internal/wal"
)

const (
	defaultAddr    = ":6379"
	defaultWALPath = "data/wal.log"
	sweepInterval  = 5 * time.Second
	reaperInterval = 1 * time.Minute
	reaperIdleTTL  = 10 * time.Minute
)

func main() {
	addr := envOr("ADDR", defaultAddr)
	walPath := envOr("WAL_PATH", defaultWALPath)

	// One context cancels the sweeper, the limiter reaper, and anything else
	// background, so a single signal shuts the whole node down.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.New()

	// --- WAL: open and recover before accepting any traffic ---
	w, err := wal.Open(walPath)
	if err != nil {
		log.Fatalf("[server] could not open WAL at %s: %v", walPath, err)
	}
	defer w.Close()

	if err := wal.Recover(w, store); err != nil {
		log.Fatalf("[server] WAL recovery failed: %v", err)
	}
	log.Printf("[server] recovered %d keys from %s", store.Len(), walPath)

	// --- Background TTL sweeper ---
	// Lazy expiry on read handles keys that are read again. The sweeper
	// reclaims keys that expire and are never read.
	store.StartSweeper(ctx, sweepInterval)

	// --- Replication ---
	var replicator *replication.Replicator
	if raw := os.Getenv("REPLICAS"); raw != "" {
		addrs := splitAndTrim(raw)
		if len(addrs) > 0 {
			replicator = replication.New(addrs)
			log.Printf("[server] replicating to %v", addrs)
		}
	}

	// --- Rate limiting ---
	limiter, err := buildLimiter()
	if err != nil {
		log.Fatalf("[server] %v", err)
	}
	if limiter != nil {
		limiter.StartReaper(ctx, reaperInterval, reaperIdleTTL)
	}

	// --- Server ---
	// limiter may be nil; WithLimiter(nil) leaves limiting disabled, and
	// Allow on a nil limiter admits everything.
	srv := tcp.New(addr, store, w, replicator, tcp.WithLimiter(limiter))

	// Signal handling: cancel background work, then unblock Start.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("[server] received %s, shutting down", sig)
		cancel()
		srv.Shutdown()
	}()

	if err := srv.Start(); err != nil {
		log.Fatalf("[server] fatal: %v", err)
	}
	log.Printf("[server] stopped cleanly")
}

// buildLimiter reads RATE_LIMIT and BURST, returning nil when rate limiting
// is not configured.
//
// Misconfiguration is a startup failure rather than a silent fallback. A
// server that was meant to be throttled but is quietly running unlimited is
// worse than one that refuses to boot.
func buildLimiter() (*ratelimiter.Limiter, error) {
	rateStr := os.Getenv("RATE_LIMIT")
	if rateStr == "" {
		if os.Getenv("BURST") != "" {
			return nil, fmt.Errorf("BURST is set but RATE_LIMIT is not — rate limiting would be disabled")
		}
		return nil, nil
	}

	rate, err := strconv.ParseFloat(rateStr, 64)
	if err != nil || rate <= 0 {
		return nil, fmt.Errorf("invalid RATE_LIMIT %q: must be a positive number", rateStr)
	}

	// Burst defaults to the rate: one second of traffic can arrive at once.
	burst := rate
	if burstStr := os.Getenv("BURST"); burstStr != "" {
		burst, err = strconv.ParseFloat(burstStr, 64)
		if err != nil || burst < 1 {
			return nil, fmt.Errorf("invalid BURST %q: must be at least 1", burstStr)
		}
	}

	log.Printf("[server] rate limiting enabled: %.1f req/s per client, burst %.0f", rate, burst)
	return ratelimiter.New(rate, burst), nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// splitAndTrim splits a comma-separated list, dropping empty entries so that
// trailing commas and stray whitespace do not become bogus addresses.
func splitAndTrim(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
