// Command server runs a single node of the distributed key-value store.
//
// Configuration is entirely through environment variables:
//
//	ADDR          listen address                  (default :6379)
//	CAPACITY      max keys before LRU eviction    (default unset = unlimited)
//	METRICS_ADDR  Prometheus endpoint address     (default :9090, "" disables)
//	WAL_PATH      write-ahead log file            (default data/wal.log)
//	REPLICAS      comma-separated replica addrs   (default none)
//	RATE_LIMIT    sustained requests/sec/client   (default unset = unlimited)
//	BURST         token bucket capacity           (default = RATE_LIMIT)
//
// Startup order:
//
//	open WAL → recover into store → start TTL sweeper → build replicator
//	→ build rate limiter → start reaper → serve metrics → listen
//
// The WAL is recovered before the listener binds, so no client can read a
// partially recovered store.
//
// # Two listeners
//
// The wire protocol is a bespoke line format and cannot also speak HTTP, so
// metrics are served on their own port. That is the usual shape for this —
// an application port and a separate operational port — and it means a
// Kubernetes deployment in Phase 9 can scrape metrics without exposing them
// on the data path.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Lenecplusultra/distributed-kv-store/internal/metrics"
	"github.com/Lenecplusultra/distributed-kv-store/internal/ratelimiter"
	"github.com/Lenecplusultra/distributed-kv-store/internal/replication"
	"github.com/Lenecplusultra/distributed-kv-store/internal/storage"
	"github.com/Lenecplusultra/distributed-kv-store/internal/tcp"
	"github.com/Lenecplusultra/distributed-kv-store/internal/wal"
)

const (
	defaultAddr        = ":6379"
	defaultMetricsAddr = ":9090"
	defaultWALPath     = "data/wal.log"
	sweepInterval      = 5 * time.Second
	reaperInterval     = 1 * time.Minute
	reaperIdleTTL      = 10 * time.Minute
	metricsIOTimeout   = 5 * time.Second
)

func main() {
	addr := envOr("ADDR", defaultAddr)
	walPath := envOr("WAL_PATH", defaultWALPath)

	// One context cancels the sweeper, the limiter reaper, and anything else
	// background, so a single signal shuts the whole node down.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, err := buildStore()
	if err != nil {
		log.Fatalf("[server] %v", err)
	}
	m := metrics.New()

	// --- WAL: open and recover before accepting any traffic ---
	w, walErr := wal.Open(walPath)
	if walErr != nil {
		log.Fatalf("[server] could not open WAL at %s: %v", walPath, walErr)
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
			defer replicator.Close()
			log.Printf("[server] replicating to %v", addrs)

			// Sampled at scrape time rather than mirrored into a counter:
			// the replicator's health tracker is the source of truth, and
			// copying it here would create a second thing to keep in sync.
			m.RegisterGauge("kv_replica_alive",
				"1 if the replica is accepting writes, 0 if declared dead",
				"replica",
				func() map[string]float64 {
					out := make(map[string]float64)
					for addr, st := range replicator.Health() {
						if st.Alive {
							out[addr] = 1
						} else {
							out[addr] = 0
						}
					}
					return out
				})
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

	// --- Metrics endpoint ---
	metricsSrv := startMetricsServer(m)

	// --- Server ---
	// limiter may be nil; WithLimiter(nil) leaves limiting disabled, and
	// Allow on a nil limiter admits everything.
	srv := tcp.New(addr, store, w, replicator,
		tcp.WithLimiter(limiter),
		tcp.WithMetrics(m),
	)

	// Signal handling: cancel background work, then unblock Start.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("[server] received %s, shutting down", sig)
		cancel()
		if metricsSrv != nil {
			shutdownCtx, done := context.WithTimeout(context.Background(), 2*time.Second)
			defer done()
			metricsSrv.Shutdown(shutdownCtx)
		}
		srv.Shutdown()
	}()

	if err := srv.Start(); err != nil {
		log.Fatalf("[server] fatal: %v", err)
	}
	log.Printf("[server] stopped cleanly")
}

// startMetricsServer serves the Prometheus endpoint on its own port,
// returning nil when METRICS_ADDR is explicitly empty.
//
// A failure to bind is logged rather than fatal: losing observability should
// not take down a node that is otherwise serving traffic correctly. That is
// the opposite of the RATE_LIMIT decision below, and deliberately so — a
// silently unthrottled server misbehaves, whereas a server without metrics
// merely goes unobserved.
func startMetricsServer(m *metrics.Metrics) *http.Server {
	addr, set := os.LookupEnv("METRICS_ADDR")
	if !set {
		addr = defaultMetricsAddr
	}
	if addr == "" {
		log.Printf("[server] metrics endpoint disabled")
		return nil
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  metricsIOTimeout,
		WriteTimeout: metricsIOTimeout,
	}

	go func() {
		log.Printf("[server] metrics on %s/metrics", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[server] metrics endpoint stopped: %v", err)
		}
	}()

	return srv
}

// buildStore reads CAPACITY and returns either an unlimited store or one
// with LRU eviction enabled.
//
// Unlimited is the default because eviction changes the system's semantics:
// a key that was written can silently disappear. That is the right tradeoff
// for a cache and the wrong one for a store of record, so it should be an
// explicit choice rather than something that happens once a workload grows.
//
// Note that WAL replay restores evicted keys, since evictions are not
// written to the log — memory management is not a client-visible deletion.
// A node that has evicted heavily will therefore come back with more keys
// resident than it had before restarting.
func buildStore() (*storage.Store, error) {
	raw := os.Getenv("CAPACITY")
	if raw == "" {
		return storage.New(), nil
	}

	capacity, err := strconv.Atoi(raw)
	if err != nil || capacity < 1 {
		return nil, fmt.Errorf("invalid CAPACITY %q: must be a positive integer", raw)
	}

	log.Printf("[server] LRU eviction enabled: capacity %d keys", capacity)
	return storage.NewWithCapacity(capacity), nil
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
