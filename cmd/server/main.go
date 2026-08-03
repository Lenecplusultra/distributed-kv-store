// Command server is the entrypoint for the distributed-kv-store node.
//
// Environment variables:
//
//	ADDR      bind address (default :6379)
//	WAL_PATH  path to WAL file (default data/wal.log)
//	REPLICAS  comma-separated replica addresses (e.g. localhost:6380,localhost:6381)
//	          if empty, node runs without replication
//
// Startup sequence:
//  1. Open WAL
//  2. Replay WAL → restore store
//  3. Start TTL sweeper
//  4. Configure replicator (if REPLICAS set)
//  5. Start TCP server
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Lenecplusultra/distributed-kv-store/internal/replication"
	"github.com/Lenecplusultra/distributed-kv-store/internal/storage"
	"github.com/Lenecplusultra/distributed-kv-store/internal/tcp"
	"github.com/Lenecplusultra/distributed-kv-store/internal/wal"
)

func main() {
	addr := env("ADDR", ":6379")
	walPath := env("WAL_PATH", "data/wal.log")

	// ── 1. Open WAL ───────────────────────────────────────────────────────────
	if err := os.MkdirAll(filepath.Dir(walPath), 0755); err != nil {
		log.Fatalf("[startup] create WAL dir: %v", err)
	}
	w, err := wal.Open(walPath)
	if err != nil {
		log.Fatalf("[startup] open WAL: %v", err)
	}
	defer w.Close()

	// ── 2. Replay WAL ─────────────────────────────────────────────────────────
	store := storage.New()
	log.Printf("[startup] replaying WAL from %s", walPath)
	if err := wal.Recover(w, store); err != nil {
		log.Fatalf("[startup] WAL recovery failed: %v", err)
	}
	log.Printf("[startup] recovery complete — %d keys restored", store.Len())

	// ── 3. Start TTL sweeper ──────────────────────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	store.StartSweeper(ctx, 5*time.Second)

	// ── 4. Configure replicator ───────────────────────────────────────────────
	var r *replication.Replicator
	if replicasEnv := env("REPLICAS", ""); replicasEnv != "" {
		var addrs []string
		for _, a := range strings.Split(replicasEnv, ",") {
			if a = strings.TrimSpace(a); a != "" {
				addrs = append(addrs, a)
			}
		}
		if len(addrs) > 0 {
			r = replication.New(addrs)
			log.Printf("[startup] replication enabled → %v", addrs)
		}
	}

	// ── 5. Start TCP server ───────────────────────────────────────────────────
	server := tcp.New(addr, store, w, r)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-quit
		log.Println("[server] shutdown signal received")
		cancel()
		server.Shutdown()
	}()

	log.Printf("[server] distributed-kv-store starting on %s", addr)
	if err := server.Start(); err != nil {
		log.Fatalf("[server] fatal: %v", err)
	}
	log.Println("[server] stopped")
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
