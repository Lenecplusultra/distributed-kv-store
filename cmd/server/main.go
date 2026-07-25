// Command server is the entrypoint for the distributed-kv-store node.
//
// Startup sequence (Phase 2):
//  1. Open the WAL file (create if it doesn't exist)
//  2. Replay the WAL into an empty store (crash recovery)
//  3. Start the background TTL sweeper
//  4. Start the TCP server
//  5. On SIGINT/SIGTERM: shutdown server, close WAL
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Lenecplusultra/distributed-kv-store/internal/storage"
	"github.com/Lenecplusultra/distributed-kv-store/internal/tcp"
	"github.com/Lenecplusultra/distributed-kv-store/internal/wal"
)

func main() {
	addr := env("ADDR", ":6379")
	walPath := env("WAL_PATH", "data/wal.log")
	sweepInterval := 5 * time.Second

	// ── 1. Open WAL ──────────────────────────────────────────────────────────
	if err := os.MkdirAll(filepath.Dir(walPath), 0755); err != nil {
		log.Fatalf("[startup] create WAL dir: %v", err)
	}
	w, err := wal.Open(walPath)
	if err != nil {
		log.Fatalf("[startup] open WAL: %v", err)
	}
	defer w.Close()

	// ── 2. Replay WAL → restore store ────────────────────────────────────────
	store := storage.New()
	log.Printf("[startup] replaying WAL from %s", walPath)
	if err := wal.Recover(w, store); err != nil {
		log.Fatalf("[startup] WAL recovery failed: %v", err)
	}
	log.Printf("[startup] recovery complete — %d keys restored", store.Len())

	// ── 3. Start background TTL sweeper ──────────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	store.StartSweeper(ctx, sweepInterval)
	log.Printf("[startup] TTL sweeper started (interval=%s)", sweepInterval)

	// ── 4. Start TCP server ───────────────────────────────────────────────────
	server := tcp.New(addr, store, w)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-quit
		log.Println("[server] shutdown signal received")
		cancel()          // stop sweeper
		server.Shutdown() // stop accepting new connections
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
