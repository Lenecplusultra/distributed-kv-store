// Command server is the entrypoint for the distributed-kv-store node.
//
// It wires together the storage layer and TCP server, starts listening,
// and handles OS signals for graceful shutdown.
package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Lenecplusultra/distributed-kv-store/internal/storage"
	"github.com/Lenecplusultra/distributed-kv-store/internal/tcp"
)

func main() {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":6379"
	}

	store := storage.New()
	server := tcp.New(addr, store)

	// Handle Ctrl+C and SIGTERM for graceful shutdown.
	// This matters in production: you want in-flight requests to finish
	// rather than dropping connections mid-response.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-quit
		log.Println("[server] shutting down...")
		server.Shutdown()
	}()

	log.Printf("[server] distributed-kv-store starting on %s", addr)
	if err := server.Start(); err != nil {
		log.Fatalf("[server] fatal: %v", err)
	}
	log.Println("[server] stopped")
}
