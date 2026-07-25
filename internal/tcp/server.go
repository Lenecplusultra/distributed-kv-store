// Package tcp implements the TCP server that accepts client connections
// and dispatches parsed commands to the storage layer.
//
// Phase 2 addition: every mutating command (SET, DEL) is written to the
// WAL before being applied to the store. If the WAL write fails, the
// command is rejected — better to return an error than to accept a write
// that cannot be recovered after a crash.
package tcp

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/Lenecplusultra/distributed-kv-store/internal/protocol"
	"github.com/Lenecplusultra/distributed-kv-store/internal/storage"
	"github.com/Lenecplusultra/distributed-kv-store/internal/wal"
)

// Server wraps a TCP listener, the shared store, and an optional WAL.
// If wal is nil, writes are applied to the store only (no persistence).
// Passing nil is useful in tests that don't need durability.
type Server struct {
	addr     string
	store    *storage.Store
	wal      *wal.WAL // nil = no persistence
	listener net.Listener
}

// New creates a Server. Pass nil for w to disable WAL persistence.
func New(addr string, store *storage.Store, w *wal.WAL) *Server {
	return &Server{addr: addr, store: store, wal: w}
}

// Start begins listening for connections. Blocks until Shutdown is called.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("tcp listen on %s: %w", s.addr, err)
	}
	s.listener = ln
	log.Printf("[server] listening on %s", s.addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			return nil // listener closed — clean shutdown
		}
		go s.handleConn(conn)
	}
}

// Shutdown closes the listener, unblocking Start.
func (s *Server) Shutdown() {
	if s.listener != nil {
		s.listener.Close()
	}
}

// handleConn runs in its own goroutine for each connected client.
func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	remote := conn.RemoteAddr().String()
	log.Printf("[server] client connected: %s", remote)

	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("[server] read error from %s: %v", remote, err)
			}
			break
		}

		cmd, err := protocol.Parse(line)
		if err != nil {
			fmt.Fprint(conn, protocol.Err("empty command"))
			continue
		}

		fmt.Fprint(conn, s.dispatch(cmd))
	}

	log.Printf("[server] client disconnected: %s", remote)
}

// dispatch routes a command to the store, writing to the WAL first for
// mutating operations. Returns the formatted response string.
func (s *Server) dispatch(cmd *protocol.Command) string {
	switch cmd.Name {

	case "PING":
		return protocol.OK("PONG")

	case "SET":
		// SET <key> <value>
		// SET <key> <value> EX <seconds>
		if len(cmd.Args) < 2 {
			return protocol.Err("SET requires key and value")
		}
		key, value := cmd.Args[0], cmd.Args[1]

		// Build the WAL entry before touching the store.
		// We compute the absolute expiry here so both the WAL entry
		// and the store write use the exact same timestamp.
		entry := wal.Entry{Op: "SET", Key: key, Value: value}

		if len(cmd.Args) >= 4 && strings.ToUpper(cmd.Args[2]) == "EX" {
			secs, err := strconv.Atoi(cmd.Args[3])
			if err != nil || secs <= 0 {
				return protocol.Err("EX requires a positive integer")
			}
			entry.HasTTL = true
			entry.ExpiresAt = time.Now().Add(time.Duration(secs) * time.Second)
		}

		// WAL write first — reject the command if durability fails.
		if s.wal != nil {
			if err := s.wal.Append(entry); err != nil {
				log.Printf("[server] WAL append error: %v", err)
				return protocol.Err("internal error: persistence failed")
			}
		}

		// Apply to in-memory store.
		if entry.HasTTL {
			s.store.SetWithExpiry(key, value, entry.ExpiresAt)
		} else {
			s.store.Set(key, value)
		}
		return protocol.OK("OK")

	case "GET":
		if len(cmd.Args) < 1 {
			return protocol.Err("GET requires a key")
		}
		val, ok := s.store.Get(cmd.Args[0])
		if !ok {
			return protocol.Err("key not found")
		}
		return protocol.OK(val)

	case "DEL":
		if len(cmd.Args) < 1 {
			return protocol.Err("DEL requires a key")
		}

		// WAL write first.
		if s.wal != nil {
			entry := wal.Entry{Op: "DEL", Key: cmd.Args[0]}
			if err := s.wal.Append(entry); err != nil {
				log.Printf("[server] WAL append error: %v", err)
				return protocol.Err("internal error: persistence failed")
			}
		}

		if !s.store.Delete(cmd.Args[0]) {
			return protocol.Err("key not found")
		}
		return protocol.OK("OK")

	default:
		return protocol.Err(fmt.Sprintf("unknown command %q", cmd.Name))
	}
}
