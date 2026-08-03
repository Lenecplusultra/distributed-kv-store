// Package tcp implements the TCP server that accepts client connections
// and dispatches parsed commands to the storage layer.
//
// Phase 5 additions:
//   - EXAT <nanoseconds> modifier on SET — absolute expiry timestamp
//     used by the replication layer so replicas get the correct deadline
//   - After every mutating command, trigger async replication if a
//     Replicator is configured
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
	"github.com/Lenecplusultra/distributed-kv-store/internal/replication"
	"github.com/Lenecplusultra/distributed-kv-store/internal/storage"
	"github.com/Lenecplusultra/distributed-kv-store/internal/wal"
)

// Server wraps a TCP listener, the shared store, an optional WAL,
// and an optional Replicator. All fields are nil-safe.
type Server struct {
	addr       string
	store      *storage.Store
	wal        *wal.WAL
	replicator *replication.Replicator
	listener   net.Listener
}

// New creates a Server. Pass nil for w or r to disable WAL/replication.
func New(addr string, store *storage.Store, w *wal.WAL, r *replication.Replicator) *Server {
	return &Server{addr: addr, store: store, wal: w, replicator: r}
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
			return nil
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

// dispatch routes a command to the store, writing to WAL first for
// mutations, then triggering async replication.
func (s *Server) dispatch(cmd *protocol.Command) string {
	switch cmd.Name {

	case "PING":
		return protocol.OK("PONG")

	case "SET":
		// Accepted forms:
		//   SET <key> <value>
		//   SET <key> <value> EX <seconds>        ← from clients
		//   SET <key> <value> EXAT <nanoseconds>  ← from replication
		if len(cmd.Args) < 2 {
			return protocol.Err("SET requires key and value")
		}
		key, value := cmd.Args[0], cmd.Args[1]

		entry := wal.Entry{Op: "SET", Key: key, Value: value}

		if len(cmd.Args) >= 4 {
			modifier := strings.ToUpper(cmd.Args[2])
			switch modifier {
			case "EX":
				// Client-facing: relative seconds → convert to absolute.
				secs, err := strconv.Atoi(cmd.Args[3])
				if err != nil || secs <= 0 {
					return protocol.Err("EX requires a positive integer")
				}
				entry.HasTTL = true
				entry.ExpiresAt = time.Now().Add(time.Duration(secs) * time.Second)
			case "EXAT":
				// Replication-facing: already an absolute nanosecond timestamp.
				ns, err := strconv.ParseInt(cmd.Args[3], 10, 64)
				if err != nil {
					return protocol.Err("EXAT requires a nanosecond timestamp")
				}
				entry.HasTTL = true
				entry.ExpiresAt = time.Unix(0, ns)
			default:
				return protocol.Err(fmt.Sprintf("unknown SET option %q", cmd.Args[2]))
			}
		}

		// WAL write first.
		if s.wal != nil {
			if err := s.wal.Append(entry); err != nil {
				log.Printf("[server] WAL append error: %v", err)
				return protocol.Err("internal error: persistence failed")
			}
		}

		// Apply to store.
		if entry.HasTTL {
			s.store.SetWithExpiry(key, value, entry.ExpiresAt)
		} else {
			s.store.Set(key, value)
		}

		// Replicate asynchronously.
		// Use EXAT so replicas get the exact expiry, not a fresh EX.
		if s.replicator != nil {
			var replicaCmd string
			if entry.HasTTL {
				replicaCmd = fmt.Sprintf("SET %s %s EXAT %d",
					key, value, entry.ExpiresAt.UnixNano())
			} else {
				replicaCmd = fmt.Sprintf("SET %s %s", key, value)
			}
			s.replicator.Replicate(replicaCmd)
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

		// Replicate the deletion.
		if s.replicator != nil {
			s.replicator.Replicate(fmt.Sprintf("DEL %s", cmd.Args[0]))
		}
		return protocol.OK("OK")

	default:
		return protocol.Err(fmt.Sprintf("unknown command %q", cmd.Name))
	}
}
