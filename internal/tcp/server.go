// Package tcp implements the TCP server that accepts client connections
// and dispatches parsed commands to the storage layer.
//
// Phase 5 additions:
//   - EXAT <nanoseconds> modifier on SET — absolute expiry timestamp
//     used by the replication layer so replicas get the correct deadline
//   - After every mutating command, trigger async replication if a
//     Replicator is configured
//
// Phase 6 additions:
//   - listener guarded by a mutex, closing the race between Start writing
//     it and Shutdown reading it
//
// Phase 7 additions:
//   - optional per-client token-bucket rate limiting
//   - HELLO <clientID> handshake, setting per-connection rate-limit identity
package tcp

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Lenecplusultra/distributed-kv-store/internal/protocol"
	"github.com/Lenecplusultra/distributed-kv-store/internal/ratelimiter"
	"github.com/Lenecplusultra/distributed-kv-store/internal/replication"
	"github.com/Lenecplusultra/distributed-kv-store/internal/storage"
	"github.com/Lenecplusultra/distributed-kv-store/internal/wal"
)

// replicaIDPrefix marks a connection as carrying replication traffic rather
// than client traffic. Connections identifying with this prefix bypass the
// rate limiter.
//
// This is a trusted-network assumption, and a forgeable one: any client can
// send "HELLO replica:anything" and escape throttling. It is accepted here
// for the same reason the replication path has no authentication at all —
// the cluster is assumed to run on a trusted network. Making it real means
// a separate replication listener or mutual auth, neither of which is in
// scope for this project.
//
// Without this exemption, enabling RATE_LIMIT on a node that also serves as
// a replica would throttle inbound replication, exhaust the replicator's
// three retries, and silently drop the write — divergence with no error
// surfaced anywhere.
const replicaIDPrefix = "replica:"

// Server wraps a TCP listener, the shared store, an optional WAL,
// an optional Replicator, and an optional rate limiter.
type Server struct {
	addr       string
	store      *storage.Store
	wal        *wal.WAL
	replicator *replication.Replicator
	limiter    *ratelimiter.Limiter // nil means unlimited

	mu       sync.Mutex // protects listener
	listener net.Listener
}

// Option configures optional Server behaviour.
//
// The constructor previously took every dependency positionally. Rate
// limiting would have made it a fifth parameter, and every existing call
// site — including three test packages that do not care about limiting —
// would have had to pass an explicit nil. Options keep the required
// dependencies explicit and let optional ones be added without churn.
type Option func(*Server)

// WithLimiter attaches a rate limiter. Passing nil is equivalent to omitting
// the option: limiting stays disabled.
func WithLimiter(l *ratelimiter.Limiter) Option {
	return func(s *Server) { s.limiter = l }
}

// New creates a Server. Pass nil for w or r to disable WAL/replication.
func New(addr string, store *storage.Store, w *wal.WAL, r *replication.Replicator, opts ...Option) *Server {
	s := &Server{addr: addr, store: store, wal: w, replicator: r}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Start begins listening for connections. Blocks until Shutdown is called.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("tcp listen on %s: %w", s.addr, err)
	}

	// Guarded: Shutdown may run on another goroutine and read this field.
	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	log.Printf("[server] listening on %s", s.addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			// Accept fails once the listener is closed — normal shutdown.
			return nil
		}
		go s.handleConn(conn)
	}
}

// Shutdown closes the listener, unblocking Start.
func (s *Server) Shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		s.listener.Close()
	}
}

// remoteHost returns a connection's remote IP without the ephemeral port.
//
// The port is stripped deliberately. Including it would give every new
// connection from the same client a fresh bucket, and since the client opens
// one connection per command, that would defeat the limiter entirely.
func remoteHost(conn net.Conn) string {
	host, _, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		return conn.RemoteAddr().String()
	}
	return host
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	remote := conn.RemoteAddr().String()
	log.Printf("[server] client connected: %s", remote)

	// Rate-limit identity is per-connection state, not per-command. This
	// mirrors how Redis tracks authentication: AUTH and HELLO set state on
	// the client struct, and every later command is evaluated against it.
	//
	// The default is the remote IP, which is correct for direct connections
	// and wrong behind NAT, a load balancer, or Kubernetes ingress — where
	// every client collapses into one bucket. HELLO exists to escape that.
	clientID := remoteHost(conn)

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

		// HELLO rebinds this connection's identity. Handled here rather than
		// in dispatch because dispatch has no access to connection state.
		//
		// It is exempt from rate limiting: if HELLO consumed a token, a
		// client sharing a NAT address with a noisy neighbour could be
		// throttled out of the very command that would move it to its own
		// bucket.
		if cmd.Name == "HELLO" {
			if len(cmd.Args) != 1 {
				fmt.Fprint(conn, protocol.Err("HELLO requires exactly one argument"))
				continue
			}
			clientID = cmd.Args[0]
			log.Printf("[server] %s identified as %q", remote, clientID)
			fmt.Fprint(conn, protocol.OK("OK"))
			continue
		}

		// Rate check sits between parse and dispatch. Parsing first means a
		// throttled client still gets a syntax error for malformed input,
		// which is more useful than a rate-limit error masking a real bug.
		if !s.isExempt(clientID) && !s.limiter.Allow(clientID) {
			fmt.Fprint(conn, protocol.Err("rate limit exceeded"))
			continue
		}

		fmt.Fprint(conn, s.dispatch(cmd))
	}
	log.Printf("[server] client disconnected: %s", remote)
}

// isExempt reports whether a client identity bypasses rate limiting.
func (s *Server) isExempt(clientID string) bool {
	return strings.HasPrefix(clientID, replicaIDPrefix)
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

		// WAL write first — disk before memory.
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
