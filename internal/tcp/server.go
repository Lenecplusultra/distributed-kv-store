// Package tcp implements the TCP server that accepts client connections
// and dispatches parsed commands to the storage layer.
//
// Each client connection gets its own goroutine. The store handles
// its own locking, so goroutines can issue reads/writes concurrently
// without any coordination at this layer.
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
)

// Server wraps a TCP listener and the shared store.
type Server struct {
	addr     string
	store    *storage.Store
	listener net.Listener
}

// New creates a Server bound to addr using the given store.
func New(addr string, store *storage.Store) *Server {
	return &Server{addr: addr, store: store}
}

// Start begins listening for connections. Blocks until the listener
// is closed (e.g. via Shutdown). Each accepted connection is handled
// in its own goroutine.
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
			// Listener was closed — clean shutdown.
			return nil
		}
		go s.handleConn(conn)
	}
}

// Shutdown closes the listener, causing Start to return.
func (s *Server) Shutdown() {
	if s.listener != nil {
		s.listener.Close()
	}
}

// handleConn runs in its own goroutine for each client.
// It reads newline-delimited commands, dispatches them, and writes responses.
//
// Using bufio.Reader here is a deliberate choice: it buffers reads from
// the socket so we issue one syscall per line rather than one syscall per
// byte. At high connection counts this matters significantly.
func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	remote := conn.RemoteAddr().String()
	log.Printf("[server] client connected: %s", remote)

	reader := bufio.NewReader(conn)

	for {
		// ReadString blocks until '\n' or EOF/error.
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

		response := s.dispatch(cmd)
		fmt.Fprint(conn, response)
	}

	log.Printf("[server] client disconnected: %s", remote)
}

// dispatch routes a parsed command to the appropriate store operation
// and returns the formatted response string.
//
// This is the single place where command names are mapped to behavior.
// Adding a new command means adding a case here and in the protocol docs.
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

		// Check for optional EX <seconds> suffix
		if len(cmd.Args) >= 4 && strings.ToUpper(cmd.Args[2]) == "EX" {
			secs, err := strconv.Atoi(cmd.Args[3])
			if err != nil || secs <= 0 {
				return protocol.Err("EX requires a positive integer")
			}
			s.store.SetWithTTL(key, value, time.Duration(secs)*time.Second)
			return protocol.OK("OK")
		}

		s.store.Set(key, value)
		return protocol.OK("OK")

	case "GET":
		// GET <key>
		if len(cmd.Args) < 1 {
			return protocol.Err("GET requires a key")
		}
		val, ok := s.store.Get(cmd.Args[0])
		if !ok {
			return protocol.Err("key not found")
		}
		return protocol.OK(val)

	case "DEL":
		// DEL <key>
		if len(cmd.Args) < 1 {
			return protocol.Err("DEL requires a key")
		}
		existed := s.store.Delete(cmd.Args[0])
		if !existed {
			return protocol.Err("key not found")
		}
		return protocol.OK("OK")

	default:
		return protocol.Err(fmt.Sprintf("unknown command '%s'", cmd.Name))
	}
}
