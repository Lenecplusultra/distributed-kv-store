// Package wal implements a write-ahead log for crash recovery.
//
// Every mutation (SET, DEL) is appended to a flat file before being
// applied to the in-memory store. On restart, Recover() replays the
// log and restores the store to its pre-crash state.
//
// # File format
//
// One entry per line:
//
//	SET <key> <value>
//	SET <key> <value> EX <unix_nano>
//	DEL <key>
//
// The EX field stores an absolute Unix nanosecond timestamp — not a
// relative duration. This is critical: if we stored "EX 60" and replayed
// after 30 seconds, the key would get 60 more seconds instead of 30.
// Absolute timestamps survive restarts correctly.
//
// (The wire protocol calls the absolute form EXAT and reserves EX for
// relative seconds. The WAL predates that split and uses the EX label for
// what is really an EXAT value. The two are never exchanged, so this is a
// naming inconsistency rather than a correctness bug — but it is the kind
// of thing worth renaming before someone reads the file and misinterprets
// it.)
//
// # Concurrency
//
// A WAL is safe for concurrent use. Every mutating operation holds a mutex.
//
// This was not always true, and the way it failed is instructive. The server
// runs one goroutine per connection, so Append was being called concurrently
// by every client at once. bufio.Writer is not safe for concurrent use:
// simultaneous writers corrupt its internal offset, the length check fails,
// and io.ErrShortWrite comes back. Worse, bufio latches that error and
// returns it on every subsequent call forever, so a single racing moment
// turned the node permanently write-rejecting while it continued to accept
// connections and serve reads.
//
// The tests missed it because wal_test.go exercised Append single-threaded
// and every TCP server test passed a nil WAL — so the one path where
// concurrency met the WAL had no coverage at all. A 50-client benchmark
// found it in ten seconds.
//
// # Durability tradeoff
//
// We flush the bufio.Writer after every append (OS buffer write) but do
// not call fsync (disk write). This means a power failure between flush
// and disk commit can lose the last entry. fsync would guarantee disk
// durability but costs ~1ms per write, capping throughput at ~1000 ops/s.
// Redis exposes this as appendfsync: always / everysec / no.
// For Phase 2 we accept the flush-only guarantee.
package wal

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Lenecplusultra/distributed-kv-store/internal/storage"
)

// Entry is a single WAL record.
type Entry struct {
	Op        string // "SET" or "DEL"
	Key       string
	Value     string    // empty for DEL
	ExpiresAt time.Time // absolute expiry; only meaningful when HasTTL is true
	HasTTL    bool
}

// WAL wraps an append-only file.
//
// Safe for concurrent use: mu guards both the file handle and the buffered
// writer, neither of which tolerates concurrent access on its own.
type WAL struct {
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer
}

// Open opens the WAL file at path, creating it if it does not exist.
// The file is opened for both reading (replay) and appending (writes).
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal open %s: %w", path, err)
	}
	return &WAL{
		file:   f,
		writer: bufio.NewWriter(f),
	}, nil
}

// Append encodes entry and writes it to the log.
// Must be called BEFORE the corresponding in-memory write.
// If the process crashes after Append but before the store write,
// replay will re-apply the entry — idempotent for SET, safe for DEL.
//
// Safe to call from multiple goroutines.
func (w *WAL) Append(e Entry) error {
	line, err := encode(e)
	if err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.writer.WriteString(line); err != nil {
		w.resetWriterLocked()
		return fmt.Errorf("wal write: %w", err)
	}

	// Flush to OS buffer. Not fsync — see package doc for the tradeoff.
	if err := w.writer.Flush(); err != nil {
		w.resetWriterLocked()
		return fmt.Errorf("wal flush: %w", err)
	}
	return nil
}

// resetWriterLocked discards the buffered writer's state, including any
// latched error, and rebinds it to the file.
//
// bufio.Writer never clears an error on its own: once one write fails, every
// subsequent call returns that same error indefinitely. Without this reset a
// single transient failure would leave the node permanently unable to accept
// writes while still accepting connections, serving reads, and reporting
// itself healthy — a silent brick rather than an honest crash.
//
// Any bytes still buffered at the point of failure are discarded. That is
// correct for a write-ahead log: the caller is told the append failed, so
// those bytes must not later appear in the file as though they had
// succeeded. The caller must hold w.mu.
func (w *WAL) resetWriterLocked() {
	w.writer.Reset(w.file)
}

// encode renders an entry as a single log line.
func encode(e Entry) (string, error) {
	switch e.Op {
	case "SET":
		if e.HasTTL {
			return fmt.Sprintf("SET %s %s EX %d\n", e.Key, e.Value, e.ExpiresAt.UnixNano()), nil
		}
		return fmt.Sprintf("SET %s %s\n", e.Key, e.Value), nil
	case "DEL":
		return fmt.Sprintf("DEL %s\n", e.Key), nil
	default:
		return "", fmt.Errorf("wal: unknown op %q", e.Op)
	}
}

// Replay reads every entry from the beginning of the log and calls fn
// for each one. Used during startup to restore in-memory state.
//
// Holds the mutex for the duration: it seeks the shared file handle, which
// would otherwise move the append cursor under a concurrent writer.
func (w *WAL) Replay(fn func(Entry)) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Flush anything buffered so replay sees a complete file.
	if err := w.writer.Flush(); err != nil {
		w.resetWriterLocked()
		return fmt.Errorf("wal flush before replay: %w", err)
	}

	// Seek to the beginning — the file was opened in append mode so
	// the write cursor is at the end, but we can still read from start.
	if _, err := w.file.Seek(0, 0); err != nil {
		return fmt.Errorf("wal seek: %w", err)
	}

	scanner := bufio.NewScanner(w.file)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		entry, err := parseLine(line)
		if err != nil {
			// Skip malformed lines — could be a torn write from a crash.
			// Log but continue: partial logs are expected after crashes.
			fmt.Fprintf(os.Stderr, "wal: skipping malformed line %d: %v\n", lineNum, err)
			continue
		}
		fn(entry)
	}
	return scanner.Err()
}

// Recover replays the WAL into store, applying entries in order.
// Expired TTL entries are silently skipped — they need not be restored.
// This is the startup sequence: Open → Recover → serve traffic.
func Recover(w *WAL, s *storage.Store) error {
	return w.Replay(func(e Entry) {
		switch e.Op {
		case "SET":
			if e.HasTTL {
				// Only restore if the key hasn't expired yet.
				if time.Now().Before(e.ExpiresAt) {
					s.SetWithExpiry(e.Key, e.Value, e.ExpiresAt)
				}
				// else: expired during downtime — drop it silently.
			} else {
				s.Set(e.Key, e.Value)
			}
		case "DEL":
			s.Delete(e.Key)
		}
	})
}

// Close flushes any buffered data and closes the file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		w.file.Close()
		return err
	}
	return w.file.Close()
}

// parseLine decodes one line from the WAL file into an Entry.
func parseLine(line string) (Entry, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return Entry{}, fmt.Errorf("empty line")
	}
	parts := strings.Fields(line)

	switch parts[0] {
	case "SET":
		// SET key value
		// SET key value EX <nanoseconds>
		if len(parts) < 3 {
			return Entry{}, fmt.Errorf("SET: need key and value, got %d fields", len(parts))
		}
		e := Entry{Op: "SET", Key: parts[1], Value: parts[2]}
		if len(parts) == 5 && parts[3] == "EX" {
			ns, err := strconv.ParseInt(parts[4], 10, 64)
			if err != nil {
				return Entry{}, fmt.Errorf("SET EX: bad nanoseconds %q: %w", parts[4], err)
			}
			e.ExpiresAt = time.Unix(0, ns)
			e.HasTTL = true
		}
		return e, nil

	case "DEL":
		if len(parts) < 2 {
			return Entry{}, fmt.Errorf("DEL: need key")
		}
		return Entry{Op: "DEL", Key: parts[1]}, nil

	default:
		return Entry{}, fmt.Errorf("unknown op %q", parts[0])
	}
}
