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
// # Known constraint
//
// Keys and values must not contain spaces. This matches the current wire
// protocol limitation and will be addressed when we move to binary framing.
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
type WAL struct {
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
func (w *WAL) Append(e Entry) error {
	var line string
	switch e.Op {
	case "SET":
		if e.HasTTL {
			line = fmt.Sprintf("SET %s %s EX %d\n", e.Key, e.Value, e.ExpiresAt.UnixNano())
		} else {
			line = fmt.Sprintf("SET %s %s\n", e.Key, e.Value)
		}
	case "DEL":
		line = fmt.Sprintf("DEL %s\n", e.Key)
	default:
		return fmt.Errorf("wal: unknown op %q", e.Op)
	}

	if _, err := fmt.Fprint(w.writer, line); err != nil {
		return fmt.Errorf("wal write: %w", err)
	}
	// Flush to OS buffer. Not fsync — see package doc for the tradeoff.
	return w.writer.Flush()
}

// Replay reads every entry from the beginning of the log and calls fn
// for each one. Used during startup to restore in-memory state.
func (w *WAL) Replay(fn func(Entry)) error {
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
	if err := w.writer.Flush(); err != nil {
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
