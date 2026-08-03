// Package replication implements async write propagation from a primary
// node to its replicas.
//
// # Model
//
// This is asynchronous replication: the primary writes to its local store
// and WAL, returns +OK to the client immediately, then replicates in the
// background. The client never waits for replicas to confirm.
//
// Tradeoff: if the primary crashes after returning +OK but before
// replication completes, that write is lost on the replicas. This is
// the same tradeoff Redis makes with its default replication config.
//
// # Retry policy
//
// Each replica gets its own goroutine. Failed sends are retried with
// exponential backoff up to maxRetries times. After that, the write
// is dropped and the replica is left stale. Recovery requires a full
// resync (planned for Phase 6).
//
// # Why EXAT instead of EX for TTL replication
//
// The primary converts client-facing EX <seconds> to an absolute
// nanosecond timestamp (EXAT) before replicating. If it sent EX 60
// and replication took 2 seconds, the replica would give the key
// 60 more seconds instead of the remaining 58. Absolute timestamps
// survive the replication delay correctly.
package replication

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"time"
)

const (
	DefaultMaxRetries = 3
	DefaultTimeout    = 2 * time.Second
)

// Replicator sends write commands to a fixed set of replica addresses.
type Replicator struct {
	replicas   []string
	maxRetries int
	timeout    time.Duration
}

// New creates a Replicator targeting the given replica addresses.
func New(replicas []string) *Replicator {
	return &Replicator{
		replicas:   replicas,
		maxRetries: DefaultMaxRetries,
		timeout:    DefaultTimeout,
	}
}

// Replicas returns the configured replica addresses.
func (r *Replicator) Replicas() []string { return r.replicas }

// Replicate sends cmd to all replicas asynchronously.
// Returns immediately — each replica gets its own goroutine.
// cmd should be a fully-formed protocol command, e.g.:
//
//	"SET key value"
//	"SET key value EXAT 1753000000000000000"
//	"DEL key"
func (r *Replicator) Replicate(cmd string) {
	for _, addr := range r.replicas {
		go r.sendWithRetry(addr, cmd)
	}
}

// sendWithRetry attempts to deliver cmd to addr with exponential backoff.
// Logs and gives up after maxRetries failures — replica will be stale.
func (r *Replicator) sendWithRetry(addr, cmd string) {
	backoff := 100 * time.Millisecond
	for attempt := 1; attempt <= r.maxRetries; attempt++ {
		if err := r.send(addr, cmd); err == nil {
			return // success
		} else {
			log.Printf("[replication] attempt %d/%d → %s failed: %v",
				attempt, r.maxRetries, addr, err)
		}
		if attempt < r.maxRetries {
			time.Sleep(backoff)
			backoff *= 2 // 100ms → 200ms → 400ms
		}
	}
	log.Printf("[replication] giving up on %s for %q — replica may be stale", addr, cmd)
}

// send opens a TCP connection to addr, writes cmd, and reads one response.
// The connection is closed after each command — simple and correct for
// Phase 5. Connection pooling is a Phase 8 optimisation.
func (r *Replicator) send(addr, cmd string) error {
	conn, err := net.DialTimeout("tcp", addr, r.timeout)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(r.timeout))

	if _, err := fmt.Fprintf(conn, "%s\n", cmd); err != nil {
		return fmt.Errorf("write to %s: %w", addr, err)
	}

	resp, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return fmt.Errorf("read from %s: %w", addr, err)
	}
	if len(resp) > 0 && resp[0] == '-' {
		return fmt.Errorf("replica %s returned error: %s", addr, resp)
	}
	return nil
}
