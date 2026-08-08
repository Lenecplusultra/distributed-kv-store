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
// replication completes, that write is lost on the replicas. Redis makes
// the same tradeoff in its default configuration.
//
// # Retry policy
//
// Each replica gets its own goroutine. Failed sends are retried with
// exponential backoff up to maxRetries times. After that, the write
// is dropped and the replica is left stale. Recovery requires a full
// manual resync — there is no replication offset tracking.
//
// # Known ordering hazard
//
// Replicate spawns a goroutine per replica per command, and each goroutine
// runs its own independent retry timers. Two rapid writes to the same key
// can therefore land out of order: if the first send needs a backoff retry
// and the second succeeds immediately, the replica ends up holding the older
// value. This is write divergence, not merely replication lag. The fix is a
// persistent per-replica goroutine draining an ordered channel, which is
// deferred and tracked separately.
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

	// replicaHello identifies this connection to the receiving node as
	// replication traffic rather than client traffic, so the receiver's rate
	// limiter exempts it.
	//
	// Without this, enabling RATE_LIMIT on a node that also serves as a
	// replica would throttle inbound replication. Throttled writes burn all
	// three retries and are then dropped permanently — silent divergence
	// with no error surfaced to any client.
	//
	// Cost: one extra request/response round trip per replicated command,
	// because the connection is not reused. Connection pooling in Phase 8
	// amortises the handshake across many commands.
	//
	// This is forgeable — any client can claim the replica prefix. It is a
	// trusted-network assumption, consistent with the fact that the
	// replication path has no authentication at all.
	replicaHello = "HELLO replica:primary"
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

// send opens a TCP connection to addr, identifies as a replica, writes cmd,
// and reads one response. The connection is closed after each command —
// simple and correct for now. Connection pooling is a Phase 8 optimisation.
func (r *Replicator) send(addr, cmd string) error {
	conn, err := net.DialTimeout("tcp", addr, r.timeout)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(r.timeout))

	reader := bufio.NewReader(conn)

	// Identify as replication traffic before sending the write.
	if _, err := fmt.Fprintf(conn, "%s\n", replicaHello); err != nil {
		return fmt.Errorf("handshake write to %s: %w", addr, err)
	}
	helloResp, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("handshake read from %s: %w", addr, err)
	}
	if len(helloResp) > 0 && helloResp[0] == '-' {
		// An older node that predates HELLO will reject it as an unknown
		// command. That is not fatal — the write below still applies. It
		// only means this connection is subject to that node's rate limiter,
		// if it has one.
		log.Printf("[replication] %s rejected handshake (%s) — proceeding unexempted",
			addr, helloResp[:len(helloResp)-1])
	}

	if _, err := fmt.Fprintf(conn, "%s\n", cmd); err != nil {
		return fmt.Errorf("write to %s: %w", addr, err)
	}

	resp, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read from %s: %w", addr, err)
	}
	if len(resp) > 0 && resp[0] == '-' {
		return fmt.Errorf("replica %s returned error: %s", addr, resp)
	}
	return nil
}
