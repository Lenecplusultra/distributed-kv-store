// Package replication implements async write propagation from a primary
// node to its replicas.
//
// # Model
//
// Asynchronous: the primary writes to its local store and WAL, returns +OK
// to the client immediately, then replicates in the background. The client
// does not wait for replicas to confirm.
//
// # Ordering (Phase 8a)
//
// Each replica has one persistent goroutine draining an ordered channel, so
// writes reach a given replica in the order the primary accepted them.
//
// The previous design spawned a goroutine per replica per command, each with
// its own retry timers. Two rapid writes to the same key could land out of
// order: if the first needed a backoff retry and the second succeeded
// immediately, the replica kept the older value. That is write divergence,
// not replication lag — no amount of waiting repairs it.
//
// Retries now run inside the replica's own goroutine, so a backoff holds
// that replica's queue instead of letting the next write overtake it.
// Ordering follows from the structure rather than from timing.
//
// # Persistent connections (Phase 8b)
//
// Each worker holds one long-lived connection to its replica rather than
// dialing per write. Because exactly one goroutine ever writes to a given
// replica, that connection has a single owner and needs no pool — the
// concurrency shape that makes internal/pool necessary for clients simply
// does not arise here.
//
// This also amortises the HELLO handshake: it is sent once when the
// connection is established, not once per write. Under the previous design
// every replicated command cost a dial, a handshake round trip, and a write
// round trip; it now costs one write round trip.
//
// Staleness is handled the same way as in internal/pool — used
// optimistically, and on transport failure the connection is dropped and
// redialed on the next attempt, which the existing retry loop already
// provides for free.
//
// # Backpressure, and why blocking beats dropping
//
// Queues are bounded. When a replica's queue fills, Replicate blocks the
// calling client write until space frees.
//
// Blocking is chosen because this system has no replication offset tracking:
// a dropped write is unrecoverable and, worse, undetectable. A replica that
// silently missed write #4000 is indistinguishable from one that is merely
// behind. Redis can afford to drop — it kills a replica connection that
// exceeds its output buffer limit, and the replica full-resyncs on reconnect
// — but that recovery path does not exist here.
//
// The cost is real: blocking converts a durability problem into an
// availability problem, and one dead replica would otherwise stall the
// primary's write path for every client.
//
// # The escape hatch
//
// That stall is bounded by failure detection, reusing cluster.HealthTracker
// — the same component and the same threshold that decides whether a node
// stays in the hash ring. The signal differs: the ring's detector feeds it
// PING outcomes, while replication feeds it real delivery outcomes, which is
// the more direct evidence of whether writes can land.
//
// After DefaultMissThreshold consecutive delivery failures a replica is
// declared dead. Its queue then drains without blocking and writes are
// dropped — each one logged individually, so the divergence is announced
// rather than silent. A background PING returns it to service.
//
// So: block while the replica is plausibly alive, drop once it is formally
// dead. Bounded degradation instead of an unbounded stall. A revived replica
// is stale and needs a resync; that is logged too.
//
// # Why EXAT instead of EX for TTL replication
//
// The primary converts client-facing EX <seconds> to an absolute nanosecond
// timestamp before replicating. Sending EX 60 after two seconds of lag would
// give the replica 60 more seconds instead of the remaining 58. Absolute
// timestamps survive the delay.
package replication

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/Lenecplusultra/distributed-kv-store/internal/cluster"
)

const (
	DefaultMaxRetries = 3
	DefaultTimeout    = 2 * time.Second

	// DefaultQueueSize bounds pending writes per replica. Large enough to
	// absorb a brief stall, small enough that a genuinely dead replica
	// reaches the death threshold rather than buffering unboundedly.
	DefaultQueueSize = 1000

	// DefaultProbeInterval is how often a dead replica is probed for revival.
	DefaultProbeInterval = time.Second

	// replicaHello identifies this connection to the receiving node as
	// replication traffic, so the receiver's rate limiter exempts it.
	// Without it, enabling RATE_LIMIT on a replica would throttle inbound
	// replication and drop writes.
	//
	// Sent once per connection rather than once per write, which is the
	// point of holding the connection open.
	//
	// Forgeable by any client — a trusted-network assumption, consistent
	// with the replication path having no authentication at all.
	replicaHello = "HELLO replica:primary"
)

// replicaConn is a worker's connection to its replica, with the buffered
// reader bound to it. The reader must travel with the connection: bufio may
// hold buffered bytes, and a fresh reader would discard them and desync the
// protocol stream.
type replicaConn struct {
	nc     net.Conn
	reader *bufio.Reader
}

func (c *replicaConn) close() {
	if c != nil && c.nc != nil {
		c.nc.Close()
	}
}

// Replicator sends write commands to a fixed set of replica addresses,
// preserving per-replica ordering.
type Replicator struct {
	replicas []string
	queues   map[string]chan string
	health   *cluster.HealthTracker

	maxRetries    int
	timeout       time.Duration
	probeInterval time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New creates a Replicator targeting the given replica addresses and starts
// one worker goroutine per replica. Call Close to stop them.
func New(replicas []string) *Replicator {
	ctx, cancel := context.WithCancel(context.Background())

	r := &Replicator{
		replicas: replicas,
		queues:   make(map[string]chan string, len(replicas)),
		// The replica set is fixed at construction, so every address is
		// registered up front and none are ever added or removed.
		health:        cluster.NewHealthTracker(cluster.DefaultMissThreshold, replicas...),
		maxRetries:    DefaultMaxRetries,
		timeout:       DefaultTimeout,
		probeInterval: DefaultProbeInterval,
		ctx:           ctx,
		cancel:        cancel,
	}

	for _, addr := range replicas {
		q := make(chan string, DefaultQueueSize)
		r.queues[addr] = q

		r.wg.Add(1)
		go func(addr string, q chan string) {
			defer r.wg.Done()
			r.worker(addr, q)
		}(addr, q)
	}

	return r
}

// Replicas returns the configured replica addresses.
func (r *Replicator) Replicas() []string { return r.replicas }

// Health returns a snapshot of replica delivery health, for observability.
func (r *Replicator) Health() map[string]cluster.NodeStatus {
	return r.health.Status()
}

// Replicate enqueues cmd for every replica.
//
// Normally returns immediately. If a replica's queue is full, this blocks
// until that replica drains or is declared dead — see the package comment.
//
// cmd should be a fully-formed protocol command:
//
//	"SET key value"
//	"SET key value EXAT 1753000000000000000"
//	"DEL key"
func (r *Replicator) Replicate(cmd string) {
	for _, addr := range r.replicas {
		select {
		case r.queues[addr] <- cmd:
		case <-r.ctx.Done():
			// Shutting down — stop enqueueing rather than blocking forever
			// on a queue whose worker has already exited.
			return
		}
	}
}

// Close stops all replica workers and waits for them to exit.
// Queued writes are abandoned.
func (r *Replicator) Close() {
	r.cancel()
	r.wg.Wait()
}

// worker drains one replica's queue sequentially, over one long-lived
// connection. Being the only goroutine sending to this replica is what makes
// delivery order match enqueue order, and what makes a single connection
// sufficient.
func (r *Replicator) worker(addr string, q chan string) {
	probe := time.NewTicker(r.probeInterval)
	defer probe.Stop()

	// Owned solely by this goroutine. nil means "not currently connected";
	// the next delivery attempt will dial.
	var rc *replicaConn
	defer func() { rc.close() }()

	for {
		select {
		case <-r.ctx.Done():
			return

		case <-probe.C:
			// Only meaningful while dead. Revival is checked with a cheap
			// PING on a throwaway connection rather than by attempting a
			// real write, so a dead replica's queue never stalls on dial
			// timeouts.
			if r.health.IsAlive(addr) {
				continue
			}
			if err := ping(addr, r.timeout); err == nil {
				if r.health.Beat(addr) {
					log.Printf("[replication] replica %s revived — resuming delivery "+
						"(it is stale and needs a resync)", addr)
				}
			}

		case cmd := <-q:
			if !r.health.IsAlive(addr) {
				// Drop, but never silently. Each dropped write is a known
				// divergence — which is exactly what we refuse to do while
				// the replica is merely slow.
				log.Printf("[replication] replica %s is dead — dropping %q", addr, cmd)
				continue
			}

			if err := r.sendWithRetry(addr, cmd, &rc); err != nil {
				log.Printf("[replication] delivery to %s failed: %v", addr, err)

				if r.health.Miss(addr) {
					log.Printf("[replication] replica %s declared dead after %d consecutive "+
						"delivery failures — queue will drain without blocking, writes dropped",
						addr, cluster.DefaultMissThreshold)
				}
				continue
			}

			r.health.Beat(addr)
		}
	}
}

// sendWithRetry attempts delivery with exponential backoff, reusing the
// worker's connection and redialing whenever it has been dropped.
//
// Runs inside the replica's worker, so retries hold that replica's queue and
// cannot be overtaken by a later write.
func (r *Replicator) sendWithRetry(addr, cmd string, rc **replicaConn) error {
	backoff := 100 * time.Millisecond
	var lastErr error

	for attempt := 1; attempt <= r.maxRetries; attempt++ {
		if err := r.send(addr, cmd, rc); err == nil {
			return nil
		} else {
			lastErr = err
		}

		if attempt < r.maxRetries {
			select {
			case <-time.After(backoff):
			case <-r.ctx.Done():
				return fmt.Errorf("shutting down: %w", lastErr)
			}
			backoff *= 2 // 100ms → 200ms → 400ms
		}
	}
	return lastErr
}

// send delivers one command over the worker's connection, establishing it
// first if necessary. On transport failure the connection is closed and
// cleared so the next attempt redials; on a protocol-level error response
// the connection is healthy and kept.
func (r *Replicator) send(addr, cmd string, rc **replicaConn) error {
	if *rc == nil {
		c, err := r.connect(addr)
		if err != nil {
			return err
		}
		*rc = c
	}

	c := *rc

	if err := c.nc.SetDeadline(time.Now().Add(r.timeout)); err != nil {
		c.close()
		*rc = nil
		return err
	}

	if _, err := fmt.Fprintf(c.nc, "%s\n", cmd); err != nil {
		c.close()
		*rc = nil
		return fmt.Errorf("write to %s: %w", addr, err)
	}

	resp, err := c.reader.ReadString('\n')
	if err != nil {
		c.close()
		*rc = nil
		return fmt.Errorf("read from %s: %w", addr, err)
	}

	if strings.HasPrefix(resp, "-") {
		// The replica answered and rejected the command. The connection is
		// fine; the command is not. Retrying would get the same answer.
		return fmt.Errorf("replica %s returned error: %s", addr, strings.TrimSpace(resp))
	}
	return nil
}

// connect dials the replica and performs the HELLO handshake once for the
// lifetime of the connection.
func (r *Replicator) connect(addr string) (*replicaConn, error) {
	nc, err := net.DialTimeout("tcp", addr, r.timeout)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	c := &replicaConn{nc: nc, reader: bufio.NewReader(nc)}

	if err := nc.SetDeadline(time.Now().Add(r.timeout)); err != nil {
		c.close()
		return nil, err
	}
	if _, err := fmt.Fprintf(nc, "%s\n", replicaHello); err != nil {
		c.close()
		return nil, fmt.Errorf("handshake write to %s: %w", addr, err)
	}
	resp, err := c.reader.ReadString('\n')
	if err != nil {
		c.close()
		return nil, fmt.Errorf("handshake read from %s: %w", addr, err)
	}
	if strings.HasPrefix(resp, "-") {
		// A node predating HELLO rejects it as unknown. Not fatal — writes
		// still apply; this connection is simply subject to that node's rate
		// limiter, if it has one.
		log.Printf("[replication] %s rejected handshake — proceeding unexempted", addr)
	}

	return c, nil
}

// ping checks whether a dead replica has come back, without sending a write.
// Uses its own short-lived connection so it never disturbs worker state.
func ping(addr string, timeout time.Duration) error {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(timeout))

	if _, err := fmt.Fprintf(conn, "PING\n"); err != nil {
		return err
	}
	resp, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return err
	}
	if !strings.HasPrefix(resp, "+PONG") {
		return fmt.Errorf("unexpected response from %s: %q", addr, strings.TrimSpace(resp))
	}
	return nil
}
