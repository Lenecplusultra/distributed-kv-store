// Package pool provides pooled, reusable TCP connections to store nodes.
//
// # The problem
//
// Every command previously cost a fresh TCP connection: dial, three-way
// handshake, one request, one response, close. On loopback that overhead is
// small; across a real network it dominates, and a benchmark against it
// measures net.Dial rather than the store.
//
// # Staleness, and why we do not validate on checkout
//
// A pooled connection can die while idle — the server restarted, an idle
// timeout fired, a NAT mapping expired. Nothing notifies us; the failure
// only surfaces on the next write or read.
//
// The alternative to detecting it lazily is validating on checkout: PING,
// confirm +PONG, then hand the connection over. That doubles the round trips
// on every command to defend against something that happens rarely — 100%
// overhead on the hot path for an occasional retry. So connections are used
// optimistically and retried once on failure. This is what Redis clients and
// HTTP keep-alive both do.
//
// Two rules make that correct rather than merely fast:
//
//   - Only *reused* connections are retried. A freshly dialed connection
//     that fails means the node is genuinely down, and retrying is noise.
//   - Only transport failures are retried. A "-ERR key not found" response
//     is a real answer; retrying it would return the same answer while
//     hiding the first one.
//
// # Idempotency caveat
//
// Retrying means a command whose request reached the server but whose
// response was lost is sent twice. SET and DEL are idempotent, so this is
// harmless for the current protocol. It would not be safe for a counter
// increment or an append, and that is a property of the commands, not of
// the pool.
//
// # One owner per connection
//
// The wire protocol is strictly request/response with no request IDs, so
// responses are matched to requests by arrival order alone. A pooled
// connection must therefore be held exclusively for the duration of a
// command — two concurrent callers on one socket would mismatch replies.
// This also rules out pipelining until the protocol grows IDs.
package pool

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	// DefaultMaxIdlePerAddr caps idle connections retained per node.
	// Beyond this, connections are closed on return rather than pooled.
	DefaultMaxIdlePerAddr = 8

	// DefaultDialTimeout bounds connection establishment.
	DefaultDialTimeout = 2 * time.Second

	// DefaultIOTimeout bounds a single request/response exchange.
	DefaultIOTimeout = 2 * time.Second

	// DefaultMaxIdleTime discards connections idle longer than this without
	// trying them. Cheap insurance against the most common staleness cause;
	// it reduces retries but does not replace them.
	DefaultMaxIdleTime = 30 * time.Second
)

// conn is a pooled connection plus the buffered reader bound to it.
//
// The reader must travel with the connection: bufio may hold buffered bytes,
// and creating a fresh reader per command would discard them and desync the
// protocol stream.
type conn struct {
	nc       net.Conn
	reader   *bufio.Reader
	idleFrom time.Time
}

// Pool holds idle connections keyed by node address.
type Pool struct {
	mu   sync.Mutex
	idle map[string][]*conn

	maxIdlePerAddr int
	dialTimeout    time.Duration
	ioTimeout      time.Duration
	maxIdleTime    time.Duration

	closed bool
}

// New creates an empty Pool with default settings.
func New() *Pool {
	return &Pool{
		idle:           make(map[string][]*conn),
		maxIdlePerAddr: DefaultMaxIdlePerAddr,
		dialTimeout:    DefaultDialTimeout,
		ioTimeout:      DefaultIOTimeout,
		maxIdleTime:    DefaultMaxIdleTime,
	}
}

// Do sends cmd to addr and returns the response line, including its trailing
// newline and protocol prefix.
//
// A pooled connection is tried first. If it fails at the transport level,
// the command is retried exactly once on a freshly dialed connection. A
// protocol-level error response ("-ERR ...") is returned to the caller as a
// successful exchange — it is an answer, not a failure.
func (p *Pool) Do(addr, cmd string) (string, error) {
	c, reused, err := p.get(addr)
	if err != nil {
		return "", err
	}

	resp, err := p.exchange(c, cmd)
	if err == nil {
		p.put(addr, c)
		return resp, nil
	}

	// This connection is unusable either way.
	c.nc.Close()

	if !reused {
		// Freshly dialed and it still failed — the node is down.
		return "", err
	}

	// The connection came from the pool, so the likeliest explanation is
	// staleness rather than an unhealthy node. One retry, dialed fresh.
	fresh, err2 := p.dial(addr)
	if err2 != nil {
		return "", fmt.Errorf("retry dial after stale connection (%v): %w", err, err2)
	}

	resp, err2 = p.exchange(fresh, cmd)
	if err2 != nil {
		fresh.nc.Close()
		return "", fmt.Errorf("retry after stale connection (%v): %w", err, err2)
	}

	p.put(addr, fresh)
	return resp, nil
}

// exchange writes one command and reads one response line.
func (p *Pool) exchange(c *conn, cmd string) (string, error) {
	if err := c.nc.SetDeadline(time.Now().Add(p.ioTimeout)); err != nil {
		return "", err
	}
	if _, err := fmt.Fprintf(c.nc, "%s\n", cmd); err != nil {
		return "", fmt.Errorf("write: %w", err)
	}
	resp, err := c.reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("read: %w", err)
	}
	return resp, nil
}

// get returns a pooled connection if one is available, otherwise dials.
// reused reports which, so Do knows whether a retry is warranted.
func (p *Pool) get(addr string) (c *conn, reused bool, err error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, false, fmt.Errorf("pool: closed")
	}

	queue := p.idle[addr]
	cutoff := time.Now().Add(-p.maxIdleTime)

	for len(queue) > 0 {
		// Take from the end: most recently returned is least likely stale.
		candidate := queue[len(queue)-1]
		queue = queue[:len(queue)-1]

		if candidate.idleFrom.Before(cutoff) {
			candidate.nc.Close()
			continue
		}

		p.idle[addr] = queue
		p.mu.Unlock()
		return candidate, true, nil
	}

	p.idle[addr] = queue
	p.mu.Unlock()

	c, err = p.dial(addr)
	return c, false, err
}

// dial opens a new connection to addr.
func (p *Pool) dial(addr string) (*conn, error) {
	nc, err := net.DialTimeout("tcp", addr, p.dialTimeout)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return &conn{nc: nc, reader: bufio.NewReader(nc)}, nil
}

// put returns a healthy connection to the pool, or closes it if the pool is
// full or closed.
func (p *Pool) put(addr string, c *conn) {
	c.idleFrom = time.Now()

	p.mu.Lock()
	if p.closed || len(p.idle[addr]) >= p.maxIdlePerAddr {
		p.mu.Unlock()
		c.nc.Close()
		return
	}
	p.idle[addr] = append(p.idle[addr], c)
	p.mu.Unlock()
}

// IdleCount reports how many idle connections are pooled for addr.
// Used by tests and by the Phase 8c metrics layer.
func (p *Pool) IdleCount(addr string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.idle[addr])
}

// Close discards every pooled connection. Connections currently checked out
// are not affected; they close when their caller finishes.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true
	for addr, queue := range p.idle {
		for _, c := range queue {
			c.nc.Close()
		}
		delete(p.idle, addr)
	}
}
