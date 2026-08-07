// Package cluster manages the set of known nodes and routes keys to them
// using a consistent hash ring.
//
// Phase 6 adds failure detection: a background goroutine probes every
// known node with PING on a configurable interval. Nodes that miss
// DefaultMissThreshold consecutive probes are removed from the ring.
// When they recover and respond again, they are re-added automatically.
package cluster

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/Lenecplusultra/distributed-kv-store/internal/hashing"
)

const defaultProbeTimeout = time.Second

// Cluster is a thread-safe node registry backed by a consistent hash ring
// and a health tracker.
type Cluster struct {
	mu     sync.RWMutex
	ring   *hashing.Ring
	health *HealthTracker
}

// New creates an empty Cluster.
func New() *Cluster {
	return &Cluster{
		ring:   hashing.New(hashing.DefaultReplicas),
		health: newHealthTracker(DefaultMissThreshold),
	}
}

// NewFromAddrs creates a Cluster pre-populated with the given addresses.
func NewFromAddrs(addrs []string) (*Cluster, error) {
	c := New()
	for _, addr := range addrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		if err := c.AddNode(addr); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// AddNode registers addr with the ring and health tracker.
func (c *Cluster) AddNode(addr string) error {
	if addr == "" {
		return fmt.Errorf("cluster: node address cannot be empty")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ring.AddNode(addr)
	c.health.register(addr)
	return nil
}

// RemoveNode deregisters addr from both the ring and health tracker.
func (c *Cluster) RemoveNode(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ring.RemoveNode(addr)
	c.health.deregister(addr)
}

// Route returns the address of the node responsible for key.
// Returns ("", false) if no live nodes are registered.
func (c *Cluster) Route(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ring.GetNode(key)
}

// RouteN returns up to n node addresses for key, walking the ring clockwise.
func (c *Cluster) RouteN(key string, n int) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ring.GetN(key, n)
}

// Nodes returns all addresses currently in the ring (alive nodes only).
func (c *Cluster) Nodes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ring.Nodes()
}

// Len returns the number of live nodes in the ring.
func (c *Cluster) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ring.Len()
}

// IsEmpty reports whether the ring has no live nodes.
func (c *Cluster) IsEmpty() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ring.IsEmpty()
}

// Health returns a snapshot of all tracked node health states.
func (c *Cluster) Health() map[string]NodeStatus {
	return c.health.Status()
}

// StartFailureDetector launches a background goroutine that probes every
// known node (alive and dead) with PING every interval.
//
// On success: Beat() resets the miss count.
//   - If the node was dead, it is re-added to the ring.
//
// On failure: Miss() increments the consecutive miss count.
//   - If it crosses DefaultMissThreshold, the node is removed from the ring.
//
// Dead nodes are still probed so we can detect recovery.
// The goroutine stops when ctx is cancelled.
func (c *Cluster) StartFailureDetector(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c.probeAll()
			case <-ctx.Done():
				return
			}
		}
	}()
}

// probeAll sends a PING to every known node concurrently.
func (c *Cluster) probeAll() {
	nodes := c.health.AllNodes()
	for _, addr := range nodes {
		go c.probeNode(addr)
	}
}

// probeNode sends one PING to addr and updates the health tracker.
// The two phases (health update, then ring update) are deliberately
// NOT held under a single lock to keep the critical section small.
// The health tracker has its own lock; the ring has the cluster mu.
func (c *Cluster) probeNode(addr string) {
	err := pingNode(addr, defaultProbeTimeout)

	if err == nil {
		// Successful probe — reset miss count.
		revived := c.health.Beat(addr)
		if revived {
			log.Printf("[cluster] node %s revived — re-adding to ring", addr)
			c.mu.Lock()
			c.ring.AddNode(addr)
			c.mu.Unlock()
		}
		return
	}

	// Failed probe — increment miss count.
	justDied := c.health.Miss(addr)
	if justDied {
		log.Printf("[cluster] node %s missed %d consecutive heartbeats — removing from ring",
			addr, DefaultMissThreshold)
		c.mu.Lock()
		c.ring.RemoveNode(addr)
		c.mu.Unlock()
	}
}

// pingNode dials addr, sends PING, and verifies a PONG response.
// Returns nil on success, error on any failure.
func pingNode(addr string, timeout time.Duration) error {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(timeout))

	fmt.Fprintf(conn, "PING\n")
	resp, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return fmt.Errorf("read from %s: %w", addr, err)
	}
	if !strings.HasPrefix(resp, "+PONG") {
		return fmt.Errorf("unexpected response from %s: %q", addr, resp)
	}
	return nil
}
