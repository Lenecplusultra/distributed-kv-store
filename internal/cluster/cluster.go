// Package cluster manages the set of known nodes and routes keys to them
// using a consistent hash ring.
//
// Cluster is the thread-safe public face of the hash ring. The ring itself
// (internal/hashing) is not goroutine-safe — Cluster wraps it with an
// RWMutex so multiple goroutines can route concurrently.
//
// In a smart-client architecture (Phase 4), the CLIENT holds a Cluster
// instance and uses Route() to decide which server to connect to.
// The server has no knowledge of the cluster — it just serves requests.
package cluster

import (
	"fmt"
	"strings"
	"sync"

	"github.com/Lenecplusultra/distributed-kv-store/internal/hashing"
)

// Cluster is a thread-safe node registry backed by a consistent hash ring.
type Cluster struct {
	mu   sync.RWMutex
	ring *hashing.Ring
}

// New creates an empty Cluster with 150 virtual nodes per physical node.
func New() *Cluster {
	return &Cluster{ring: hashing.New(hashing.DefaultReplicas)}
}

// NewFromAddrs creates a Cluster pre-populated with the given node addresses.
// Useful for initialising from a NODES environment variable.
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

// AddNode registers a node address with the ring.
// addr should be "host:port" e.g. "localhost:6379".
func (c *Cluster) AddNode(addr string) error {
	if addr == "" {
		return fmt.Errorf("cluster: node address cannot be empty")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ring.AddNode(addr)
	return nil
}

// RemoveNode deregisters a node. Keys previously owned by it are
// automatically redistributed to the next node clockwise on the ring.
func (c *Cluster) RemoveNode(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ring.RemoveNode(addr)
}

// Route returns the address of the node responsible for key.
// Returns ("", false) if no nodes are registered.
func (c *Cluster) Route(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ring.GetNode(key)
}

// RouteN returns the addresses of the n nodes responsible for key,
// walking clockwise from the key's ring position.
// Used by the replication layer (Phase 5) to find replica targets.
func (c *Cluster) RouteN(key string, n int) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ring.GetN(key, n)
}

// Nodes returns all currently registered node addresses.
func (c *Cluster) Nodes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ring.Nodes()
}

// Len returns the number of registered nodes.
func (c *Cluster) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ring.Len()
}

// IsEmpty reports whether the cluster has no nodes.
func (c *Cluster) IsEmpty() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ring.IsEmpty()
}
