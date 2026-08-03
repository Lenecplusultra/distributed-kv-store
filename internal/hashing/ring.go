// Package hashing implements a consistent hash ring with virtual nodes.
//
// # The problem with modulo sharding
//
// The naive approach is hash(key) % N. This is simple but fragile:
// remove one node from a 10-node cluster and % 9 remaps ~90% of keys.
// In production that means ~90% of data is suddenly unreachable.
//
// # How consistent hashing fixes this
//
// Both nodes and keys are placed on a ring of 2^32 positions (uint32).
// A key belongs to the first node clockwise from its position.
// Add or remove a node and only ~K/N keys move — not the whole dataset.
//
// # Virtual nodes
//
// Without virtual nodes, physical nodes cluster unevenly on the ring.
// We place each physical node at `replicas` positions using a chained
// hash — each virtual node's position is derived from the previous one.
// This spreads virtual nodes more evenly than a simple index suffix.
//
// # Hash function
//
// FNV-1a (hash/fnv) — fast, built into the standard library.
package hashing

import (
	"encoding/binary"
	"hash/fnv"
	"sort"
)

const DefaultReplicas = 150

// Ring is a consistent hash ring.
// Not goroutine-safe — callers must synchronise (see cluster.Cluster).
type Ring struct {
	replicas int
	keys     []uint32          // sorted ring positions
	ring     map[uint32]string // position → node address
	nodes    map[string]bool   // set of known physical node addresses
}

// New creates a Ring with replicas virtual nodes per physical node.
func New(replicas int) *Ring {
	if replicas < 1 {
		replicas = DefaultReplicas
	}
	return &Ring{
		replicas: replicas,
		ring:     make(map[uint32]string),
		nodes:    make(map[string]bool),
	}
}

// AddNode adds a physical node to the ring, creating `replicas` virtual nodes.
// No-op if the node is already present.
func (r *Ring) AddNode(addr string) {
	if r.nodes[addr] {
		return
	}
	r.nodes[addr] = true

	// Generate virtual node positions using a chained hash:
	// pos[0] = hash(addr)
	// pos[i] = hash(pos[i-1])
	// This spreads virtual nodes more evenly than addr#0, addr#1, ...  // recheck
	// because each position is derived from the previous hash output,
	// not from a predictable string suffix.
	pos := r.hashBytes([]byte(addr))
	for i := 0; i < r.replicas; i++ {
		r.keys = append(r.keys, pos)
		r.ring[pos] = addr
		// Chain: hash the current position to get the next.
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], pos)
		pos = r.hashBytes(buf[:])
	}
	sort.Slice(r.keys, func(i, j int) bool { return r.keys[i] < r.keys[j] }) // or slices.sort(r.keys) in Go 1.21+
}

// RemoveNode removes a physical node and all its virtual nodes.
// No-op if the node is not present.
func (r *Ring) RemoveNode(addr string) {
	if !r.nodes[addr] {
		return
	}
	delete(r.nodes, addr)

	// Recompute the same positions that were added and remove them.
	pos := r.hashBytes([]byte(addr))
	for i := 0; i < r.replicas; i++ {
		delete(r.ring, pos)
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], pos)
		pos = r.hashBytes(buf[:])
	}

	// Rebuild sorted key slice.
	r.keys = r.keys[:0]
	for p := range r.ring {
		r.keys = append(r.keys, p)
	}
	sort.Slice(r.keys, func(i, j int) bool { return r.keys[i] < r.keys[j] })
}

// GetNode returns the address of the node responsible for key.
// Returns ("", false) if the ring is empty.
func (r *Ring) GetNode(key string) (string, bool) {
	if len(r.keys) == 0 {
		return "", false
	}
	pos := r.hashBytes([]byte(key))
	idx := sort.Search(len(r.keys), func(i int) bool {
		return r.keys[i] >= pos
	})
	if idx == len(r.keys) {
		idx = 0 // wrap around
	}
	return r.ring[r.keys[idx]], true
}

// GetN returns up to n distinct physical nodes starting from key's position.
// Used for replication (Phase 5).
func (r *Ring) GetN(key string, n int) []string {
	if len(r.nodes) == 0 || n <= 0 {
		return nil
	}
	if n > len(r.nodes) {
		n = len(r.nodes)
	}
	pos := r.hashBytes([]byte(key))
	idx := sort.Search(len(r.keys), func(i int) bool {
		return r.keys[i] >= pos
	})
	seen := make(map[string]bool)
	result := make([]string, 0, n)
	total := len(r.keys)
	for i := 0; len(result) < n; i++ {
		node := r.ring[r.keys[(idx+i)%total]]
		if !seen[node] {
			seen[node] = true
			result = append(result, node)
		}
	}
	return result
}

// Nodes returns all physical node addresses currently in the ring.
func (r *Ring) Nodes() []string {
	nodes := make([]string, 0, len(r.nodes))
	for addr := range r.nodes {
		nodes = append(nodes, addr)
	}
	return nodes
}

// Len returns the number of physical nodes in the ring.
func (r *Ring) Len() int { return len(r.nodes) }

// IsEmpty reports whether the ring has no nodes.
func (r *Ring) IsEmpty() bool { return len(r.nodes) == 0 }

// hashBytes computes the FNV-1a 32-bit hash of b.
func (r *Ring) hashBytes(b []byte) uint32 {
	h := fnv.New32a()
	h.Write(b)
	return h.Sum32()
}
