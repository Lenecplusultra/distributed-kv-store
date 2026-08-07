// Package cluster — see cluster.go for package doc.
//
// This file implements HealthTracker: tracks consecutive heartbeat
// failures per node and declares nodes dead after a configurable
// threshold. Revival is detected when a dead node responds again.
//
// # Why consecutive misses instead of elapsed time
//
// A single missed heartbeat could be a network blip, a GC pause, or
// a brief CPU spike — not a real failure. Declaring a node dead on the
// first miss causes unnecessary churn: keys reroute, load spikes on
// other nodes, then the node recovers and keys reroute back.
//
// Requiring N consecutive misses filters transient noise. Three misses
// at a 2-second probe interval means a node must be unreachable for
// ~6 seconds before we act — long enough to rule out blips, short
// enough to fail fast on real outages.
//
// This is the same model Kubernetes uses for liveness probes
// (failureThreshold: 3) and how most production failure detectors work.
package cluster

import (
	"sync"
	"time"
)

const DefaultMissThreshold = 3

// nodeHealth is the internal health state for one node.
type nodeHealth struct {
	addr              string
	alive             bool
	consecutiveMisses int
	lastSeen          time.Time
}

// NodeStatus is an externally visible snapshot of one node's health.
// Used by the observability layer (Phase 8).
type NodeStatus struct {
	Addr              string
	Alive             bool
	ConsecutiveMisses int
	LastSeen          time.Time
}

// HealthTracker records heartbeat outcomes per node.
// It is goroutine-safe and has no knowledge of the ring —
// it only tracks health state. The Cluster decides what to do
// when a node dies or revives.
type HealthTracker struct {
	mu            sync.RWMutex
	nodes         map[string]*nodeHealth
	missThreshold int
}

// newHealthTracker creates a HealthTracker that declares nodes dead
// after missThreshold consecutive missed heartbeats.
func newHealthTracker(missThreshold int) *HealthTracker {
	return &HealthTracker{
		nodes:         make(map[string]*nodeHealth),
		missThreshold: missThreshold,
	}
}

// register adds addr to the tracker in the alive state.
// No-op if addr is already tracked.
func (h *HealthTracker) register(addr string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.nodes[addr]; !ok {
		h.nodes[addr] = &nodeHealth{
			addr:     addr,
			alive:    true,
			lastSeen: time.Now(),
		}
	}
}

// deregister removes addr from the tracker entirely.
func (h *HealthTracker) deregister(addr string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.nodes, addr)
}

// Beat records a successful heartbeat for addr.
// Resets consecutive miss count. Returns true if the node was
// previously dead — the caller should re-add it to the ring.
func (h *HealthTracker) Beat(addr string) (revived bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	n, ok := h.nodes[addr]
	if !ok {
		return false
	}
	wasAlive := n.alive
	n.alive = true
	n.consecutiveMisses = 0
	n.lastSeen = time.Now()
	return !wasAlive // true only if it was dead and is now back
}

// Miss records a failed heartbeat for addr.
// Increments consecutive miss count. Returns true if the node just
// crossed the death threshold — the caller should remove it from the ring.
// Returns false if the node was already dead (avoids duplicate removes).
func (h *HealthTracker) Miss(addr string) (justDied bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	n, ok := h.nodes[addr]
	if !ok || !n.alive {
		return false // unknown node or already dead — nothing to do
	}
	n.consecutiveMisses++
	if n.consecutiveMisses >= h.missThreshold {
		n.alive = false
		return true
	}
	return false
}

// IsAlive reports whether addr is currently considered alive.
func (h *HealthTracker) IsAlive(addr string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	n, ok := h.nodes[addr]
	return ok && n.alive
}

// AllNodes returns all tracked addresses (alive and dead).
// Used by the failure detector to probe dead nodes for revival.
func (h *HealthTracker) AllNodes() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	addrs := make([]string, 0, len(h.nodes))
	for addr := range h.nodes {
		addrs = append(addrs, addr)
	}
	return addrs
}

// Status returns a health snapshot of every tracked node.
// Used by the observability layer.
func (h *HealthTracker) Status() map[string]NodeStatus {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make(map[string]NodeStatus, len(h.nodes))
	for addr, n := range h.nodes {
		out[addr] = NodeStatus{
			Addr:              n.addr,
			Alive:             n.alive,
			ConsecutiveMisses: n.consecutiveMisses,
			LastSeen:          n.lastSeen,
		}
	}
	return out
}
