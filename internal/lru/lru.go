// Package lru implements a fixed-capacity Least Recently Used cache.
//
// Every get and set moves the key to the front of the list (most recent).
// When capacity is exceeded, the tail (least recent) is evicted.
//
// # Data structure
//
// A doubly linked list combined with a hashmap:
//
//	Hashmap  → O(1) lookup of any node by key
//	List     → O(1) move-to-front and remove-tail
//
// Together every operation is O(1). This is the standard LRU implementation
// used in Redis, database buffer pools, and CPU caches.
//
// # Concurrency
//
// Cache is not goroutine-safe on its own. The caller (storage.Store)
// holds its RWMutex across all LRU operations, so no additional locking
// is needed here. Keeping locking at one layer avoids lock nesting bugs.
package lru

// node is one element in the doubly linked list.
type node struct {
	key        string
	prev, next *node
}

// Cache is a fixed-capacity LRU eviction tracker.
// It tracks access order but does not store values —
// values live in the storage map. This separation keeps
// each concern in one place.
type Cache struct {
	capacity int
	keys     map[string]*node // key → node for O(1) lookup
	head     *node            // most recently used (sentinel)
	tail     *node            // least recently used (sentinel)
}

// New creates a Cache with the given capacity.
// Panics if capacity < 1 — a zero-capacity cache makes no sense.
func New(capacity int) *Cache {
	if capacity < 1 {
		panic("lru: capacity must be >= 1")
	}
	// Sentinel head and tail simplify insert/remove logic —
	// no nil checks needed at list boundaries.
	head := &node{}
	tail := &node{}
	head.next = tail
	tail.prev = head

	return &Cache{
		capacity: capacity,
		keys:     make(map[string]*node, capacity),
		head:     head,
		tail:     tail,
	}
}

// Touch marks key as most recently used.
// If the key is new, it is added to the front.
// If adding it exceeds capacity, the least recently used key
// is evicted and returned so the caller can remove it from
// the value store. Returns ("", false) if no eviction occurred.
func (c *Cache) Touch(key string) (evicted string, ok bool) {
	if n, exists := c.keys[key]; exists {
		// Key already tracked — move to front, no eviction.
		c.remove(n)
		c.pushFront(n)
		return "", false
	}

	// New key — add to front.
	n := &node{key: key}
	c.keys[key] = n
	c.pushFront(n)

	// Evict tail if over capacity.
	if len(c.keys) > c.capacity {
		lru := c.tail.prev // node just before sentinel tail
		c.remove(lru)
		delete(c.keys, lru.key)
		return lru.key, true
	}

	return "", false
}

// Remove explicitly removes a key from the tracker.
// Called when a key is deleted by the client (DEL command) —
// distinct from eviction, which is internal.
func (c *Cache) Remove(key string) {
	if n, ok := c.keys[key]; ok {
		c.remove(n)
		delete(c.keys, key)
	}
}

// Len returns the number of keys currently tracked.
func (c *Cache) Len() int {
	return len(c.keys)
}

// pushFront inserts n immediately after the head sentinel.
func (c *Cache) pushFront(n *node) {
	n.prev = c.head
	n.next = c.head.next
	c.head.next.prev = n
	c.head.next = n
}

// remove unlinks n from the list without touching the map.
func (c *Cache) remove(n *node) {
	n.prev.next = n.next
	n.next.prev = n.prev
}
