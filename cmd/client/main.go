// Command client is an interactive CLI for the distributed-kv-store.
//
// Single-node mode (default):
//
//	ADDR=localhost:6379 make run-client
//
// Cluster mode (Phase 4+):
//
//	NODES=localhost:6379,localhost:6380,localhost:6381 make run-client
//
// In cluster mode:
//   - Writes (SET/DEL) go to the primary (first node clockwise from key)
//   - Reads (GET) try the primary first, then replicas if the primary is down
//
// Phase 8b: connections are pooled and reused per node rather than dialed
// per command. A stale pooled connection is retried once transparently —
// see internal/pool for why that beats validating on checkout.
package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/Lenecplusultra/distributed-kv-store/internal/cluster"
	"github.com/Lenecplusultra/distributed-kv-store/internal/pool"
)

func main() {
	nodesEnv := os.Getenv("NODES")
	singleAddr := os.Getenv("ADDR")
	if singleAddr == "" {
		singleAddr = "localhost:6379"
	}

	var c *cluster.Cluster
	if nodesEnv != "" {
		addrs := strings.Split(nodesEnv, ",")
		var err error
		c, err = cluster.NewFromAddrs(addrs)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error building cluster: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("cluster mode — %d nodes\n", c.Len())
	} else {
		fmt.Printf("single-node mode — %s\n", singleAddr)
	}
	fmt.Println("type SET/GET/DEL/PING — Ctrl+C to quit")

	p := pool.New()
	defer p.Close()

	stdin := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("> ")
		line, err := stdin.ReadString('\n')
		if err != nil {
			fmt.Println("\nbye")
			return
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		targets := resolveTargets(c, line, singleAddr)

		var resp string
		var sendErr error
		for i, target := range targets {
			resp, sendErr = p.Do(target, line)
			if sendErr == nil {
				break
			}
			if i < len(targets)-1 {
				fmt.Fprintf(os.Stderr, "[client] %s unreachable, trying replica...\n", target)
			}
		}

		if sendErr != nil {
			fmt.Fprintf(os.Stderr, "error: all nodes failed: %v\n", sendErr)
			continue
		}
		fmt.Print(resp)
	}
}

// resolveTargets returns an ordered list of node addresses to try for
// the given command. For reads, replicas are included as fallbacks.
// For writes, only the primary is returned — we don't want to write
// to a replica directly; replication is the server's responsibility.
func resolveTargets(c *cluster.Cluster, line, fallback string) []string {
	if c == nil {
		return []string{fallback}
	}

	parts := strings.Fields(line)
	if len(parts) < 2 {
		// PING or no key — any node will do.
		if nodes := c.Nodes(); len(nodes) > 0 {
			return []string{nodes[0]}
		}
		return []string{fallback}
	}

	key := parts[1]
	cmd := strings.ToUpper(parts[0])

	switch cmd {
	case "GET":
		// Reads can fall back to replicas if the primary is down.
		nodes := c.RouteN(key, 3)
		if len(nodes) == 0 {
			return []string{fallback}
		}
		return nodes
	default:
		// Writes go to the primary only.
		addr, ok := c.Route(key)
		if !ok {
			return []string{fallback}
		}
		return []string{addr}
	}
}
