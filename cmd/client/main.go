// Command client is an interactive CLI for the distributed-kv-store.
//
// It connects to a running server via TCP, reads commands from stdin,
// sends them, and prints the server's response. Think: redis-cli but ours.
package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: cannot connect to %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("connected to %s\ntype SET/GET/DEL/PING — Ctrl+C to quit\n\n", addr)

	// stdin reader for user input
	stdinReader := bufio.NewReader(os.Stdin)
	// server response reader
	serverReader := bufio.NewReader(conn)

	for {
		fmt.Print("> ")

		line, err := stdinReader.ReadString('\n')
		if err != nil {
			// EOF = user hit Ctrl+D
			fmt.Println("\nbye")
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Send to server (server expects newline-terminated)
		fmt.Fprintf(conn, "%s\n", line)

		// Read one response line
		resp, err := serverReader.ReadString('\n')
		if err != nil {
			fmt.Fprintf(os.Stderr, "error reading response: %v\n", err)
			return
		}
		fmt.Print(resp)
	}
}
