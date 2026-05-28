// Package protocol defines the wire format for client-server communication.
//
// Commands use a simple newline-delimited text protocol (Phase 1).
// Example exchanges:
//
//	SET name alice\n        → +OK\n
//	GET name\n              → +alice\n
//	GET missing\n           → -ERR key not found\n
//	DEL name\n              → +OK\n
//	SET counter 1 EX 60\n   → +OK\n   (expires in 60 seconds)
package protocol

import (
	"fmt"
	"strings"
)

// Response prefixes — Redis-inspired.
const (
	PrefixOK    = "+"
	PrefixError = "-"
)

// Command represents a parsed client command.
type Command struct {
	Name string   // SET | GET | DEL | PING
	Args []string // remaining tokens
}

// Parse splits a raw line into a Command.
// Returns an error if the line is empty or malformed.
func Parse(line string) (*Command, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return nil, fmt.Errorf("empty command")
	}
	parts := strings.Fields(line)
	return &Command{
		Name: strings.ToUpper(parts[0]),
		Args: parts[1:],
	}, nil
}

// OK formats a success response.
func OK(msg string) string {
	return PrefixOK + msg + "\n"
}

// Err formats an error response.
func Err(msg string) string {
	return PrefixError + "ERR " + msg + "\n"
}
