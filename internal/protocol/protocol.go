package protocol

import (
	"fmt"
	"strings"
)

const (
	PrefixOK    = "+"
	PrefixError = "-"
)

// Command represents a parsed client command.
type Command struct {
	Name string   // SET | GET | DEL | PING (always uppercase)
	Args []string // remaining tokens after the command name
}

// Parse splits a raw input line into a Command.
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

// OK formats a success response line.
func OK(msg string) string {
	return PrefixOK + msg + "\n"
}

// Err formats an error response line.
func Err(msg string) string {
	return PrefixError + "ERR " + msg + "\n"
}
