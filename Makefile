.PHONY: build run-server run-client test lint clean

BINARY_SERVER=bin/server
BINARY_CLIENT=bin/client

build:
	@echo "Building server and client..."
	go build -o $(BINARY_SERVER) ./cmd/server
	go build -o $(BINARY_CLIENT) ./cmd/client

run-server:
	go run ./cmd/server

run-client:
	go run ./cmd/client

test:
	go test ./... -v -race

lint:
	golangci-lint run ./...

clean:
	rm -rf bin/

tidy:
	go mod tidy

.DEFAULT_GOAL := build
