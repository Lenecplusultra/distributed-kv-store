# distributed-kv-store

A distributed in-memory key-value store built in Go — designed from the ground up as a miniature infrastructure product, not a tutorial project.

Inspired by the internals of systems like Redis, Dynamo, and Cassandra.

---

## What This Is

A multi-node key-value database supporting:

- **Concurrent-safe storage** with fine-grained locking
- **TTL expiration** on keys
- **Append-only persistence** for crash recovery *(Phase 2)*
- **Consistent hashing** for distributed key routing *(Phase 4)*
- **Replication** with primary/replica failover *(Phase 5)*
- **Heartbeat-based failure detection** and node rejoin *(Phase 6)*
- **Token-bucket rate limiting** per client *(Phase 7)*
- **Observability** — structured logs and metrics endpoint *(Phase 8)*

---

## Architecture

```
┌─────────────────────────────────────────────────┐
│                   Client (CLI)                  │
└────────────────────┬────────────────────────────┘
                     │ TCP
        ┌────────────▼────────────┐
        │     Rate Limiter        │  ← token bucket, per-IP
        └────────────┬────────────┘
        ┌────────────▼────────────┐
        │     Protocol Layer      │  ← command parsing
        └────────────┬────────────┘
        ┌────────────▼────────────┐
        │   Consistent Hash Ring  │  ← routes key → node
        └──────┬──────────┬───────┘
               │          │
        ┌──────▼──┐  ┌────▼────┐
        │ Node A  │  │ Node B  │   ...
        │ Primary │  │ Primary │
        └────┬────┘  └─────────┘
        ┌────▼────┐
        │Replica A│  ← replication + failover
        └─────────┘
```

---

## Project Structure

```
distributed-kv-store/
├── cmd/
│   ├── server/
│   └── client/
├── internal/
│   ├── storage/        # concurrent KV store + TTL + sweeper
│   ├── protocol/       # wire protocol parsing
│   ├── tcp/            # TCP server/client
│   ├── wal/            # append-only log (Phase 2)
│   ├── lru/            # LRU eviction (Phase 3)
│   ├── hashing/        # consistent hash ring (Phase 4)
│   ├── cluster/        # node registry + routing (Phase 4)
│   ├── replication/    # primary/replica sync (Phase 5)
│   └── metrics/        # observability (Phase 8)
├── docs/
├── deployments/
│   ├── docker/
│   └── kubernetes/
├── Makefile
└── go.mod
```

---

## Getting Started

### Prerequisites

- Go 1.22+
- Docker (for multi-node setup, Phase 9)

### Build

```bash
make build
```

### Run

```bash
make run-server   # starts the KV server
make run-client   # starts the CLI client
```

### Test

```bash
make test
```

---

## Wire Protocol

Simple newline-delimited text protocol (Phase 1):

```
SET name alice          → +OK
GET name                → +alice
GET missing             → -ERR key not found
DEL name                → +OK
SET session abc EX 60   → +OK   (expires in 60s)
PING                    → +PONG
```

---

## Development Roadmap

| Phase | Feature                         | Status        |
|-------|---------------------------------|---------------|
| 1     | Single-node TCP KV store        | ✅ Complete   |
| 2     | Append-only persistence         | ✅ Complete   |
| 3     | LRU eviction                    | ✅ Complete   |
| 4     | Consistent hashing + clustering | ✅ Complete   |
| 5     | Replication                     | ✅ Complete   |
| 6     | Failure detection + heartbeats  | ✅ Complete   |
| 7     | Token-bucket rate limiting      | ✅ Complete   |
| 8     | Observability + benchmarks      | ✅ Complete   |
| 9     | Docker + Kubernetes + CI/CD     | ✅ Complete   |

---

## Key Design Decisions

**Why consistent hashing over modulo sharding?**
Modulo sharding (`key % N`) causes massive key redistribution when nodes are added/removed. Consistent hashing minimizes key movement — only `K/N` keys need to move when a node joins or leaves.

**Why token bucket for rate limiting?**
Token buckets allow controlled bursting (smooth traffic spikes) while enforcing average rate limits. Compared to fixed-window counters, they're more accurate and don't have boundary-edge thundering herd issues.

**Why append-only log for persistence?**
Sequential writes are dramatically faster than random writes. An AOL can be replayed on startup to rebuild state after a crash, similar to how Redis AOF and database WALs work.

---

## License

MIT
