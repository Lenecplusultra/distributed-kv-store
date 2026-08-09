# Multi-stage build: compile in a full toolchain, ship only the binary.
#
# The runtime image is distroless/static — no shell, no package manager, no
# libc. This works because the binary is pure Go stdlib with no cgo, so it
# needs nothing from the base image at all. The result is a few megabytes
# instead of a few hundred, with a correspondingly small attack surface.
#
# The tradeoff: you cannot `docker exec` into a running container to poke
# around, because there is no shell to exec. Debugging happens through logs,
# the metrics endpoint, and `kubectl debug` ephemeral containers.

# ── Build stage ───────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

WORKDIR /src

# Copy go.mod first and download separately from the source copy. Docker
# caches each layer, so dependency resolution is only re-run when go.mod
# changes rather than on every source edit. (This project has no external
# dependencies today, but the layering costs nothing and stops being free
# to add later.)
COPY go.mod ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 produces a statically linked binary with no libc dependency,
# which is what makes the distroless/static base viable.
#
# -trimpath strips local filesystem paths from the binary, so it does not
# leak /Users/yanoubryan/... into a shipped artifact and builds are
# reproducible across machines.
#
# -ldflags="-s -w" drops the symbol table and DWARF debug info. Smaller
# binary; stack traces still work, but delve debugging does not.
RUN CGO_ENABLED=0 GOOS=linux go build \
        -trimpath \
        -ldflags="-s -w" \
        -o /out/server ./cmd/server && \
    CGO_ENABLED=0 GOOS=linux go build \
        -trimpath \
        -ldflags="-s -w" \
        -o /out/client ./cmd/client && \
    CGO_ENABLED=0 GOOS=linux go build \
        -trimpath \
        -ldflags="-s -w" \
        -o /out/bench ./cmd/bench

# Assert the binaries actually exist and are executable.
#
# `go build ./...` succeeding does not mean a binary was produced — a package
# declared as a library builds fine and silently emits nothing. This project
# shipped exactly that bug: cmd/server was declared `package tcp`, CI stayed
# green, and no server binary existed. The check belongs here as well as in
# CI, so a broken image cannot be built at all.
RUN test -x /out/server && test -x /out/client && test -x /out/bench

# ── Runtime stage ─────────────────────────────────────────────────────────
FROM gcr.io/distroless/static-debian12:nonroot

# distroless provides an unprivileged `nonroot` user (uid 65532). Running as
# non-root means a container escape does not start from uid 0, and it
# satisfies the restricted Pod Security Standard in Kubernetes.
USER nonroot:nonroot

COPY --from=builder /out/server /server
COPY --from=builder /out/client /client
COPY --from=builder /out/bench /bench

# WAL lives here. Mounted as a volume in compose and as a PVC in Kubernetes,
# so it survives container restarts.
#
# The directory must exist and be writable by uid 65532. distroless has no
# shell for mkdir, so the volume mount supplies it — see the note in
# deployments/kubernetes about fsGroup.
VOLUME ["/data"]

ENV ADDR=:6379 \
    METRICS_ADDR=:9090 \
    WAL_PATH=/data/wal.log

# 6379 — wire protocol (client traffic and replication)
# 9090 — Prometheus metrics and health endpoints
#
# Two ports because the wire protocol is a bespoke line format and cannot
# also speak HTTP. This is the usual application-port/operational-port split,
# and it lets Kubernetes scrape metrics without exposing them on the data
# path.
EXPOSE 6379 9090

ENTRYPOINT ["/server"]