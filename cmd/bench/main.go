// Command bench load-tests a distributed-kv-store cluster and reports
// throughput and latency percentiles.
//
// # Two modes, because throughput and latency want different harnesses
//
// Saturation mode (-rate 0, the default) runs every worker in a tight loop:
// send, wait for the reply, send again. This finds maximum throughput. The
// latencies it reports are service times — how long the server took once it
// began work — and are labelled as such, because a closed loop has no
// schedule to fall behind and therefore cannot measure queueing delay.
//
// Fixed-rate mode (-rate N) issues requests on a schedule and measures each
// latency from the time the request was *due*, not from when it actually
// went out. This matters: in a closed loop, a server stall stops new
// requests from being issued, so the requests that would have suffered are
// never recorded and the percentiles come out flattering. That is
// coordinated omission, and it is the most common way a benchmark produces
// dishonest tail latency. Fixed-rate mode is where a quotable p99 comes
// from.
//
// # Exact percentiles, unlike the server
//
// Every latency sample is retained and percentiles are computed by sorting,
// so the numbers are exact rather than interpolated. The server's
// internal/metrics histogram makes the opposite trade — fixed buckets,
// constant memory — because it runs indefinitely. This runs for a bounded
// duration and can afford the memory. When the two disagree, this one is
// more precise and the server's is the one that can run forever.
//
// # Uniform keys by default
//
// Uniform random access over a keyspace that fits in memory measures the
// store rather than eviction behaviour, and does not flatter the cache hit
// rate. Zipfian (-dist zipf) concentrates traffic on a hot subset, which is
// what production traffic actually looks like and which produces better
// numbers — reported separately rather than as the headline.
//
// # Warmup
//
// The warmup phase is excluded from all results. It populates the keyspace
// so reads hit rather than miss, establishes pooled connections, and lets
// the Go runtime settle. Including it would blend one-time costs into the
// steady-state numbers.
//
// Usage:
//
//	go run ./cmd/bench -addr localhost:6379 -clients 50 -duration 30s
//	go run ./cmd/bench -rate 50000 -duration 30s      # fixed-rate, honest p99
//	go run ./cmd/bench -dist zipf -read-ratio 0.95    # hot-key workload
package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Lenecplusultra/distributed-kv-store/internal/cluster"
	"github.com/Lenecplusultra/distributed-kv-store/internal/pool"
)

type config struct {
	addr      string
	nodes     string
	clients   int
	duration  time.Duration
	warmup    time.Duration
	readRatio float64
	keyspace  int
	valueSize int
	dist      string
	zipfS     float64
	rate      float64
	seed      int64
}

// sample is one recorded operation.
type sample struct {
	latency time.Duration
	isRead  bool
	failed  bool
}

func main() {
	cfg := parseFlags()

	targets, err := resolveNodes(cfg)
	if err != nil {
		log.Fatalf("[bench] %v", err)
	}
	fmt.Printf("target(s): %s\n", strings.Join(targets, ", "))

	p := pool.New()
	defer p.Close()

	value := strings.Repeat("x", cfg.valueSize)

	// --- Warmup: populate keys, establish connections, settle the runtime ---
	fmt.Printf("warming up for %s...\n", cfg.warmup)
	warmup(p, targets, cfg, value)

	// --- Measured run ---
	mode := "saturation (closed loop)"
	if cfg.rate > 0 {
		mode = fmt.Sprintf("fixed rate (%.0f ops/sec target)", cfg.rate)
	}
	fmt.Printf("running %s for %s with %d clients...\n", mode, cfg.duration, cfg.clients)

	start := time.Now()
	samples := run(p, targets, cfg, value)
	elapsed := time.Since(start)

	report(cfg, samples, elapsed, targets)
}

func parseFlags() config {
	var cfg config

	flag.StringVar(&cfg.addr, "addr", "localhost:6379", "single node address")
	flag.StringVar(&cfg.nodes, "nodes", "", "comma-separated cluster nodes (overrides -addr)")
	flag.IntVar(&cfg.clients, "clients", 50, "concurrent clients")
	flag.DurationVar(&cfg.duration, "duration", 30*time.Second, "measured run duration")
	flag.DurationVar(&cfg.warmup, "warmup", 5*time.Second, "warmup duration, excluded from results")
	flag.Float64Var(&cfg.readRatio, "read-ratio", 0.8, "fraction of operations that are GET")
	flag.IntVar(&cfg.keyspace, "keyspace", 100000, "number of distinct keys")
	flag.IntVar(&cfg.valueSize, "value-size", 64, "value size in bytes")
	flag.StringVar(&cfg.dist, "dist", "uniform", "key distribution: uniform | zipf")
	flag.Float64Var(&cfg.zipfS, "zipf-s", 1.1, "zipf skew (must be > 1; higher is hotter)")
	flag.Float64Var(&cfg.rate, "rate", 0, "total target ops/sec; 0 means saturation mode")
	flag.Int64Var(&cfg.seed, "seed", 1, "random seed, for reproducible runs")

	flag.Parse()

	if cfg.clients < 1 {
		log.Fatal("[bench] -clients must be at least 1")
	}
	if cfg.readRatio < 0 || cfg.readRatio > 1 {
		log.Fatal("[bench] -read-ratio must be between 0 and 1")
	}
	if cfg.dist != "uniform" && cfg.dist != "zipf" {
		log.Fatal("[bench] -dist must be uniform or zipf")
	}
	if cfg.dist == "zipf" && cfg.zipfS <= 1 {
		log.Fatal("[bench] -zipf-s must be greater than 1")
	}
	return cfg
}

// resolveNodes returns the addresses to send to. In cluster mode the hash
// ring picks the node per key, matching how a real smart client routes.
func resolveNodes(cfg config) ([]string, error) {
	if cfg.nodes == "" {
		return []string{cfg.addr}, nil
	}
	parts := strings.Split(cfg.nodes, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("-nodes was set but contained no addresses")
	}
	return out, nil
}

// keyPicker returns a function generating key indices for one worker.
// Each worker gets its own generator: math/rand sources are not safe for
// concurrent use, and sharing one would serialise workers on its mutex,
// which would show up as latency that belongs to the benchmark rather than
// the server.
func keyPicker(cfg config, workerSeed int64) func() int {
	r := rand.New(rand.NewSource(workerSeed))

	if cfg.dist == "zipf" {
		z := rand.NewZipf(r, cfg.zipfS, 1, uint64(cfg.keyspace-1))
		return func() int { return int(z.Uint64()) }
	}
	return func() int { return r.Intn(cfg.keyspace) }
}

// router picks the node for a key. With one node it is a constant; with
// several it uses the same consistent hash ring the real client uses.
func router(targets []string) func(string) string {
	if len(targets) == 1 {
		single := targets[0]
		return func(string) string { return single }
	}

	c, err := cluster.NewFromAddrs(targets)
	if err != nil {
		log.Fatalf("[bench] building ring: %v", err)
	}
	return func(key string) string {
		addr, ok := c.Route(key)
		if !ok {
			return targets[0]
		}
		return addr
	}
}

// warmup populates every key so that reads hit, and opens the connections
// the measured run will reuse.
func warmup(p *pool.Pool, targets []string, cfg config, value string) {
	route := router(targets)
	deadline := time.Now().Add(cfg.warmup)

	var wg sync.WaitGroup
	perWorker := cfg.keyspace / cfg.clients

	for w := 0; w < cfg.clients; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			startKey := w * perWorker
			endKey := startKey + perWorker
			if w == cfg.clients-1 {
				endKey = cfg.keyspace // last worker takes the remainder
			}
			for i := startKey; i < endKey; i++ {
				if time.Now().After(deadline) {
					return
				}
				key := fmt.Sprintf("key%d", i)
				p.Do(route(key), fmt.Sprintf("SET %s %s", key, value))
			}
		}(w)
	}
	wg.Wait()
}

// run executes the measured phase and returns every sample.
func run(p *pool.Pool, targets []string, cfg config, value string) []sample {
	route := router(targets)
	deadline := time.Now().Add(cfg.duration)

	// Per-worker slices, merged at the end. No shared state on the hot path,
	// so the harness contributes no lock contention of its own.
	perWorker := make([][]sample, cfg.clients)

	// Pre-size to the expected sample count to avoid reallocating mid-run,
	// which would appear as latency spikes.
	estimate := 10000
	if cfg.rate > 0 {
		estimate = int(cfg.rate/float64(cfg.clients)*cfg.duration.Seconds()) + 1000
	}

	var wg sync.WaitGroup
	for w := 0; w < cfg.clients; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			perWorker[w] = worker(p, route, cfg, value, w, deadline, estimate)
		}(w)
	}
	wg.Wait()

	total := 0
	for _, s := range perWorker {
		total += len(s)
	}
	all := make([]sample, 0, total)
	for _, s := range perWorker {
		all = append(all, s...)
	}
	return all
}

func worker(p *pool.Pool, route func(string) string, cfg config, value string,
	id int, deadline time.Time, estimate int) []sample {

	samples := make([]sample, 0, estimate)
	pick := keyPicker(cfg, cfg.seed+int64(id))
	r := rand.New(rand.NewSource(cfg.seed*7919 + int64(id)))

	// Fixed-rate mode: this worker owns a share of the target rate and keeps
	// its own schedule.
	var interval time.Duration
	var next time.Time
	if cfg.rate > 0 {
		perWorkerRate := cfg.rate / float64(cfg.clients)
		interval = time.Duration(float64(time.Second) / perWorkerRate)
		next = time.Now()
	}

	for time.Now().Before(deadline) {
		var due time.Time

		if cfg.rate > 0 {
			due = next
			next = next.Add(interval)

			if wait := time.Until(due); wait > 0 {
				time.Sleep(wait)
			}
			// If the server has stalled, `due` is already in the past and
			// the latency below includes the time this request spent
			// waiting to be issued. That inclusion is the whole point:
			// omitting it is what makes closed-loop benchmarks lie about
			// the tail.
		}

		key := fmt.Sprintf("key%d", pick())
		isRead := r.Float64() < cfg.readRatio

		var cmd string
		if isRead {
			cmd = "GET " + key
		} else {
			cmd = fmt.Sprintf("SET %s %s", key, value)
		}

		sent := time.Now()
		if cfg.rate == 0 {
			due = sent
		}

		resp, err := p.Do(route(key), cmd)
		latency := time.Since(due)

		failed := err != nil
		if !failed && isRead && strings.HasPrefix(resp, "-") {
			// A GET miss is a valid answer, not a failure. Only transport
			// errors and unexpected write rejections count as errors.
			failed = false
		}
		if !failed && !isRead && strings.HasPrefix(resp, "-") {
			failed = true
		}

		samples = append(samples, sample{latency: latency, isRead: isRead, failed: failed})
	}

	return samples
}

// ── Reporting ─────────────────────────────────────────────────────────────

func report(cfg config, samples []sample, elapsed time.Duration, targets []string) {
	if len(samples) == 0 {
		fmt.Println("no samples collected")
		return
	}

	var reads, writes []time.Duration
	var all []time.Duration
	errors := 0

	for _, s := range samples {
		if s.failed {
			errors++
			continue
		}
		all = append(all, s.latency)
		if s.isRead {
			reads = append(reads, s.latency)
		} else {
			writes = append(writes, s.latency)
		}
	}

	throughput := float64(len(samples)) / elapsed.Seconds()

	latencyKind := "service time (closed loop — NOT a queueing-delay p99)"
	if cfg.rate > 0 {
		latencyKind = "response time from scheduled send (includes queueing delay)"
	}

	fmt.Printf(`
────────────────────────────────────────────────────────────
 distributed-kv-store benchmark
────────────────────────────────────────────────────────────
 targets        %s
 clients        %d
 duration       %s (plus %s warmup, excluded)
 workload       %.0f%% read / %.0f%% write
 keyspace       %d keys, %d-byte values, %s distribution
 mode           %s
────────────────────────────────────────────────────────────
 THROUGHPUT     %.0f ops/sec
 operations     %d total (%d reads, %d writes)
 errors         %d (%.3f%%)
────────────────────────────────────────────────────────────
 LATENCY — %s
`,
		strings.Join(targets, ", "),
		cfg.clients,
		cfg.duration, cfg.warmup,
		cfg.readRatio*100, (1-cfg.readRatio)*100,
		cfg.keyspace, cfg.valueSize, cfg.dist,
		modeLabel(cfg),
		throughput,
		len(samples), len(reads), len(writes),
		errors, float64(errors)/float64(len(samples))*100,
		latencyKind,
	)

	printPercentiles("all", all)
	printPercentiles("reads", reads)
	printPercentiles("writes", writes)

	fmt.Println("────────────────────────────────────────────────────────────")

	if cfg.rate == 0 {
		fmt.Println(` NOTE: saturation mode measures maximum throughput. Its
       latencies are service times and understate the tail,
       because a closed loop cannot fall behind a schedule.
       For a quotable p99, rerun with -rate set below the
       throughput above.`)
		fmt.Println("────────────────────────────────────────────────────────────")
	}
}

func modeLabel(cfg config) string {
	if cfg.rate > 0 {
		return fmt.Sprintf("fixed rate, %.0f ops/sec target", cfg.rate)
	}
	return "saturation (closed loop)"
}

// printPercentiles sorts and reports exact percentiles.
func printPercentiles(label string, d []time.Duration) {
	if len(d) == 0 {
		fmt.Printf(" %-8s (none)\n", label)
		return
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })

	fmt.Printf(" %-8s n=%-9d p50=%-10s p90=%-10s p99=%-10s p99.9=%-10s max=%s\n",
		label, len(d),
		round(pct(d, 0.50)), round(pct(d, 0.90)),
		round(pct(d, 0.99)), round(pct(d, 0.999)),
		round(d[len(d)-1]),
	)
}

// pct returns the exact value at the given percentile using nearest-rank,
// which is the standard for a sorted sample set and needs no interpolation.
func pct(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(q * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// round trims sub-microsecond noise that is below the measurement floor.
func round(d time.Duration) time.Duration {
	if d < time.Millisecond {
		return d.Round(time.Microsecond)
	}
	return d.Round(10 * time.Microsecond)
}
