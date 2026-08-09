// Package metrics collects server-side counters, latency histograms, and
// gauges, and exposes them in the Prometheus text exposition format.
//
// # Why Prometheus text, and why no client library
//
// The exposition format is plain text over HTTP, so producing it needs
// nothing beyond fmt and net/http. That keeps the project stdlib-only, and
// it is what a Kubernetes deployment in Phase 9 would actually scrape.
//
// The alternative — an INFO-style STATS command, as Redis does — would keep
// everything on one port and one protocol, but this wire protocol is
// single-line request/response, and multi-line stats would need protocol
// changes that buy nothing operationally.
//
// # Why bucketed histograms rather than exact samples
//
// Reporting a p99 requires either every sample (exact, memory grows without
// bound) or fixed buckets (approximate, constant memory). A server runs
// indefinitely, so it takes the bucketed form: memory is fixed regardless of
// traffic, at the cost of a p99 interpolated between bucket edges.
//
// The benchmark tool makes the opposite choice — it runs for a bounded
// duration and keeps every sample, so its percentiles are exact. When the
// two disagree, the benchmark is the more precise number and this is the one
// that can run forever.
//
// # Concurrency
//
// All recording paths use atomics rather than mutexes. Metrics sit on the
// hot path of every command, and a contended mutex there would show up in
// the very latency numbers being measured.
//
// A nil *Metrics is valid and drops everything, matching the nil-safe
// pattern used for the WAL, replicator, and rate limiter.
package metrics

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// defaultBuckets are upper bounds in seconds, chosen to straddle the
// latencies this system actually produces: sub-millisecond for local
// in-memory hits, single-digit milliseconds once the network and WAL are
// involved, and a long tail for retries and stalls.
var defaultBuckets = []float64{
	0.00005, // 50µs
	0.0001,  // 100µs
	0.00025, // 250µs
	0.0005,  // 500µs
	0.001,   // 1ms
	0.0025,  // 2.5ms
	0.005,   // 5ms
	0.01,    // 10ms
	0.025,   // 25ms
	0.05,    // 50ms
	0.1,     // 100ms
	0.25,    // 250ms
	0.5,     // 500ms
	1.0,     // 1s
}

// Histogram counts observations falling into fixed latency buckets.
//
// Buckets hold non-cumulative counts internally; the Prometheus format
// requires cumulative ("le") semantics, so they are summed at render time.
// Storing them non-cumulatively means a recording only touches one counter
// rather than every bucket above the observed value.
type Histogram struct {
	bounds  []float64
	counts  []atomic.Int64
	sum     atomic.Uint64 // float64 bits, accumulated via CAS
	total   atomic.Int64
	nameFor string
}

func newHistogram(name string, bounds []float64) *Histogram {
	return &Histogram{
		bounds:  bounds,
		counts:  make([]atomic.Int64, len(bounds)+1), // +1 for +Inf
		nameFor: name,
	}
}

// Observe records one duration.
func (h *Histogram) Observe(d time.Duration) {
	if h == nil {
		return
	}
	v := d.Seconds()

	// Linear scan. With ~14 buckets this beats binary search in practice —
	// no branch misprediction, and the whole slice sits in one cache line's
	// worth of loads.
	idx := len(h.bounds) // +Inf bucket unless we find a smaller bound
	for i, b := range h.bounds {
		if v <= b {
			idx = i
			break
		}
	}
	h.counts[idx].Add(1)
	h.total.Add(1)
	h.addSum(v)
}

// addSum accumulates a float64 total without a lock, via compare-and-swap on
// the bit pattern.
func (h *Histogram) addSum(v float64) {
	for {
		old := h.sum.Load()
		next := float64FromBits(old) + v
		if h.sum.CompareAndSwap(old, float64ToBits(next)) {
			return
		}
	}
}

// Count returns the number of observations recorded.
func (h *Histogram) Count() int64 {
	if h == nil {
		return 0
	}
	return h.total.Load()
}

// Quantile estimates the value at q (0 < q < 1) by linear interpolation
// within the bucket where the target observation falls.
//
// This is an estimate, and deliberately labelled as one wherever it is
// reported. A p99 landing in the 1ms–2.5ms bucket is reported somewhere in
// that range according to how far into the bucket the target index sits; the
// true value could be anywhere within it.
func (h *Histogram) Quantile(q float64) float64 {
	if h == nil {
		return 0
	}
	total := h.total.Load()
	if total == 0 {
		return 0
	}

	target := q * float64(total)
	var cumulative int64
	prevBound := 0.0

	for i, bound := range h.bounds {
		c := h.counts[i].Load()
		if float64(cumulative+c) >= target && c > 0 {
			// Interpolate within this bucket.
			into := (target - float64(cumulative)) / float64(c)
			return prevBound + (bound-prevBound)*into
		}
		cumulative += c
		prevBound = bound
	}

	// Fell into +Inf — report the largest finite bound as a floor.
	if len(h.bounds) == 0 {
		return 0
	}
	return h.bounds[len(h.bounds)-1]
}

// Metrics is the server's metric registry.
type Metrics struct {
	// Command counters, keyed by uppercase command name. Pre-populated at
	// construction so the recording path never writes to the map.
	commands map[string]*atomic.Int64

	CommandsUnknown atomic.Int64
	ParseErrors     atomic.Int64

	GetHits   atomic.Int64
	GetMisses atomic.Int64

	RateLimitDenied atomic.Int64

	ConnectionsOpened atomic.Int64
	ConnectionsActive atomic.Int64

	WALAppendErrors atomic.Int64

	Latency *Histogram

	startedAt time.Time

	// gauges are sampled at scrape time rather than recorded continuously,
	// because their source of truth lives elsewhere — replica health in the
	// replicator, idle connections in the pool. Copying that state into a
	// counter would just create a second thing to keep in sync.
	gaugeMu sync.RWMutex
	gauges  map[string]gauge
}

type gauge struct {
	help  string
	label string
	fn    func() map[string]float64 // label value → number
}

// trackedCommands is the fixed command set. An unrecognised command
// increments CommandsUnknown rather than growing the map, so a client
// sending garbage cannot cause unbounded memory growth — the same concern
// that motivated the rate limiter's bucket reaper.
var trackedCommands = []string{"PING", "SET", "GET", "DEL", "HELLO"}

// New creates a Metrics registry.
func New() *Metrics {
	m := &Metrics{
		commands:  make(map[string]*atomic.Int64, len(trackedCommands)),
		Latency:   newHistogram("kv_command_duration_seconds", defaultBuckets),
		startedAt: time.Now(),
		gauges:    make(map[string]gauge),
	}
	for _, c := range trackedCommands {
		m.commands[c] = new(atomic.Int64)
	}
	return m
}

// RecordCommand increments the counter for a command name.
func (m *Metrics) RecordCommand(name string) {
	if m == nil {
		return
	}
	if c, ok := m.commands[strings.ToUpper(name)]; ok {
		c.Add(1)
		return
	}
	m.CommandsUnknown.Add(1)
}

// RecordLatency records how long a command took.
func (m *Metrics) RecordLatency(d time.Duration) {
	if m == nil {
		return
	}
	m.Latency.Observe(d)
}

// RecordGet records whether a GET found its key.
func (m *Metrics) RecordGet(hit bool) {
	if m == nil {
		return
	}
	if hit {
		m.GetHits.Add(1)
		return
	}
	m.GetMisses.Add(1)
}

// RecordRateLimitDenied counts one throttled request.
func (m *Metrics) RecordRateLimitDenied() {
	if m == nil {
		return
	}
	m.RateLimitDenied.Add(1)
}

// RecordParseError counts one unparseable line.
func (m *Metrics) RecordParseError() {
	if m == nil {
		return
	}
	m.ParseErrors.Add(1)
}

// RecordWALError counts one failed WAL append.
func (m *Metrics) RecordWALError() {
	if m == nil {
		return
	}
	m.WALAppendErrors.Add(1)
}

// ConnOpened and ConnClosed track concurrent connections.
func (m *Metrics) ConnOpened() {
	if m == nil {
		return
	}
	m.ConnectionsOpened.Add(1)
	m.ConnectionsActive.Add(1)
}

func (m *Metrics) ConnClosed() {
	if m == nil {
		return
	}
	m.ConnectionsActive.Add(-1)
}

// RegisterGauge adds a labelled gauge sampled at scrape time.
//
// fn returns a map of label value to number — for example replica address to
// 1 (alive) or 0 (dead). Called during scraping, so it must be cheap and
// must not block.
func (m *Metrics) RegisterGauge(name, help, label string, fn func() map[string]float64) {
	if m == nil {
		return
	}
	m.gaugeMu.Lock()
	defer m.gaugeMu.Unlock()
	m.gauges[name] = gauge{help: help, label: label, fn: fn}
}

// WritePrometheus renders every metric in the Prometheus text exposition
// format.
func (m *Metrics) WritePrometheus(w io.Writer) {
	if m == nil {
		return
	}

	writeCounter(w, "kv_uptime_seconds", "Seconds since the server started",
		int64(time.Since(m.startedAt).Seconds()))

	// Commands, sorted so scrape output is stable and diffable.
	fmt.Fprintf(w, "# HELP kv_commands_total Commands processed by name\n")
	fmt.Fprintf(w, "# TYPE kv_commands_total counter\n")
	names := make([]string, 0, len(m.commands))
	for name := range m.commands {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Fprintf(w, "kv_commands_total{command=\"%s\"} %d\n", name, m.commands[name].Load())
	}
	fmt.Fprintf(w, "kv_commands_total{command=\"unknown\"} %d\n", m.CommandsUnknown.Load())

	writeCounter(w, "kv_parse_errors_total", "Lines that could not be parsed",
		m.ParseErrors.Load())
	writeCounter(w, "kv_get_hits_total", "GET commands that found a key",
		m.GetHits.Load())
	writeCounter(w, "kv_get_misses_total", "GET commands that did not find a key",
		m.GetMisses.Load())
	writeCounter(w, "kv_rate_limit_denied_total", "Requests rejected by the rate limiter",
		m.RateLimitDenied.Load())
	writeCounter(w, "kv_wal_append_errors_total", "Failed WAL appends",
		m.WALAppendErrors.Load())
	writeCounter(w, "kv_connections_opened_total", "Connections accepted since start",
		m.ConnectionsOpened.Load())
	writeGauge(w, "kv_connections_active", "Currently open client connections",
		m.ConnectionsActive.Load())

	m.writeHistogram(w)
	m.writeGauges(w)
}

func (m *Metrics) writeHistogram(w io.Writer) {
	h := m.Latency
	fmt.Fprintf(w, "# HELP %s Command handling latency in seconds\n", h.nameFor)
	fmt.Fprintf(w, "# TYPE %s histogram\n", h.nameFor)

	// Prometheus buckets are cumulative: each le="X" reports everything at
	// or below X.
	var cumulative int64
	for i, bound := range h.bounds {
		cumulative += h.counts[i].Load()
		fmt.Fprintf(w, "%s_bucket{le=\"%g\"} %d\n", h.nameFor, bound, cumulative)
	}
	cumulative += h.counts[len(h.bounds)].Load()
	fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", h.nameFor, cumulative)
	fmt.Fprintf(w, "%s_sum %g\n", h.nameFor, float64FromBits(h.sum.Load()))
	fmt.Fprintf(w, "%s_count %d\n", h.nameFor, h.total.Load())
}

func (m *Metrics) writeGauges(w io.Writer) {
	m.gaugeMu.RLock()
	defer m.gaugeMu.RUnlock()

	names := make([]string, 0, len(m.gauges))
	for name := range m.gauges {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		g := m.gauges[name]
		values := g.fn()
		if values == nil {
			continue
		}
		fmt.Fprintf(w, "# HELP %s %s\n", name, g.help)
		fmt.Fprintf(w, "# TYPE %s gauge\n", name)

		labels := make([]string, 0, len(values))
		for l := range values {
			labels = append(labels, l)
		}
		sort.Strings(labels)
		for _, l := range labels {
			fmt.Fprintf(w, "%s{%s=\"%s\"} %g\n", name, g.label, l, values[l])
		}
	}
}

func writeCounter(w io.Writer, name, help string, v int64) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s counter\n", name)
	fmt.Fprintf(w, "%s %d\n", name, v)
}

func writeGauge(w io.Writer, name, help string, v int64) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s gauge\n", name)
	fmt.Fprintf(w, "%s %d\n", name, v)
}

// Handler returns an http.Handler serving the metrics endpoint.
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		m.WritePrometheus(w)
	})
}

// float64ToBits / float64FromBits let a float64 live in an atomic.Uint64,
// which is how the histogram accumulates a floating-point sum without a lock.
func float64ToBits(f float64) uint64 { return math.Float64bits(f) }

func float64FromBits(b uint64) float64 { return math.Float64frombits(b) }
