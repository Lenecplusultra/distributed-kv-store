package metrics_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Lenecplusultra/distributed-kv-store/internal/metrics"
)

// scrape renders the registry and returns it as a string.
func scrape(m *metrics.Metrics) string {
	var sb strings.Builder
	m.WritePrometheus(&sb)
	return sb.String()
}

// findLine returns the first rendered line beginning with prefix.
func findLine(t *testing.T, out, prefix string) string {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, prefix) {
			return line
		}
	}
	t.Fatalf("no line starting with %q in:\n%s", prefix, out)
	return ""
}

// ── Nil safety ────────────────────────────────────────────────────────────

func TestNilMetricsIsSafe(t *testing.T) {
	var m *metrics.Metrics

	// Every recording path must tolerate a nil registry, since metrics are
	// optional in the same way the WAL and rate limiter are.
	m.RecordCommand("GET")
	m.RecordLatency(time.Millisecond)
	m.RecordGet(true)
	m.RecordRateLimitDenied()
	m.RecordParseError()
	m.RecordWALError()
	m.ConnOpened()
	m.ConnClosed()
	m.RegisterGauge("x", "help", "label", func() map[string]float64 { return nil })

	var sb strings.Builder
	m.WritePrometheus(&sb)
	if sb.Len() != 0 {
		t.Fatalf("nil registry wrote output: %q", sb.String())
	}
}

// ── Counters ──────────────────────────────────────────────────────────────

func TestCommandCounters(t *testing.T) {
	m := metrics.New()

	m.RecordCommand("GET")
	m.RecordCommand("GET")
	m.RecordCommand("SET")
	m.RecordCommand("get") // case-insensitive

	out := scrape(m)

	if got := findLine(t, out, `kv_commands_total{command="GET"}`); !strings.HasSuffix(got, " 3") {
		t.Fatalf("GET counter: %q", got)
	}
	if got := findLine(t, out, `kv_commands_total{command="SET"}`); !strings.HasSuffix(got, " 1") {
		t.Fatalf("SET counter: %q", got)
	}
}

func TestUnknownCommandsDoNotGrowTheMap(t *testing.T) {
	m := metrics.New()

	// A client sending garbage must not be able to create unbounded label
	// cardinality — the same concern as the rate limiter's bucket reaper.
	for i := 0; i < 100; i++ {
		m.RecordCommand(strings.Repeat("X", i+1))
	}

	out := scrape(m)
	if got := findLine(t, out, `kv_commands_total{command="unknown"}`); !strings.HasSuffix(got, " 100") {
		t.Fatalf("unknown counter: %q", got)
	}

	// Exactly the tracked commands plus "unknown" should appear.
	if n := strings.Count(out, "kv_commands_total{"); n != 6 {
		t.Fatalf("expected 6 command series, got %d — label cardinality is unbounded", n)
	}
}

func TestGetHitsAndMisses(t *testing.T) {
	m := metrics.New()

	m.RecordGet(true)
	m.RecordGet(true)
	m.RecordGet(false)

	out := scrape(m)
	if got := findLine(t, out, "kv_get_hits_total "); !strings.HasSuffix(got, " 2") {
		t.Fatalf("hits: %q", got)
	}
	if got := findLine(t, out, "kv_get_misses_total "); !strings.HasSuffix(got, " 1") {
		t.Fatalf("misses: %q", got)
	}
}

func TestActiveConnectionsGoesUpAndDown(t *testing.T) {
	m := metrics.New()

	m.ConnOpened()
	m.ConnOpened()
	m.ConnOpened()
	m.ConnClosed()

	out := scrape(m)
	if got := findLine(t, out, "kv_connections_opened_total "); !strings.HasSuffix(got, " 3") {
		t.Fatalf("opened: %q", got)
	}
	if got := findLine(t, out, "kv_connections_active "); !strings.HasSuffix(got, " 2") {
		t.Fatalf("active: %q", got)
	}
}

// ── Histogram ─────────────────────────────────────────────────────────────

func TestHistogramCountAndSum(t *testing.T) {
	m := metrics.New()

	m.RecordLatency(1 * time.Millisecond)
	m.RecordLatency(2 * time.Millisecond)
	m.RecordLatency(3 * time.Millisecond)

	if got := m.Latency.Count(); got != 3 {
		t.Fatalf("count = %d, want 3", got)
	}

	out := scrape(m)
	if got := findLine(t, out, "kv_command_duration_seconds_count"); !strings.HasSuffix(got, " 3") {
		t.Fatalf("count line: %q", got)
	}
	// 1ms + 2ms + 3ms = 0.006s
	if got := findLine(t, out, "kv_command_duration_seconds_sum"); !strings.Contains(got, "0.006") {
		t.Fatalf("sum line: %q", got)
	}
}

func TestHistogramBucketsAreCumulative(t *testing.T) {
	m := metrics.New()

	// One observation in a low bucket, one high.
	m.RecordLatency(60 * time.Microsecond) // ≤ 100µs
	m.RecordLatency(200 * time.Millisecond)

	out := scrape(m)

	// Prometheus le semantics: each bucket includes everything below it.
	if got := findLine(t, out, `kv_command_duration_seconds_bucket{le="0.0001"}`); !strings.HasSuffix(got, " 1") {
		t.Fatalf("100µs bucket: %q", got)
	}
	if got := findLine(t, out, `kv_command_duration_seconds_bucket{le="+Inf"}`); !strings.HasSuffix(got, " 2") {
		t.Fatalf("+Inf bucket should hold every observation: %q", got)
	}
}

func TestQuantileLandsInTheRightBucket(t *testing.T) {
	m := metrics.New()

	// 99 fast observations and 1 slow one — p99 should be near the boundary,
	// p50 firmly in the fast range.
	for i := 0; i < 99; i++ {
		m.RecordLatency(60 * time.Microsecond)
	}
	m.RecordLatency(200 * time.Millisecond)

	p50 := m.Latency.Quantile(0.50)
	if p50 > 0.0001 {
		t.Fatalf("p50 = %v, expected ≤ 100µs", p50)
	}

	p99 := m.Latency.Quantile(0.99)
	if p99 > 0.0001 {
		t.Fatalf("p99 = %v, expected ≤ 100µs with 99%% fast samples", p99)
	}
}

func TestEmptyHistogramQuantileIsZero(t *testing.T) {
	m := metrics.New()
	if got := m.Latency.Quantile(0.99); got != 0 {
		t.Fatalf("empty histogram quantile = %v, want 0", got)
	}
}

// ── Gauges ────────────────────────────────────────────────────────────────

func TestRegisteredGaugeIsSampledAtScrapeTime(t *testing.T) {
	m := metrics.New()

	alive := 1.0
	m.RegisterGauge("kv_replica_alive", "Replica liveness", "replica",
		func() map[string]float64 {
			return map[string]float64{"10.0.0.1:6379": alive}
		})

	out := scrape(m)
	if got := findLine(t, out, `kv_replica_alive{replica="10.0.0.1:6379"}`); !strings.HasSuffix(got, " 1") {
		t.Fatalf("gauge: %q", got)
	}

	// Changing the underlying value must be reflected on the next scrape —
	// that is the point of sampling rather than recording.
	alive = 0
	out = scrape(m)
	if got := findLine(t, out, `kv_replica_alive{replica="10.0.0.1:6379"}`); !strings.HasSuffix(got, " 0") {
		t.Fatalf("gauge after change: %q", got)
	}
}

// ── HTTP endpoint ─────────────────────────────────────────────────────────

func TestHandlerServesPrometheusText(t *testing.T) {
	m := metrics.New()
	m.RecordCommand("PING")

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("content type = %q", ct)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "# TYPE kv_commands_total counter") {
		t.Fatalf("missing TYPE line:\n%s", body)
	}
	if !strings.Contains(body, `kv_commands_total{command="PING"} 1`) {
		t.Fatalf("missing PING counter:\n%s", body)
	}
}

// ── Concurrency ───────────────────────────────────────────────────────────

func TestConcurrentRecordingIsRaceFree(t *testing.T) {
	m := metrics.New()

	var wg sync.WaitGroup
	for w := 0; w < 40; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				m.RecordCommand("GET")
				m.RecordLatency(time.Duration(i) * time.Microsecond)
				m.RecordGet(i%2 == 0)
				m.ConnOpened()
				m.ConnClosed()
			}
		}()
	}

	// Scrape concurrently with recording — this is what a live Prometheus
	// scrape actually does.
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				scrape(m)
			}
		}
	}()

	wg.Wait()
	close(done)

	if got := m.Latency.Count(); got != 40*200 {
		t.Fatalf("observations = %d, want %d — lost updates under contention", got, 40*200)
	}

	out := scrape(m)
	if got := findLine(t, out, `kv_commands_total{command="GET"}`); !strings.HasSuffix(got, " 8000") {
		t.Fatalf("GET counter: %q", got)
	}
}
