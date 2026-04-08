// Package e2e provides shared test infrastructure for E2E acceptance tests.
package e2e

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// SystemSnapshot captures proxy-side resource metrics at a point in time.
type SystemSnapshot struct {
	Timestamp         string  `json:"timestamp"`
	CPUSecondsTotal   float64 `json:"cpu_seconds_total"`
	ResidentMemoryMB  float64 `json:"resident_memory_mb"`
	Goroutines        float64 `json:"goroutines"`
	HeapAllocMB       float64 `json:"heap_alloc_mb"`
	HeapInUseMB       float64 `json:"heap_in_use_mb"`
	OpenFDs           float64 `json:"open_fds"`
	HTTPRequestsTotal float64 `json:"http_requests_total"`
	GCSAPICalls       float64 `json:"gcs_api_calls_total"`
}

// SystemDelta represents the change between two snapshots.
type SystemDelta struct {
	DurationSec       float64        `json:"duration_sec"`
	CPUUsagePercent   float64        `json:"cpu_usage_percent"` // CPU delta / wall-clock delta * 100
	MemoryDeltaMB     float64        `json:"memory_delta_mb"`   // resident memory change
	PeakResidentMB    float64        `json:"peak_resident_mb"`  // max of before/after
	GoroutineDelta    float64        `json:"goroutine_delta"`
	HeapAllocDeltaMB  float64        `json:"heap_alloc_delta_mb"`
	HTTPRequestsDelta float64        `json:"http_requests_delta"`
	GCSAPICallsDelta  float64        `json:"gcs_api_calls_delta"`
	Before            SystemSnapshot `json:"before"`
	After             SystemSnapshot `json:"after"`
}

// MetricsCollector fetches Prometheus metrics from the proxy's /metrics endpoint.
type MetricsCollector struct {
	metricsURL string
	client     *http.Client
}

// NewMetricsCollector creates a collector pointed at the proxy endpoint.
func NewMetricsCollector(proxyEndpoint string) *MetricsCollector {
	return &MetricsCollector{
		metricsURL: strings.TrimRight(proxyEndpoint, "/") + "/metrics",
		client:     &http.Client{Timeout: 10 * time.Second},
	}
}

// Snapshot fetches current metrics and returns a SystemSnapshot.
func (mc *MetricsCollector) Snapshot() (SystemSnapshot, error) {
	resp, err := mc.client.Get(mc.metricsURL)
	if err != nil {
		return SystemSnapshot{}, fmt.Errorf("failed to fetch metrics: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return SystemSnapshot{}, fmt.Errorf("failed to read metrics body: %w", err)
	}

	text := string(body)
	snap := SystemSnapshot{
		Timestamp:         time.Now().UTC().Format(time.RFC3339),
		CPUSecondsTotal:   parseMetric(text, "process_cpu_seconds_total"),
		ResidentMemoryMB:  parseMetric(text, "process_resident_memory_bytes") / (1024 * 1024),
		Goroutines:        parseMetric(text, "go_goroutines"),
		HeapAllocMB:       parseMetric(text, "go_memstats_alloc_bytes") / (1024 * 1024),
		HeapInUseMB:       parseMetric(text, "go_memstats_heap_inuse_bytes") / (1024 * 1024),
		OpenFDs:           parseMetric(text, "process_open_fds"),
		HTTPRequestsTotal: sumCounterVec(text, "s3proxy_http_requests_total"),
		GCSAPICalls:       sumHistogramCount(text, "s3proxy_gcs_api_duration_seconds"),
	}
	return snap, nil
}

// ComputeDelta calculates the difference between two snapshots.
func ComputeDelta(before, after SystemSnapshot, wallClock time.Duration) SystemDelta {
	durSec := wallClock.Seconds()
	cpuDelta := after.CPUSecondsTotal - before.CPUSecondsTotal
	cpuPercent := 0.0
	if durSec > 0 {
		cpuPercent = (cpuDelta / durSec) * 100
	}

	return SystemDelta{
		DurationSec:       math.Round(durSec*100) / 100,
		CPUUsagePercent:   math.Round(cpuPercent*100) / 100,
		MemoryDeltaMB:     math.Round((after.ResidentMemoryMB-before.ResidentMemoryMB)*100) / 100,
		PeakResidentMB:    math.Max(before.ResidentMemoryMB, after.ResidentMemoryMB),
		GoroutineDelta:    after.Goroutines - before.Goroutines,
		HeapAllocDeltaMB:  math.Round((after.HeapAllocMB-before.HeapAllocMB)*100) / 100,
		HTTPRequestsDelta: after.HTTPRequestsTotal - before.HTTPRequestsTotal,
		GCSAPICallsDelta:  after.GCSAPICalls - before.GCSAPICalls,
		Before:            before,
		After:             after,
	}
}

// parseMetric extracts a single gauge/counter value from Prometheus text format.
// Matches lines like: metric_name 123.45
func parseMetric(text, name string) float64 {
	// Match exact metric name (no labels) at start of line
	re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(name) + `\s+([\d.eE+\-]+)`)
	match := re.FindStringSubmatch(text)
	if match == nil {
		return 0
	}
	v, err := strconv.ParseFloat(match[1], 64)
	if err != nil {
		return 0
	}
	return v
}

// sumCounterVec sums all label combinations of a counter metric.
// Matches lines like: metric_name{label="value",...} 123
func sumCounterVec(text, name string) float64 {
	re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(name) + `\{[^}]*\}\s+([\d.eE+\-]+)`)
	matches := re.FindAllStringSubmatch(text, -1)
	total := 0.0
	for _, m := range matches {
		v, err := strconv.ParseFloat(m[1], 64)
		if err == nil {
			total += v
		}
	}
	return total
}

// sumHistogramCount sums _count entries of a histogram metric.
func sumHistogramCount(text, name string) float64 {
	countName := name + "_count"
	re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(countName) + `\{[^}]*\}\s+([\d.eE+\-]+)`)
	matches := re.FindAllStringSubmatch(text, -1)
	total := 0.0
	for _, m := range matches {
		v, err := strconv.ParseFloat(m[1], 64)
		if err == nil {
			total += v
		}
	}
	// Also try without labels
	total += parseMetric(text, countName)
	return total
}
