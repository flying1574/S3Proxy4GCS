// Package e2e provides shared test infrastructure for E2E acceptance tests.
//
// This file implements PrometheusCollector, which queries the in-cluster
// kube-prometheus-stack Prometheus instance to collect pod-level performance
// metrics for the s3proxy pods during a benchmark run.
//
// Unlike metrics_collector.go (which scrapes the proxy's /metrics endpoint
// directly), PrometheusCollector pulls already-scraped time-series from
// Prometheus. This lets us compute max/min/avg over an interval and also
// pull pod-level signals that the proxy itself doesn't expose
// (container_network_*, container_cpu_usage_seconds_total, etc.).
//
// Only standard library is used — no prometheus client dependency.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

// Stats captures the max / min / avg of a metric over a time interval.
type Stats struct {
	Max float64 `json:"max"`
	Min float64 `json:"min"`
	Avg float64 `json:"avg"`
}

// PodMetrics captures pod-level performance for a single benchmark run.
type PodMetrics struct {
	NetRxBps    Stats `json:"net_rx_bps"`    // inbound bandwidth, bytes/sec
	NetTxBps    Stats `json:"net_tx_bps"`    // outbound bandwidth, bytes/sec
	CPUCores    Stats `json:"cpu_cores"`     // CPU cores used
	MemoryMB    Stats `json:"memory_mb"`     // working-set memory, MB
	Goroutines  Stats `json:"goroutines"`    // goroutines in the proxy
	HTTPReqRate Stats `json:"http_req_rate"` // QPS

	// Totals over the interval (counter delta = end - start).
	StatusCodes map[string]float64 `json:"status_codes"` // e.g. {"200": 1234, "500": 2}
	EndpointOps map[string]float64 `json:"endpoint_ops"` // e.g. {"put_object": 500, ...}
}

// PrometheusCollector queries an in-cluster Prometheus HTTP API.
type PrometheusCollector struct {
	baseURL     string
	client      *http.Client
	podSelector string // label matcher substring, e.g. 'pod=~"s3proxy-.*",namespace="s3proxy-e2e"'
	enabled     bool
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

// NewPrometheusCollector builds a collector from PROMETHEUS_URL (defaulted
// to the kube-prometheus-stack in-cluster endpoint).
func NewPrometheusCollector() *PrometheusCollector {
	baseURL := os.Getenv("PROMETHEUS_URL")
	if baseURL == "" {
		baseURL = "http://monitoring-kube-prometheus-prometheus.monitoring.svc.cluster.local:9090"
	}
	return &PrometheusCollector{
		baseURL:     strings.TrimRight(baseURL, "/"),
		client:      &http.Client{Timeout: 10 * time.Second},
		podSelector: `namespace="s3proxy-e2e",pod=~"s3proxy-.*"`,
		enabled:     true,
	}
}

// ---------------------------------------------------------------------------
// Prometheus API response shapes
// ---------------------------------------------------------------------------

// promResponse is the minimal shape of /api/v1/query and /api/v1/query_range.
type promResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string          `json:"resultType"`
		Result     json.RawMessage `json:"result"`
	} `json:"data"`
	ErrorType string `json:"errorType,omitempty"`
	Error     string `json:"error,omitempty"`
}

// matrixEntry is a single time series from query_range.
type matrixEntry struct {
	Metric map[string]string `json:"metric"`
	Values [][2]interface{}  `json:"values"` // [ [ts, "value"], ... ]
}

// vectorEntry is a single sample from an instant query.
type vectorEntry struct {
	Metric map[string]string `json:"metric"`
	Value  [2]interface{}    `json:"value"` // [ts, "value"]
}

// ---------------------------------------------------------------------------
// Low-level query helpers
// ---------------------------------------------------------------------------

// QueryRange runs a range query and returns max/min/avg over all samples
// from all returned series.  On transport error or bad response, returns
// zero Stats + the error so the caller can decide to warn and continue.
func (pc *PrometheusCollector) QueryRange(ctx context.Context, query string, start, end time.Time, step time.Duration) (Stats, error) {
	if !pc.enabled {
		return Stats{}, nil
	}

	params := url.Values{}
	params.Set("query", query)
	params.Set("start", strconv.FormatInt(start.Unix(), 10))
	params.Set("end", strconv.FormatInt(end.Unix(), 10))
	params.Set("step", fmt.Sprintf("%ds", int(step.Seconds())))

	u := pc.baseURL + "/api/v1/query_range?" + params.Encode()

	body, err := pc.get(ctx, u)
	if err != nil {
		return Stats{}, err
	}

	var resp promResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return Stats{}, fmt.Errorf("decode: %w", err)
	}
	if resp.Status != "success" {
		return Stats{}, fmt.Errorf("prometheus error: %s: %s", resp.ErrorType, resp.Error)
	}

	var series []matrixEntry
	if len(resp.Data.Result) > 0 {
		if err := json.Unmarshal(resp.Data.Result, &series); err != nil {
			return Stats{}, fmt.Errorf("decode matrix: %w", err)
		}
	}

	// Collapse all samples from all series into one list.
	var all []float64
	for _, s := range series {
		for _, pt := range s.Values {
			v, ok := sampleFloat(pt[1])
			if !ok {
				continue
			}
			if math.IsNaN(v) {
				continue
			}
			all = append(all, v)
		}
	}

	return statsOf(all), nil
}

// QueryInstantDelta returns (value @ end) - (value @ start) for a scalar
// expression.  Used for counter totals over an interval.
func (pc *PrometheusCollector) QueryInstantDelta(ctx context.Context, query string, start, end time.Time) (float64, error) {
	if !pc.enabled {
		return 0, nil
	}
	startV, err := pc.queryInstantScalar(ctx, query, start)
	if err != nil {
		return 0, err
	}
	endV, err := pc.queryInstantScalar(ctx, query, end)
	if err != nil {
		return 0, err
	}
	return endV - startV, nil
}

// queryInstantLabeled runs an instant query and returns a map keyed by the
// given label (for `sum by (X) (...)` style queries).  Missing labels map
// to "unknown".
func (pc *PrometheusCollector) queryInstantLabeled(ctx context.Context, query string, t time.Time, labelName string) (map[string]float64, error) {
	if !pc.enabled {
		return map[string]float64{}, nil
	}

	params := url.Values{}
	params.Set("query", query)
	params.Set("time", strconv.FormatInt(t.Unix(), 10))
	u := pc.baseURL + "/api/v1/query?" + params.Encode()

	body, err := pc.get(ctx, u)
	if err != nil {
		return nil, err
	}

	var resp promResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if resp.Status != "success" {
		return nil, fmt.Errorf("prometheus error: %s: %s", resp.ErrorType, resp.Error)
	}

	var vec []vectorEntry
	if len(resp.Data.Result) > 0 {
		if err := json.Unmarshal(resp.Data.Result, &vec); err != nil {
			return nil, fmt.Errorf("decode vector: %w", err)
		}
	}

	out := map[string]float64{}
	for _, entry := range vec {
		key := entry.Metric[labelName]
		if key == "" {
			key = "unknown"
		}
		v, ok := sampleFloat(entry.Value[1])
		if !ok {
			continue
		}
		out[key] = v
	}
	return out, nil
}

// queryInstantScalar runs an instant query and returns the single scalar
// value (summing across any returned samples).
func (pc *PrometheusCollector) queryInstantScalar(ctx context.Context, query string, t time.Time) (float64, error) {
	params := url.Values{}
	params.Set("query", query)
	params.Set("time", strconv.FormatInt(t.Unix(), 10))
	u := pc.baseURL + "/api/v1/query?" + params.Encode()

	body, err := pc.get(ctx, u)
	if err != nil {
		return 0, err
	}

	var resp promResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, fmt.Errorf("decode: %w", err)
	}
	if resp.Status != "success" {
		return 0, fmt.Errorf("prometheus error: %s: %s", resp.ErrorType, resp.Error)
	}

	var vec []vectorEntry
	if len(resp.Data.Result) > 0 {
		if err := json.Unmarshal(resp.Data.Result, &vec); err != nil {
			return 0, fmt.Errorf("decode vector: %w", err)
		}
	}

	total := 0.0
	for _, entry := range vec {
		v, ok := sampleFloat(entry.Value[1])
		if !ok {
			continue
		}
		total += v
	}
	return total, nil
}

// get performs an HTTP GET with the collector's context + timeout.
func (pc *PrometheusCollector) get(ctx context.Context, urlStr string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, err
	}
	resp, err := pc.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("prometheus HTTP %d: %s", resp.StatusCode, truncate(string(body), 200))
	}
	return body, nil
}

// ---------------------------------------------------------------------------
// High-level collection
// ---------------------------------------------------------------------------

// Collect gathers the full PodMetrics for a benchmark interval.  The caller
// should pass an interval that already accounts for rate windows (i.e. a
// bit wider than the raw test run, see benchmark_test.go).  On any failure
// we log a warning and return a zero-valued PodMetrics — the benchmark
// itself must never fail because Prometheus was unreachable.
func (pc *PrometheusCollector) Collect(ctx context.Context, start, end time.Time) (PodMetrics, error) {
	out := PodMetrics{
		StatusCodes: map[string]float64{},
		EndpointOps: map[string]float64{},
	}
	if !pc.enabled {
		return out, nil
	}

	step := 5 * time.Second
	sel := pc.podSelector

	rangeQueries := []struct {
		target *Stats
		expr   string
	}{
		{
			&out.NetRxBps,
			fmt.Sprintf(`sum(rate(container_network_receive_bytes_total{%s}[30s]))`, sel),
		},
		{
			&out.NetTxBps,
			fmt.Sprintf(`sum(rate(container_network_transmit_bytes_total{%s}[30s]))`, sel),
		},
		{
			&out.CPUCores,
			fmt.Sprintf(`sum(rate(container_cpu_usage_seconds_total{%s,container!="POD",container!=""}[30s]))`, sel),
		},
		{
			&out.MemoryMB,
			fmt.Sprintf(`sum(container_memory_working_set_bytes{%s,container!="POD",container!=""}) / 1024 / 1024`, sel),
		},
		{
			&out.Goroutines,
			`sum(go_goroutines{job=~"s3proxy"})`,
		},
		{
			&out.HTTPReqRate,
			`sum(rate(s3proxy_http_requests_total[30s]))`,
		},
	}

	for _, q := range rangeQueries {
		s, err := pc.QueryRange(ctx, q.expr, start, end, step)
		if err != nil {
			log.Printf("[prometheus] range query failed (%s): %v", shortExpr(q.expr), err)
			continue
		}
		*q.target = s
	}

	// Counter deltas over the interval: status codes and endpoint ops.
	statusStart, err := pc.queryInstantLabeled(ctx, `sum by (status_code) (s3proxy_http_requests_total)`, start, "status_code")
	if err != nil {
		log.Printf("[prometheus] status_code @start failed: %v", err)
	}
	statusEnd, err := pc.queryInstantLabeled(ctx, `sum by (status_code) (s3proxy_http_requests_total)`, end, "status_code")
	if err != nil {
		log.Printf("[prometheus] status_code @end failed: %v", err)
	}
	for k, v := range diffMaps(statusStart, statusEnd) {
		if v != 0 {
			out.StatusCodes[k] = v
		}
	}

	epStart, err := pc.queryInstantLabeled(ctx, `sum by (endpoint) (s3proxy_http_requests_total)`, start, "endpoint")
	if err != nil {
		log.Printf("[prometheus] endpoint @start failed: %v", err)
	}
	epEnd, err := pc.queryInstantLabeled(ctx, `sum by (endpoint) (s3proxy_http_requests_total)`, end, "endpoint")
	if err != nil {
		log.Printf("[prometheus] endpoint @end failed: %v", err)
	}
	for k, v := range diffMaps(epStart, epEnd) {
		if v != 0 {
			out.EndpointOps[k] = v
		}
	}

	return out, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// sampleFloat parses a Prometheus sample value, which is encoded as a
// string in the JSON.
func sampleFloat(v interface{}) (float64, bool) {
	s, ok := v.(string)
	if !ok {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

// statsOf computes max/min/avg over a slice, rounded to 4 decimal places.
// Empty input yields zero stats.
func statsOf(xs []float64) Stats {
	if len(xs) == 0 {
		return Stats{}
	}
	min, max := xs[0], xs[0]
	sum := 0.0
	for _, v := range xs {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
		sum += v
	}
	avg := sum / float64(len(xs))
	return Stats{
		Max: round4(max),
		Min: round4(min),
		Avg: round4(avg),
	}
}

func round4(v float64) float64 {
	return math.Round(v*10000) / 10000
}

// diffMaps returns end - start per key (keys from either side included).
// Negative values (Prometheus counter reset) are clamped to 0.
func diffMaps(start, end map[string]float64) map[string]float64 {
	out := map[string]float64{}
	keys := map[string]struct{}{}
	for k := range start {
		keys[k] = struct{}{}
	}
	for k := range end {
		keys[k] = struct{}{}
	}
	for k := range keys {
		d := end[k] - start[k]
		if d < 0 {
			d = 0
		}
		out[k] = d
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

// shortExpr trims a PromQL expression for compact log output.
func shortExpr(e string) string {
	e = strings.ReplaceAll(e, "\n", " ")
	if len(e) > 60 {
		return e[:60] + "..."
	}
	return e
}
