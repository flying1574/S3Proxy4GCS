package e2e

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

// TestHealthEndpoint verifies GET /health returns 200 OK.
func TestHealthEndpoint(t *testing.T) {
	url := strings.TrimRight(testEnv.ProxyEndpoint, "/") + "/health"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET /health failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /health returned %d, expected 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	t.Logf("GET /health -> %d: %s", resp.StatusCode, string(body))
}

// TestReadyzEndpoint verifies GET /readyz returns 200 with JSON status.
func TestReadyzEndpoint(t *testing.T) {
	url := strings.TrimRight(testEnv.ProxyEndpoint, "/") + "/readyz"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET /readyz failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET /readyz returned %d: %s", resp.StatusCode, string(body))
	}

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)
	if !strings.Contains(bodyStr, "ready") {
		t.Errorf("GET /readyz body does not contain 'ready': %s", bodyStr)
	}
	t.Logf("GET /readyz -> %d: %s", resp.StatusCode, bodyStr)
}

// TestMetricsEndpoint verifies GET /metrics returns Prometheus metrics with s3proxy_ prefix.
func TestMetricsEndpoint(t *testing.T) {
	url := strings.TrimRight(testEnv.ProxyEndpoint, "/") + "/metrics"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET /metrics failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics returned %d, expected 200", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	// Verify our custom metrics are present
	requiredMetrics := []string{
		"s3proxy_http_requests_total",
		"s3proxy_http_request_duration_seconds",
	}
	optionalMetrics := []string{
		"s3proxy_gcs_api_duration_seconds", // Only appears after GCS calls
	}
	for _, metric := range requiredMetrics {
		if !strings.Contains(bodyStr, metric) {
			t.Errorf("GET /metrics missing required metric: %s", metric)
		}
	}
	for _, metric := range optionalMetrics {
		if !strings.Contains(bodyStr, metric) {
			t.Logf("GET /metrics: optional metric %s not yet present (expected if no GCS calls made)", metric)
		}
	}

	lines := strings.Count(bodyStr, "\n")
	t.Logf("GET /metrics -> %d (%d lines, all expected metrics present)", resp.StatusCode, lines)
}
