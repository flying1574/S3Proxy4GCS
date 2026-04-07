package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// BenchmarkReport holds the aggregate benchmark results for JSON export.
type BenchmarkReport struct {
	Timestamp     string            `json:"timestamp"`
	ProxyEndpoint string            `json:"proxy_endpoint"`
	Results       []BenchmarkResult `json:"results"`
}

// BenchmarkResult holds a single benchmark's statistics.
type BenchmarkResult struct {
	Name      string  `json:"name"`
	Ops       int     `json:"total_ops"`
	OpsPerSec float64 `json:"ops_per_sec"`
	P50Ms     float64 `json:"p50_ms"`
	P95Ms     float64 `json:"p95_ms"`
	P99Ms     float64 `json:"p99_ms"`
	AvgMs     float64 `json:"avg_ms"`
}

// collectLatencies runs fn n times, collecting latencies, and returns sorted durations.
func collectLatencies(n int, fn func() error) ([]time.Duration, error) {
	latencies := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		start := time.Now()
		if err := fn(); err != nil {
			return latencies, fmt.Errorf("iteration %d: %w", i, err)
		}
		latencies = append(latencies, time.Since(start))
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	return latencies, nil
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func avgDuration(sorted []time.Duration) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	var total time.Duration
	for _, d := range sorted {
		total += d
	}
	return total / time.Duration(len(sorted))
}

func toBenchmarkResult(name string, latencies []time.Duration, totalDur time.Duration) BenchmarkResult {
	ops := len(latencies)
	opsPerSec := float64(ops) / totalDur.Seconds()
	return BenchmarkResult{
		Name:      name,
		Ops:       ops,
		OpsPerSec: opsPerSec,
		P50Ms:     float64(percentile(latencies, 0.50).Microseconds()) / 1000.0,
		P95Ms:     float64(percentile(latencies, 0.95).Microseconds()) / 1000.0,
		P99Ms:     float64(percentile(latencies, 0.99).Microseconds()) / 1000.0,
		AvgMs:     float64(avgDuration(latencies).Microseconds()) / 1000.0,
	}
}

const benchmarkIterations = 50

// TestBenchmarkSuite runs all benchmarks and outputs a JSON report.
// Using Test prefix (not Benchmark) so it runs with `go test -run` and can output custom JSON.
func TestBenchmarkSuite(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket

	report := BenchmarkReport{
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		ProxyEndpoint: testEnv.ProxyEndpoint,
	}

	// --- Benchmark: PutObject 1KB ---
	t.Log("Benchmark: PutObject_1KB...")
	putKey := GenerateTestKey(testEnv, "bench-put")
	t.Cleanup(func() { Cleanup(t, client, bucket, putKey) })

	start := time.Now()
	putLatencies, err := collectLatencies(benchmarkIterations, func() error {
		_, e := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(putKey),
			Body:   strings.NewReader(strings.Repeat("X", 1024)),
		})
		return e
	})
	if err != nil {
		t.Fatalf("PutObject benchmark failed: %v", err)
	}
	putResult := toBenchmarkResult("PutObject_1KB", putLatencies, time.Since(start))
	report.Results = append(report.Results, putResult)
	t.Logf("  PutObject_1KB: ops/s=%.1f p50=%.1fms p95=%.1fms p99=%.1fms", putResult.OpsPerSec, putResult.P50Ms, putResult.P95Ms, putResult.P99Ms)

	// --- Benchmark: GetObject 1KB ---
	t.Log("Benchmark: GetObject_1KB...")
	// Object already exists from PutObject benchmark
	start = time.Now()
	getLatencies, err := collectLatencies(benchmarkIterations, func() error {
		out, e := client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(putKey),
		})
		if e != nil {
			return e
		}
		io.Copy(io.Discard, out.Body)
		out.Body.Close()
		return nil
	})
	if err != nil {
		t.Fatalf("GetObject benchmark failed: %v", err)
	}
	getResult := toBenchmarkResult("GetObject_1KB", getLatencies, time.Since(start))
	report.Results = append(report.Results, getResult)
	t.Logf("  GetObject_1KB: ops/s=%.1f p50=%.1fms p95=%.1fms p99=%.1fms", getResult.OpsPerSec, getResult.P50Ms, getResult.P95Ms, getResult.P99Ms)

	// --- Benchmark: PutGetDelete full CRUD cycle ---
	t.Log("Benchmark: PutGetDelete_CRUD...")
	start = time.Now()
	crudLatencies, err := collectLatencies(benchmarkIterations, func() error {
		crudKey := fmt.Sprintf("%sbench-crud-%d", testEnv.TestPrefix, time.Now().UnixNano())
		// Put
		_, e := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(crudKey),
			Body:   strings.NewReader(strings.Repeat("Y", 1024)),
		})
		if e != nil {
			return e
		}
		// Get
		out, e := client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(crudKey),
		})
		if e != nil {
			return e
		}
		io.Copy(io.Discard, out.Body)
		out.Body.Close()
		// Delete
		_, e = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(crudKey),
		})
		return e
	})
	if err != nil {
		t.Fatalf("CRUD benchmark failed: %v", err)
	}
	crudResult := toBenchmarkResult("PutGetDelete_CRUD", crudLatencies, time.Since(start))
	report.Results = append(report.Results, crudResult)
	t.Logf("  PutGetDelete_CRUD: ops/s=%.1f p50=%.1fms p95=%.1fms p99=%.1fms", crudResult.OpsPerSec, crudResult.P50Ms, crudResult.P95Ms, crudResult.P99Ms)

	// --- Benchmark: ControlPlane PutBucketLifecycle ---
	t.Log("Benchmark: PutBucketLifecycle...")
	t.Cleanup(func() {
		client.DeleteBucketLifecycle(context.TODO(), &s3.DeleteBucketLifecycleInput{
			Bucket: aws.String(bucket),
		})
	})

	start = time.Now()
	lcLatencies, err := collectLatencies(benchmarkIterations, func() error {
		_, e := client.PutBucketLifecycleConfiguration(context.TODO(), &s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(bucket),
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{
				Rules: []types.LifecycleRule{
					{
						ID:     aws.String("bench-rule"),
						Status: types.ExpirationStatusEnabled,
						Filter: &types.LifecycleRuleFilter{Prefix: aws.String("bench/")},
						Expiration: &types.LifecycleExpiration{
							Days: aws.Int32(90),
						},
					},
				},
			},
		})
		return e
	})
	if err != nil {
		t.Fatalf("Lifecycle benchmark failed: %v", err)
	}
	lcResult := toBenchmarkResult("PutBucketLifecycle", lcLatencies, time.Since(start))
	report.Results = append(report.Results, lcResult)
	t.Logf("  PutBucketLifecycle: ops/s=%.1f p50=%.1fms p95=%.1fms p99=%.1fms", lcResult.OpsPerSec, lcResult.P50Ms, lcResult.P95Ms, lcResult.P99Ms)

	// --- Write JSON report ---
	reportJSON, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal benchmark report: %v", err)
	}

	reportPath := "benchmark_report.json"
	if err := os.WriteFile(reportPath, reportJSON, 0644); err != nil {
		t.Logf("Warning: failed to write benchmark report to %s: %v", reportPath, err)
	} else {
		t.Logf("Benchmark report written to %s", reportPath)
	}

	// Print summary table
	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("  Benchmark Summary")
	fmt.Println("========================================")
	fmt.Printf("  %-25s %8s %8s %8s %8s %8s\n", "Name", "Ops", "ops/s", "p50(ms)", "p95(ms)", "p99(ms)")
	fmt.Println("  " + strings.Repeat("-", 73))
	for _, r := range report.Results {
		fmt.Printf("  %-25s %8d %8.1f %8.1f %8.1f %8.1f\n", r.Name, r.Ops, r.OpsPerSec, r.P50Ms, r.P95Ms, r.P99Ms)
	}
	fmt.Println("========================================")
}
