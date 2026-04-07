package e2e

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func getStabilityRounds() int {
	if v := os.Getenv("STABILITY_ROUNDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 100
}

func getConcurrency() int {
	if v := os.Getenv("CONCURRENCY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 10
}

// TestLongRunningCRUD repeatedly executes Object CRUD in a loop to verify stability.
func TestLongRunningCRUD(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket
	rounds := getStabilityRounds()

	t.Logf("Starting long-running CRUD stability test: %d rounds", rounds)

	var totalDuration time.Duration
	successCount := 0

	for i := 0; i < rounds; i++ {
		key := fmt.Sprintf("%sstability-crud-%d-%d", testEnv.TestPrefix, time.Now().UnixNano(), i)
		content := fmt.Sprintf("stability test round %d", i)

		start := time.Now()

		// Put
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader(content),
		})
		if err != nil {
			t.Fatalf("Round %d: PutObject failed: %v", i, err)
		}

		// Get + verify
		getOut, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("Round %d: GetObject failed: %v", i, err)
		}
		body, _ := io.ReadAll(getOut.Body)
		getOut.Body.Close()
		if string(body) != content {
			t.Fatalf("Round %d: body mismatch: got %q, want %q", i, string(body), content)
		}

		// Delete
		_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("Round %d: DeleteObject failed: %v", i, err)
		}

		elapsed := time.Since(start)
		totalDuration += elapsed
		successCount++

		if (i+1)%10 == 0 || i == rounds-1 {
			avg := totalDuration / time.Duration(successCount)
			t.Logf("Round %d/%d complete (avg latency: %v)", i+1, rounds, avg)
		}
	}

	avg := totalDuration / time.Duration(successCount)
	t.Logf("Stability CRUD test complete: %d/%d rounds passed, avg latency: %v", successCount, rounds, avg)
}

// TestConcurrentOperations runs N goroutines each performing CRUD to verify
// the proxy handles concurrent data-plane requests without errors or data mixing.
func TestConcurrentOperations(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket
	concurrency := getConcurrency()

	t.Logf("Starting concurrent operations test: %d goroutines", concurrency)

	var wg sync.WaitGroup
	var failures atomic.Int64

	for g := 0; g < concurrency; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			key := fmt.Sprintf("%sconcurrent-%d-%d", testEnv.TestPrefix, goroutineID, time.Now().UnixNano())
			content := fmt.Sprintf("goroutine-%d-content", goroutineID)

			// Put
			_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   strings.NewReader(content),
			})
			if err != nil {
				t.Errorf("Goroutine %d: PutObject failed: %v", goroutineID, err)
				failures.Add(1)
				return
			}

			// Get + verify (no data mixing)
			getOut, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				t.Errorf("Goroutine %d: GetObject failed: %v", goroutineID, err)
				failures.Add(1)
				return
			}
			body, _ := io.ReadAll(getOut.Body)
			getOut.Body.Close()
			if string(body) != content {
				t.Errorf("Goroutine %d: DATA MIXING DETECTED! got %q, want %q", goroutineID, string(body), content)
				failures.Add(1)
				return
			}

			// Delete
			_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				t.Errorf("Goroutine %d: DeleteObject failed: %v", goroutineID, err)
				failures.Add(1)
				return
			}
		}(g)
	}

	wg.Wait()

	failCount := failures.Load()
	if failCount > 0 {
		t.Fatalf("Concurrent test had %d failures out of %d goroutines", failCount, concurrency)
	}
	t.Logf("All %d goroutines completed successfully", concurrency)
}

// TestControlPlaneConcurrency runs N goroutines doing PutBucketCors + GetBucketCors
// to verify the control plane doesn't crash under concurrent access.
func TestControlPlaneConcurrency(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket
	concurrency := getConcurrency()

	t.Cleanup(func() {
		client.DeleteBucketCors(context.TODO(), &s3.DeleteBucketCorsInput{
			Bucket: aws.String(bucket),
		})
	})

	t.Logf("Starting control plane concurrency test: %d goroutines", concurrency)

	var wg sync.WaitGroup
	var failures atomic.Int64

	for g := 0; g < concurrency; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			// PutBucketCors
			_, err := client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
				Bucket: aws.String(bucket),
				CORSConfiguration: &types.CORSConfiguration{
					CORSRules: []types.CORSRule{
						{
							AllowedMethods: []string{"GET"},
							AllowedOrigins: []string{fmt.Sprintf("https://g%d.example.com", goroutineID)},
						},
					},
				},
			})
			if err != nil {
				t.Errorf("Goroutine %d: PutBucketCors failed: %v", goroutineID, err)
				failures.Add(1)
				return
			}

			// GetBucketCors
			_, err = client.GetBucketCors(context.TODO(), &s3.GetBucketCorsInput{
				Bucket: aws.String(bucket),
			})
			if err != nil {
				t.Errorf("Goroutine %d: GetBucketCors failed: %v", goroutineID, err)
				failures.Add(1)
				return
			}
		}(g)
	}

	wg.Wait()

	failCount := failures.Load()
	if failCount > 0 {
		t.Fatalf("Control plane concurrency test had %d failures out of %d goroutines", failCount, concurrency)
	}
	t.Logf("All %d goroutines completed successfully", concurrency)
}
