// Package metrics provides Prometheus metrics and an HTTP middleware for the
// s3proxy4gcs service.
//
// All metrics exported here are registered against the default Prometheus
// registry on package import, so callers only need to wire up the middleware
// via WithMetrics and expose promhttp.Handler() at /metrics.
package metrics

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Histogram buckets for request/response body sizes (bytes):
// 1KB, 10KB, 100KB, 1MB, 10MB, 100MB, 1GB.
var byteSizeBuckets = []float64{
	1 << 10,          // 1 KB
	10 << 10,         // 10 KB
	100 << 10,        // 100 KB
	1 << 20,          // 1 MB
	10 << 20,         // 10 MB
	100 << 20,        // 100 MB
	1 << 30,          // 1 GB
}

// All metrics are auto-registered against the default prometheus registry.
var (
	BytesReceivedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3proxy_bytes_received_total",
			Help: "Total number of request body bytes received by the s3proxy.",
		},
		[]string{"method", "endpoint"},
	)

	BytesSentTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3proxy_bytes_sent_total",
			Help: "Total number of response body bytes sent by the s3proxy.",
		},
		[]string{"method", "endpoint"},
	)

	RequestSizeBytes = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "s3proxy_request_size_bytes",
			Help:    "Distribution of request body sizes in bytes.",
			Buckets: byteSizeBuckets,
		},
		[]string{"method", "endpoint"},
	)

	ResponseSizeBytes = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "s3proxy_response_size_bytes",
			Help:    "Distribution of response body sizes in bytes.",
			Buckets: byteSizeBuckets,
		},
		[]string{"method", "endpoint"},
	)

	HTTPRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3proxy_http_requests_total",
			Help: "Total number of HTTP requests processed, labeled by method, endpoint and status code.",
		},
		[]string{"method", "endpoint", "status_code"},
	)

	HTTPRequestDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "s3proxy_http_request_duration_seconds",
			Help:    "HTTP request duration in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "endpoint"},
	)

	// GCSAPIDurationSeconds preserves the existing GCS SDK timing metric so
	// the move to this package does not drop observability we already had.
	GCSAPIDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "s3proxy_gcs_api_duration_seconds",
			Help:    "GCS SDK call duration in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation"},
	)
)

// classifyEndpoint returns a simplified S3 operation label based on the HTTP
// method and request URL. The mapping keeps cardinality low and avoids leaking
// bucket/object names into label values.
func classifyEndpoint(method, rawPath string) string {
	// Strip query string and leading slash.
	if i := strings.IndexByte(rawPath, '?'); i >= 0 {
		rawPath = rawPath[:i]
	}
	trimmed := strings.Trim(rawPath, "/")

	// Reserved operational endpoints never hit this middleware, but guard anyway.
	switch trimmed {
	case "health", "readyz", "metrics":
		return "other"
	}

	// Determine whether the path has a bucket and/or object key.
	// path style: /<bucket>/<key...>
	// virtual-hosted style: / (bucket in Host header) — treat as service-level,
	// which callers typically use for ListBuckets; we classify as "list".
	var hasBucket, hasKey bool
	if trimmed != "" {
		hasBucket = true
		if strings.Contains(trimmed, "/") {
			hasKey = true
		}
	}

	switch method {
	case http.MethodPut:
		if hasBucket && hasKey {
			return "put_object"
		}
		return "other"
	case http.MethodGet:
		if hasKey {
			return "get_object"
		}
		if hasBucket {
			return "list_objects"
		}
		return "list"
	case http.MethodDelete:
		return "delete_object"
	case http.MethodHead:
		return "head_object"
	default:
		return "other"
	}
}

// countingReadCloser wraps an io.ReadCloser and counts bytes read from it.
// It preserves streaming semantics: it never buffers the body.
type countingReadCloser struct {
	rc    io.ReadCloser
	count int64
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.rc.Read(p)
	if n > 0 {
		c.count += int64(n)
	}
	return n, err
}

func (c *countingReadCloser) Close() error { return c.rc.Close() }

// countingResponseWriter wraps http.ResponseWriter to capture status code and
// count bytes written to the response body. It implements http.Flusher and
// http.Hijacker when the underlying writer does.
type countingResponseWriter struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

func (w *countingResponseWriter) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}
	w.status = code
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(code)
}

func (w *countingResponseWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		// Implicit 200 OK — record it so the status_code label is accurate.
		w.status = http.StatusOK
		w.wroteHeader = true
	}
	n, err := w.ResponseWriter.Write(b)
	if n > 0 {
		w.bytes += int64(n)
	}
	return n, err
}

// Flush implements http.Flusher for upstream writers that stream responses.
func (w *countingResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap exposes the underlying ResponseWriter so Go 1.20+ helpers and
// middlewares (e.g. chi's middleware.Recoverer) can access features via
// http.ResponseController.
func (w *countingResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// WithMetrics wraps the given handler and records:
//   - bytes_received / request_size from the request body
//   - bytes_sent / response_size from the response body
//   - http_requests_total (method, endpoint, status_code)
//   - http_request_duration_seconds (method, endpoint)
//
// It is safe to use alongside any other middleware. The middleware swaps
// r.Body for a counting reader and wraps the ResponseWriter, so it does NOT
// buffer bodies and preserves streaming.
func WithMetrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		endpoint := classifyEndpoint(r.Method, r.URL.RequestURI())

		var reqCounter *countingReadCloser
		if r.Body != nil && r.Body != http.NoBody {
			reqCounter = &countingReadCloser{rc: r.Body}
			r.Body = reqCounter
		}

		rec := &countingResponseWriter{ResponseWriter: w, status: http.StatusOK}

		start := time.Now()
		next.ServeHTTP(rec, r)
		duration := time.Since(start)

		var reqBytes int64
		if reqCounter != nil {
			reqBytes = reqCounter.count
		}
		respBytes := rec.bytes

		BytesReceivedTotal.WithLabelValues(r.Method, endpoint).Add(float64(reqBytes))
		BytesSentTotal.WithLabelValues(r.Method, endpoint).Add(float64(respBytes))
		RequestSizeBytes.WithLabelValues(r.Method, endpoint).Observe(float64(reqBytes))
		ResponseSizeBytes.WithLabelValues(r.Method, endpoint).Observe(float64(respBytes))

		statusStr := strconv.Itoa(rec.status)
		HTTPRequestsTotal.WithLabelValues(r.Method, endpoint, statusStr).Inc()
		HTTPRequestDurationSeconds.WithLabelValues(r.Method, endpoint).Observe(duration.Seconds())
	})
}

// StatusCode returns the final status recorded by a counting response writer
// wrapped by WithMetrics. Returns 0 if w is not a wrapped writer.
//
// Intended for downstream middleware (e.g. structured loggers) that want to
// read the status without duplicating the ResponseWriter wrapping.
func StatusCode(w http.ResponseWriter) int {
	if crw, ok := w.(*countingResponseWriter); ok {
		return crw.status
	}
	return 0
}
