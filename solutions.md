# Solutions for S3 to GCS Proxy

This document outlines the proposed solutions for resolving S3 features that are currently unsupported or have limited compatibility in Google Cloud Storage (GCS).

## Architecture Overview

The solution will be implemented as a **Middleware Proxy**. We are considering two primary deployment patterns:

1. **Centralized Proxy Service (e.g., Cloud Run)**
   - Deployed as a standalone microservice behind a load balancer. 
   - Handles all routing and heavy translation loads.

2. **GLB Service Extension (Callouts)**
   - Deployed directly at the Google Cloud Load Balancing (GLB) layer.
   - Intercepts requests using WebAssembly or external gRPC callouts.
   - **Pros**: Lower latency for pass-through traffic, tightly integrated with GLB routing.
   - **Cons**: More complex development model (Service Extensions API).

The proxy layer will handle:
1. **Request Interception**: Identifying subresources (like `?tagging`, `?lifecycle`) that require special handling.
2. **Body Translation**: Converting S3 XML schemas to GCS-compatible formats (XML or JSON).
3. **Authentication**: Re-signing requests before forwarding to GCS.



### Feature Feasibility by Proxy Type

Choosing between a GLB Extension and a Cloud Run proxy depends on whether the feature requires stateless header modification, payload translation, or stateful orchestration:

**🟢 Perfect for GLB Extensions (Header/Routing Modification)**
- **Versioning Interop**: Injecting the `x-amz-interop-list-objects-format: enabled` header upon request and mapping `x-goog-generation` to `x-amz-version-id` on the response.
- **RestoreObject (Synthetic Responses)**: Immediately returning `200 OK` without hitting GCS, since objects are "live".
- **Proxy Protection (ABAC)**: Inspecting the URL to proactively reject unsupported requests like `PUT ?policy` with `501 Not Implemented`.
- **Transparent Tag Translation**: Rewriting standard S3 `x-amz-tagging` upload headers into GCS custom metadata `x-goog-meta-s3tag-` instantly.

**🟡 Difficult for GLB Extensions (Requires Body Translation & Re-signing)**
- **XML Parsing (Lifecycle, CORS, Logging)**: Modifying the XML body invalidates the original AWS v4 signature. Generating a new GCP HMAC signature within a load balancer extension is heavily resource-intensive and risks hitting execution timeouts. Best routed to a dedicated Cloud Run instance.

**🔴 Unsuitable for GLB Extensions (Stateful/Orchestration)**
- **DeleteObjects (Fan-out)**: Cannot fan-out single requests into up to 1,000 separate GCS API calls and aggregate responses.
- **S3 `?tagging` API**: The read-modify-write cycle (GET metadata -> merge -> PUT) is too slow for load balancer inline validation.
- **Upload Part Copy**: Exceeds memory/streaming limits due to required buffering.

---

## Proposed Solutions per Feature

We have redefined the feature categories based on **latency impact, proxy resource consumption (CPU/Memory), and architectural complexity**. The "Hard" group requires deeper discussion with the customer to align on performance and cost trade-offs.

### Group A: Control-Plane Configuration (Easy / Low Impact)
These features are infrequent bucket management API calls. The proxy only needs to parse XML/JSON payloads and swap schemas. They **do not** impact the latency, memory, or bandwidth of heavy data-plane object transfers.

#### 1. Static Website Configuration
**Decision**: Stateless configuration mapping.
- **CORS**: **[Implemented ✅]** Translate `CORSConfiguration` XML to GCS CORS JSON bucket settings. Full PUT/GET/DELETE CRUD with bi-directional translation.
- **Logging**: **[Implemented ✅]** Map S3 bucket logging to GCS TargetBucket/Prefix using Go SDK. Full PUT/GET/DELETE CRUD.
- **Website**: **[Implemented ✅]** Map `IndexDocument`/`ErrorDocument` to GCS website fields. Full PUT/GET/DELETE CRUD.

#### 2. Lifecycle Management — **[Implemented ✅]**
**Decision**: **Translation Layer**. The proxy intercepts `PUT/GET/DELETE /?lifecycle` and maps S3 actions (Expiration, Transition) to GCS Bucket Lifecycle configuration. Bi-directional translation (S3 XML ↔ GCS JSON) with full CRUD support. Rejects unsupported filters (Size, Tags) to prevent scope broadening.

### Group B: Data-Plane & Stateful Operations (Hard / High Impact)
These features intercept high-frequency data path operations or require heavy background processing. They introduce significant latency, require high proxy resources (memory/connections), or involve complex race conditions.

#### 1. Tagging (Object Tagging) — **[Implemented ✅]**
**Issue**: GCS lacks an exact `?tagging` equivalent and relies on object metadata. 
**Implementation**: The Proxy transparently translates `PUT ?tagging` requests directly into GCS Object Custom Metadata (`x-goog-meta-s3tag-`). It uses a read-modify-write cycle with **Optimistic Concurrency Control (IfMetagenerationMatch)** to prevent lost updates safely without heavy locking.

#### 2. Access Control (ACLs & Policies & Tag-Based ABAC) - **[Deferred]**
**Issue**: S3 uses XML ACLs, JSON Bucket Policies. GCS uses IAM.
**Proxy Impact**: Extreme Latency / Infeasible on Data Path for tag-based ABAC.
**Recommendation**: Shift to prefix-based security rather than object-level tags.

#### 3. DeleteObjects (Multi-Object Delete) - **[Deferred]**
**Issue**: GCS S3-compatible API does not support bulk `DeleteObjects`.
**Proxy Impact**: Resource Exhaustion (Fan-out). Requires concurrent GCS delete calls in proxy.

#### 5. Inventory Data Manifests - **[Deferred]**
**Issue**: Automations expect specific S3 Inventory output formats.
**Proxy Impact**: Requires External Stateful ETL Worker.

#### 6. Flexible Checksums (aws-chunked unwrapping) - **[Deferred]**
**Issue**: Modern SDKs use `aws-chunked` framing for checksum trailers, unsupported by GCS.
**Proxy Impact**: Extreme Memory/Bandwidth Overhead. Unwrapping requires heavy stream parsing and may limit high-speed transparent data-plane throughput.
**Recommendation**: Use client-side `AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_REQUIRED` or use standard `Content-MD5` headers for integrity.

---

## SDK Compatibility & Client-Side Workarounds

Recent testing across multiple AWS SDKs (Python, Java V1/V2, C++, Go V1/V2) against GCS surfaced several compatibility nuances that can be addressed directly via client configuration, reducing the translation burden on the proxy.

### 1. Checksums and PutObject (Signature Mismatch)
**Issue**: Modern SDKs (Python, Java V2, Go V2) default to "Flexible Checksums" (trailers) using algorithms like CRC32/CRC32C. This wraps payloads in `aws-chunked` format, which GCS does not support, resulting in `SignatureDoesNotMatch` errors.
**Solution**: 
- Set the `AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_REQUIRED` environment variable to bypass unexpected checksum framing.
- **Alternative (High Integrity)**: Configure the client to skip automatic checksums and manually compute `ContentMD5`.
*(Note: Java V1 and C++ natively use standard `Content-MD5` and work seamlessly).*

### 2. Java V2 CopyObject (`411 Length Required`)
**Issue**: The default `UrlConnectionHttpClient` in the Java V2 SDK incorrectly omits the `Content-Length: 0` header on empty `PUT` requests, causing GCS to reject `CopyObject`.
**Solution**: Explicitly bind the alternative `ApacheHttpClient` in the Java V2 client configuration, which correctly transmits the `Content-Length` header.

### 3. RestoreObject
**Issue**: Throws `InvalidArgument` against GCS.
**Solution**: GCS objects in archive classes are considered "live" and do not require restoration. Client applications should remove calls to `RestoreObject`, or the proxy can be configured to intercept and return a synthetic `200 OK`.

### 4. Storage Classes
**Issue**: GCS rejects AWS-specific storage class values (e.g., `STANDARD_IA`, `GLACIER`).
**Solution (Validated)**: The proxy transparently translates AWS storage classes to GCS equivalents before forwarding:
- `STANDARD_IA` / `ONEZONE_IA` -> `NEARLINE`
- `GLACIER_IR` (Instant Retrieval) -> `COLDLINE`
- `GLACIER` / `DEEP_ARCHIVE` -> `ARCHIVE`
- `INTELLIGENT_TIERING` -> `AUTOCLASS`
- Standard falls back to `STANDARD`.
*(Note: Client can still pass GCS-native strings directly if preferred, but standard AWS SDK values are now supported).*

### 5. Versioning (ListObjectVersions / HeadObject)
**Issue**: GCS uses `<Generation>` instead of `<VersionId>`.
**Proxy Solution (Validated)**: 
- Intercept `ListObjectVersions` and inject the header `x-amz-interop-list-objects-format: enabled` to force S3-compatible XML.
- Intercept responses for metadata operations (like `HeadObject`) and map the `x-goog-generation` response header back to `x-amz-version-id`.

---

### 6. Explicit Client Transport Routing Strategy

**Issue**: Customers prefer scoped routing over global environment variables, or want to avoid setting system-wide `HTTP_PROXY` which might affect other services.
**Solution**: Use a custom `http.Transport` with `DialContext` when initializing the S3 SDK client.
- **How it works**: The S3 SDK signs requests for `storage.googleapis.com` (preserving signature integrity). The underlying `DialContext` overrides the connection to route to the local proxy (`localhost:8081`).
- **Scope**: Isolated purely to the S3 Client instance. No side effects for other services or standard HTTP requests using `http.DefaultClient`.

---

## Technical Considerations

### Authentication & Re-signing
When the proxy modifies a request body (e.g., translating Lifecycle XML), the original AWS V4 signature becomes invalid because the payload hash changes.
- **Requirement**: The proxy must possess a GCS Service Account HMAC key to **re-sign** requests before forwarding them to GCS.
- **Workflow**: 
  1. Validate incoming S3 signature.
  2. Modify request (translate body).
  3. Generate new signature with proxy's HMAC keys.
  4. Forward to GCS.

### HTTP vs HTTPS Tradeoffs in Internal Networks

When deploying the proxy in a private VPC, you can choose between unencrypted HTTP or HTTPS for both inbound (Client to Proxy) and outbound (Proxy to GCS) traffic.

#### ⚖️ Summary Comparison:
| Type | Performance | Security | Network Integrity |
| :--- | :--- | :--- | :--- |
| **HTTP (Unencrypted)** | 🚀 **Highest** (No TLS handshake overhead) | ❌ **Low** (Sniffable if VPC is breached) | ⚠️ **TCP Checksum only** (Weak) |
| **HTTPS (HTTP/2 with TLS 1.3)** | ⚡ **Fast** (Multiplexed requests via single connection) | ✅ **High** (Encrypted in transit) | ✅ **AEAD MAC Protection** (Prevents bit-flips during transit) |

#### Recommendations:
1. **Outbound (Proxy to GCS)**: Use **HTTPS with HTTP/2** (Default: `https://storage.googleapis.com`). This ensures that data leaving your proxy environment is encrypted and protected against bit-flips by TLS’s native integrity checks (AEAD), even if application-level checksums (`aws-chunked`) are disabled for speed.
2. **Inbound (Client to Proxy)**: In a trusted internal VPC, **unencrypted HTTP** is often preferred to save TLS handshake latency for every client connect.

---

---

## Implementation Status Summary

| Feature | Status | Scope |
|---|---|---|
| **Lifecycle** (PUT/GET/DELETE) | ✅ Implemented | Bi-directional S3 XML ↔ GCS JSON, unsupported filter rejection |
| **CORS** (PUT/GET/DELETE) | ✅ Implemented | S3 XML ↔ GCS CORS struct translation |
| **Logging** (PUT/GET/DELETE) | ✅ Implemented | S3 XML ↔ GCS BucketLogging |
| **Website** (PUT/GET/DELETE) | ✅ Implemented | IndexDocument/ErrorDocument mapping |
| **Tagging** (PUT/GET/DELETE) | ✅ Implemented | GCS metadata with OCC (IfMetagenerationMatch) |
| **Storage Class Translation** | ✅ Implemented | STANDARD_IA→NEARLINE, GLACIER→ARCHIVE, etc. |
| **Versioning Interop** | ✅ Implemented | x-goog-generation ↔ x-amz-version-id mapping |
| **SigV4 Re-signing** | ✅ Implemented | Automatic re-sign on header/query modification |
| **Reverse Proxy (Data Plane)** | ✅ Implemented | Streaming proxy with connection pooling |
| **Observability** | ✅ Implemented | Prometheus metrics, structured JSON logging (slog), /health, /readyz |
| **DryRun Mode** | ✅ Implemented | Local dev without real GCS hits |
| **Graceful Shutdown** | ✅ Implemented | SIGTERM/SIGINT with 10s drain |
| **Unit Tests** | ✅ Implemented | 17 tests across all translate modules |
| **Integration Tests** | ✅ Implemented | Isolated Go module, auto-spawns local proxy |
| **E2E Acceptance Tests** | ✅ Implemented | Functional + Stability + Benchmark suites |
| **CI/CD (GitHub Actions)** | ✅ Implemented | Manual-trigger workflow with 3 parallel jobs |
| **ACLs & Policies** | ⏸ Deferred | IAM model mismatch, recommend prefix-based security |
| **DeleteObjects (Bulk)** | ⏸ Deferred | Fan-out resource exhaustion risk |
| **Inventory Manifests** | ⏸ Deferred | Requires external stateful ETL worker |
| **Flexible Checksums** | ⏸ Deferred | Client-side `WHEN_REQUIRED` workaround available |

---

## Open Questions & Next Steps

1. **Target SDKs Compatibility**: The target SDKs are **Go, Java, Python, and C++**. We must ensure the proxy's XML formatting strictly adheres to what these specific SDKs expect (e.g., namespace prefixes, exact header values).
2. **Consistency Requirements**: For features like Tagging via Metadata, is the eventual consistency of GCS metadata acceptable for the customer's application logic?
3. **Performance Hardening**: Consider implementing request body size limits (`MaxBytesReader`), server-level timeouts, and concurrency limiting middleware.
4. **E2E Validation**: Run the E2E acceptance test suite against a live GKE deployment to validate all translations end-to-end.

---

## Architecture decisions (Cloud Service & Language)

### 1. Cloud Service: Cloud Run vs GKE
To serve high concurrent requests robustly, we evaluate:

| Feature | Cloud Run | GKE (Google Kubernetes Engine) |
| :--- | :--- | :--- |
| **Ops Overhead** | Low (Serverless) | High (Requires cluster management) |
| **Scaling** | Instant, request-based | Metric-based (HPA), can pre-warm |
| **Latency Tails** | Potential cold starts (mitigated by `min-instances`) | Zero cold starts (warm pools) |
| **Connection Pooling** | Harder to tune kernel-level limits | Ultimate control over TCP/IP tuning |
| **Cost at Scale** | Linear with requests | Dense utilization is cheaper |

**Recommendation**: 
- **Start with Cloud Run** using `min-instances` and `CPU always allocated`. It is often fast enough (especially using Go) and avoids the massive overhead of K8s.
- **Pivot to GKE** if we hit hard limits on TCP connection reuse, or if the sheer volume makes sustained VM pools significantly cheaper than Serverless compute.

### 2. Coding Language
For a high-performance, low-latency proxy:

- **Go (Golang)**: **Recommended**. 
  - Standard for cloud infrastructure (Kubernetes, Docker, Envoy are Go/C++).
  - Excellent concurrency primitives (Goroutines) for handling multi-delete fan-outs.
  - Low memory footprint and fast startup.
  - Native GCP and AWS SDK support is top-tier.
- **Rust**: Highest performance, but slower development velocity and steeper learning curve.
- **Java/C#**: Heavy runtimes, GC pauses (bad for sub-100ms proxy latency tails), and higher memory usage.

**Decision**: **Go** is the sweet spot for performance, maintainability, and ecosystem support.
