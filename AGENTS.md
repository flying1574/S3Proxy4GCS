# Autonomous Agent Context (AGENTS.md)

This file contains instructions and context for AI coding assistants working on the `s3proxy4gcs` repository.

## Project Vision

The goal is to serve as a transparent middleware for S3 protocols to translate unsupported features into GCS APIs seamlessly.

## Engineering Rules

1.  **Zero Tolerance for Syntax Errors**: Before committing or saving, ensure bracket matching and interface compliance is correct.
2.  **Centralized Configuration**: All environment variables and settings must be managed in `config/settings.go`. Use `.env` file for local development.
3.  **Documentation Sync**: Update `README.md` and `AGENTS.md` whenever the project footprint (ports, dependencies, paths) changes.
4.  **Reject Unsupported Filters**: Reject lifecycle rules using unsupported filters (Size, Tags) to prevent accidental over-deletion in GCS (Scope Broadening).
5.  **Full Scope Search**: Before implement translation, search official AWS S3 SDK for full parameters. Enforce strict type validation and test both valid and invalid fields.
6.  **Full Reverse Proxy**: The proxy handles all traffic by default using standard Go `httputil.NewSingleHostReverseProxy`. For data-plane operations (`GET`/`PUT` objects), ensure streaming behavior is preserved (do not read the entire body into memory). Tune `http.Transport` connection pools (`MaxIdleConns`, `MaxIdleConnsPerHost`) for high concurrency.
7.  **Context Propagation**: Always use the request's context (`r.Context()`) for outbound GCS API calls (e.g. `bucket.Update()`). If the client aborts, the outbound GCS call automatically cancels to save compute/cost.
8.  **Standard S3 Errors**: Use the `writeS3Error` helper to respond with standard AWS S3 XML error formats. Do not use plain text `http.Error` as SDK clients expect XML.
9.  **Structured JSON Logging**: When logging, use standard Go 1.21's `log/slog` module instead of standard `log.Printf`. Use semantic levels (`Info`, `Error`, `Debug`) and use keyword arguments (e.g., `slog.Info("msg", "key", val)`) to ensure parsed compatibility with Cloud Logging.
10. **Guaranteed QoS for K8s**: All K8s deployments must set `requests == limits` (Guaranteed QoS class) for both CPU and memory. Burstable QoS causes CFS throttling on overcommitted GKE Autopilot nodes, resulting in 30~60% throughput loss. Use Pod anti-affinity to separate proxy and client/benchmark Pods onto different nodes.

## Environment Layout

- `main.go`: Entry point for the Chi router setup.
- `config/settings.go`: Parameter load path.
- `pkg/translate`: Location for XML translation logic (implements S3 Lifecycle).
- `.env`: Secret bind template. Use `GCS_PREFIX` for test isolation.

---

## Workspace Status

The project is currently set up as a standalone Go module (`module s3proxy4gcs`).
For testing locally without breaking user paths, you can build locally with standard Go runtimes.
