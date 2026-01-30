# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- gRPC auth hook via `WithGRPCAuth`.
- Task tracing hooks via `TaskTracer`.
- OpenTelemetry metrics via `SetMeterProvider`.
- OpenTelemetry tracing example and OTLP metrics example.
- Result fan-out examples for OTel metrics and tracing.
- Compatibility shim `GetResults()` for legacy callers.
- Retention load and OTel metrics wiring tests.
- Benchmarks for registration throughput and retention pruning.
- Durable task APIs with a Redis backend (rueidis).
- Durable lease renewal via `WithDurableLeaseRenewalInterval`.
- `NewTaskManagerWithOptions` for functional configuration.
- gRPC durable task registration via `RegisterDurableTasks`.
- Durable Redis examples and tooling: `__examples/durable_redis`, `__examples/grpc_durable`, `__examples/durable_dlq_replay`, `__examples/durable_queue_inspect`.

### Changed

- Rate limiter burst now defaults to `min(maxWorkers, maxTasks)` for deterministic throttling.
- `ExecuteTask` respects the task rate limiter (may wait or return context errors).
- Breaking: `DurableBackend` now requires `Extend` for lease renewal.
- Breaking: `Stop()` removed; use `StopGraceful(ctx)` or `StopNow()`.
- Breaking: `SubscribeResults` replaces `GetResults()`/`StreamResults()` (shim restored for legacy).
- Breaking: `RegisterTasks` now returns an error.
- Breaking: `RegisterTask(s)` are disabled when a durable backend is enabled (use `RegisterDurableTask(s)`).
- gRPC: new `DurableTask` and `RegisterDurableTasks` for persisted tasks (existing RPCs unchanged).

## [v0.1.1] - 2025-08-18

### Added (v0.1.1)

- docs: expand README with architecture and community guidance by @hyp3rd in <https://github.com/hyp3rd/go-worker/pull/9>
- feat(api): introduce proto tooling and update Task/Worker interfaces for context support by @hyp3rd in <https://github.com/hyp3rd/go-worker/pull/11>
- Add gRPC task service with streaming results by @hyp3rd in <https://github.com/hyp3rd/go-worker/pull/12>
- feat: enhance gRPC service and add example by @hyp3rd in <https://github.com/hyp3rd/go-worker/pull/13>
- feat: expand gRPC task definition by @hyp3rd in <https://github.com/hyp3rd/go-worker/pull/14>

**Full Changelog**: <https://github.com/hyp3rd/go-worker/compare/v0.1.0...v0.1.1>

## [v0.1.0] - 2025-08-16

### Added (v0.1.0)

- Initial release of go-worker with prioritized concurrent task execution.
