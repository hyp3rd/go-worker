# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- gRPC auth hook via `WithGRPCAuth`.
- Task tracing hooks via `TaskTracer`.

### Changed

- Rate limiter burst now defaults to `min(maxWorkers, maxTasks)` for deterministic throttling.
- `ExecuteTask` respects the task rate limiter (may wait or return context errors).
- Breaking: `Stop()` removed; use `StopGraceful(ctx)` or `StopNow()`.
- Breaking: `SubscribeResults` replaces `GetResults()`/`StreamResults()`.
- Breaking: `RegisterTasks` now returns an error.

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
