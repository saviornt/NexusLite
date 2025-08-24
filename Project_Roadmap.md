# NexusLite — Project Roadmap

This document tracks the AGILE roadmap and sprint checklists. It’s a living plan; we iterate as code and tests evolve. For contributor guidelines and day-to-day development workflow, see Project_Development.md.

---

## AGILE Project Roadmap

We follow an iterative AGILE approach where each sprint adds working, testable functionality. Future features build on stable, well-tested foundations.

### Linting & Coding Standards

- Run clippy locally and deny warnings to keep the codebase clean:

```powershell
cargo clippy -q --all-targets --all-features -- -D warnings -W clippy::pedantic -W clippy::nursery --fix
```

- Common lint to avoid: needless reference of operands in comparisons (`clippy::op-ref`). Prefer `bytes[0..4] != SNAPSHOT_MAGIC` over `&bytes[0..4] != SNAPSHOT_MAGIC`.
- CI also denies warnings; add targeted `#[allow(...)]` only when justified and scoped.
- Design with concurrency in mind using `parking_lot::RwLock`.
- Use `thiserror`, `log`, and `log4rs` for error handling and logging.

---

## Sprint 1 — Core In-Memory Engine

- [x] Developer Documentation (Project_Development.md)
- [x] Error handling/logging via `log` and `log4rs`
- [x] Document module
  - Create, find, update, delete BSON-like documents
  - UUID v4 IDs; metadata with timestamps; persistent vs ephemeral types
  - Ephemeral supports optional TTL and resides in hidden `_tempDocuments`
- [x] Collection module
  - Manages documents; maintains ID index and vector
  - Hidden `_tempDocuments` for ephemeral docs
- [x] Engine module
  - Manages collections; create/save/delete database files
- [x] Rust API in `lib.rs`
- [ ] Ensure `RwLock` use properly scoped (benchmark read-heavy scenarios)
- [x] Unit & integration testing framework (`tests/` + logger helper)
- [x] Generate Rust documentation (`cargo doc`)
- [x] Tests run clean; fix issues and update docs

## Sprint 2 — Cache Layer (Redis-inspired)

- [x] Hybrid TTL-first + LRU eviction; tunable sampling
- [x] Cache metrics (hit/miss, eviction counts)
- [x] Runtime tuning (capacity, batch size, max_samples)
- [x] Background sweeper; lazy expiration
- [x] Per-collection overrides
- [x] Load ephemeral docs into cache at startup
- [x] Comprehensive tests; docs updated

## Sprint 3 — Persistence

- [x] WASP storage engine (Write-Ahead Shadow-Paging) as default backend
- [x] Benchmark-only WAL maintained under benchmarks for comparison
- [x] Page/tree/manifest format with checksums and atomic flips
- [x] Tiny WAL integration and recovery path
- [x] Immutable segment store; compaction & GC
- [x] MVCC-friendly reads; snapshot checkpoints
- [x] Store DB in single `.db` with separate `.wasp`; periodic checkpoint

## Sprint 4 — Import & Export

- [x] Streaming import/export for NDJSON/CSV/BSON with auto-detect
- [x] Options: batch size, persistent toggle, TTL mapping, skip_errors with sidecar
- [x] Windows-safe atomic writes on export
- [x] Roundtrip tests; large-file smoke tests; docs updated

## Sprint 5 — Query Engine, CLI & APIs

- [x] Typed filters/operators; projection, sort, pagination
- [x] Updates `$set`/`$inc`/`$unset`; counts/deletes
- [x] CLI commands for query/admin; programmatic API
- [x] Tests and docs updated

## Sprint 6 — Optimization, Security, Features

- [x] Indexing (hash/B-tree), index manager, metadata persistence, rebuilds
- [x] CLI/UX for telemetry and feature flags; structured logs; rate limiting
- [x] ECC crypto helpers (P-256) for file sign/verify/encrypt/decrypt
- [x] Password-based encryption (PBE) helpers for `.db`/`.wasp`
- [x] Threat model and SECURITY.md
- [x] Fuzz/property tests; CI smoke

---

## Fuzzing, Property Tests, and Benchmarks

- [x] `cargo fuzz` targets (filters, evaluator, updates, CSV/NDJSON)
- [x] Property tests for evaluator invariants, sort stability, projection, filesystem utils
- [x] Benchmarks comparing WASP vs simple WAL (benchmark-only)

---

## File I/O and Snapshot Compatibility

- [x] Atomic writes via `tempfile` and safe Windows replace
- [x] Snapshot header: magic `NXL1`, version `u32` (current 1), payload `DbSnapshot`
- [x] Newer snapshot versions return `Unsupported` errors (no panic)

---

## Security and Observability

- [x] Forbid `unsafe`
- [x] `cargo audit` and `cargo deny` in CI; license/source policies
- [x] Structured query logs with redaction; audit logs (toggle)
- [x] Rate limits (token bucket) and result caps
