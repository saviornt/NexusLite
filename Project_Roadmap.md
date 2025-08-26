# NexusLite — Project Roadmap

This document tracks the AGILE roadmap and sprint checklists. It’s a living plan; we iterate as code and tests evolve. For contributor guidelines and day-to-day development workflow, see Project_Development.md.

---

## AGILE Project Roadmap

We follow an iterative AGILE approach where each sprint adds working, testable functionality. Future features build on stable, well-tested foundations.

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

- [x] WASP recovery engine (Write-Ahead Shadow-Paging)
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
- [x] Implement import features to import various data formats.
  - The importer should infer what data format is being imported.
  - Once inferred, it should import the data into the database properly formatted.
  - At a minimum, the importer should support CSV, JSON, BSON and Pandas DataFrame formats.
- [x] Implement export features to export to various data formats.
- [x] Perform tests and then troubleshoot and fix any issues.
- [x] Update Developer Documentation (Project_Development.md).

- [x] Define import/export API contracts and options
  - [x] `import_from_reader`/`import_file` and `export_to_writer`/`export_file`
  - [x] Options include: `format (auto|ndjson|csv|bson)`, `collection`, `batch_size`, `persistent`, `ttl_field`, `skip_errors`
  - [x] Per-format options: CSV `{ delimiter, has_headers, type_infer? }`, JSON `{ array_mode?, pretty? }`

- [x] Prioritize streaming formats (memory-safe, large files)
  - [x] NDJSON (JSON Lines) import/export (stream via serde_json deserializer)
  - [x] CSV import/export (headers, delimiter support; stream via csv::Reader/Writer)
  - [x] BSON import/export (length-prefixed docs; stream read/write)

- [x] Format auto-detection with explicit override
  - [x] Use file extension as hint, then sniff first KB for: BOM/UTF-16, JSON tokens, CSV delimiter patterns, BSON length prefix
  - [x] Allow forcing the format via options when detection is ambiguous

- [x] Performance and memory controls
  - [x] Batch inserts with backpressure (configurable `batch_size`)
  - [x] Streamed IO with BufRead/Write; avoid loading entire datasets into memory

- [x] TTL and IDs mapping
  - [x] Optional `ttl_field` maps to ephemeral documents; otherwise persistent
  - [x] Accept optional `_id`; generate UUID if missing

- [x] Errors and reporting
  - [x] `skip_errors` mode: continue on row errors
  - [x] Produce sidecar `.errors.jsonl` with failed rows and reasons
  - [x] Return `ImportReport`/`ExportReport` with counts and timing

- [x] CLI integration (developer ergonomics)
  - [x] Programmatic CLI commands wired for import/export with tests

- [x] Windows-friendly file operations
  - [x] Export to a temp file and atomically replace destination (MoveFileExW with replace; fallback std::fs::rename + short retry)

- [x] Testing
  - [x] Unit tests for CSV/NDJSON/BSON parsers and type mapping
  - [x] Round-trip tests (import → export → compare)
  - [x] Large-file smoke tests (bounded memory)
  - [x] Windows path/encoding and atomic rename behavior

- [x] Documentation
  - [x] Update Project_Development.md with Sprint 4 completion
  - [x] README examples and pandas notes (NDJSON: `lines=True`)

## Sprint 5 — Query Engine, CLI & APIs

- [x] Core query engine with typed filters (no string-eval; injection-safe)
- [x] Public Rust APIs: find, count, update_many, delete_many, update_one, delete_one, cursor
- [x] Update operators: `$set`, `$inc`, `$unset` (validated and type-safe)
- [x] Projection, sort, pagination (limit/skip) and stable multi-key sort
- [x] Typed filters/operators; projection, sort, pagination
- [x] Updates `$set`/`$inc`/`$unset`; counts/deletes
- [x] CLI commands for query/admin; programmatic API
- [x] CLI commands: find, count, update, delete (streaming NDJSON/CSV output)
  - Added single-document variants: update_one, delete_one
- [x] Baseline security: input validation, limits, lock-scoping
- [x] Tests: unit + integration
- [x] Tests and docs updated

- [x] Filter DSL and evaluation
  - [x] BSON/JSON filter structure with operators: `$eq`(implicit), `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$and`, `$or`, `$not`, `$exists`
  - [x] Dot-notation field paths for nested documents (e.g., `profile.name.first`)
  - [x] Type coercion for numerics (i32/i64/f64) with strict cross-type rules
  - [x] Clear distinction between missing and null; `$exists` semantics

- [x] Public APIs
  - [x] `find(&self, filter: &Filter, opts: &FindOptions) -> Cursor`
  - [x] `count(&self, filter: &Filter) -> usize`
  - [x] `update_many(&self, filter: &Filter, update: &UpdateDoc) -> UpdateReport`
  - [x] `delete_many(&self, filter: &Filter) -> DeleteReport`
  - [x] `Cursor`: iterator over IDs; resolves documents lazily; test-only `to_vec()`
  - [x] `FindOptions { projection, sort, limit, skip }` with `SortSpec { field, order }`

- [x] Update operators
  - [x] `$set`: assign/create nested field paths
  - [x] `$inc`: numeric add; error on non-numeric targets
  - [x] `$unset`: remove field if present
  - [x] `UpdateReport { matched, modified }` (modified only on value change)

- [x] Sort, projection, pagination
  - [x] Stable comparator with multi-key sort; deterministic total order
  - [x] Include-only projection by field paths
  - [x] Enforce reasonable `limit`/`skip` bounds

- [x] CLI (programmatic for now)
  - [x] `query find --collection C --filter JSON --project 'a,b' --sort '-age,+name' --limit N --skip M --output (ndjson|csv|bson)`
  - [x] `query count --collection C --filter JSON`
  - [x] `query update --collection C --filter JSON --update JSON`
  - [x] `query delete --collection C --filter JSON --confirm`
  - [x] Stream results as NDJSON by default; CSV optional (headers)

- [x] Security and safety
  - [x] Parse filter/update via serde into typed structs (no string interpolation)
  - [x] Enforce limits: max filter depth, max array length, max `$in` list size
  - [x] Optional `$regex` behind feature flag with length guard (<= 512 chars)
  - [x] Query timeout/cancellation hooks (best-effort deadline during scan)
  - [x] Avoid panics; return structured errors; property tests for evaluator
  - [x] Lock scoping: hold RwLocks minimally; snapshot IDs before iteration
  - [x] Memory: prefer iterators; avoid cloning full collections

- [x] Testing
  - [x] Unit: operators, nested paths, numeric coercion, exists/missing, projection
  - [x] Sort comparator correctness (multi-key, missing/null ordering)
  - [x] Update operators: set/inc/unset; matched vs modified
  - [x] Integration: import sample → queries → updates → exports
  - [x] CLI: parse/execute filters/updates; stream output fixtures

- [x] Documentation
  - [x] README: add “Query & Update” examples (Rust + CLI)
  - [x] Project_Development.md: finalize Sprint 5 spec and checklists

## Sprint 6 — Optimization, Security, Features

- [x] Indexing (hash/B-tree), index manager, metadata persistence, rebuilds
- [x] CLI/UX for telemetry and feature flags; structured logs; rate limiting
- [x] ECC crypto helpers (P-256) for file sign/verify/encrypt/decrypt
- [x] Password-based encryption (PBE) helpers for `.db`/`.wasp`
- [x] Threat model and SECURITY.md
- [x] Fuzz/property tests; CI smoke

- [x] Performance/indexing
  - [x] Initial indexing strategies (exact-match and range on popular fields)
  - [x] Implement an index manager abstraction to allow for future pluggable index types
  - [x] Basic index selection for single-field equality/range; fallback to full scan
  - [x] Track index statistics (size, hit/miss, build time) for observability
  - [x] Persist index metadata and rebuild if missing or inconsistent
  - [x] Implement indexing invalidation: Call out when indexes rebuild (insert/update/delete, collection renames) and persistence across restarts
  - [x] Index + WASP interaction: Define and implement an atomic "commit + index update" step; ensure index metadata/cardinality stays consistent across crashes
  - [x] Index build mode (acceptance): offline builds block writes to the target collection; reject writes during build with a clear error; tests cover build/rebuild safety (no data corruption)
  - [x] Minimal planner rule (acceptance): use a single-field index for simple equality/range predicates; otherwise full scan; tests assert planner chooses the index when available
  - [x] Index metadata versioning: bump on index format changes; auto-rebuild indexes on version mismatch at startup (document behavior)

- [x] API/CLI/UX
  - [x] Clap-based binary exposing Import/Export/Query commands and DB/collection admin (`src/bin/nexuslite.rs`)
  - [x] CLI config file loader with precedence (flags > env > config files > defaults)
    - [x] Config secrets hygiene: discourage storing secrets in config files; prefer environment variables; redact secret-like keys in logs and diagnostics
    - [x] Implement on-disk index metadata/versioning and rebuild UX
  - [x] `nexuslite info` command to print basic database stats (collections, cache metrics)
  - [x] `nexuslite doctor` command to check basic DB/WASP file access
    - [x] `nexuslite shell` for interactive query/collection management (REPL)
  - [x] Implement full API/FFI calls exposing Import/Export/Query commands, DB/collection admin, and an info to print engine/cache/index stats and other metrics that should be exposed (`api.rs`)
  - [x] Tests updated to cover CLI programmatic path; binary executes and compiles via cargo test

  Notes: As part of hardening, remaining uses of `.unwrap()`/`.expect()` in runtime paths were removed or isolated behind guaranteed-safe constructs. CLI now propagates errors instead of panicking.

- [x] Cryptography (optional features)
  - [x] ECC-256 encryption (key/Pair) and ECDSA signature verification
    - [x] ECC-256 based encryption for files (ECDH + AES-256-GCM) with header
  - [x] ECDSA signature verification for files
  - [x] Add API helper to hash secret fields in documents (argon2id)
  - [x] Add CLI option to redact or mask sensitive fields in exports; doctor masks secrets in config/env.
  - [x] PQC roadmap alignment (ml-kem, sphincs+) documented (see PQC section below)
  - [x] Add PQC code stub to the `crypto` module for future integration.
  - [x] Create tests for keygen/sign/verify, encrypt/decrypt, and hashing
  - [x] Update project documentation and README with CLI crypto commands and export redaction.

- [ ] Code security and supply-chain
  - [x] `cargo audit` + `cargo deny` in CI; fail on vulnerable/yanked deps
    - Configured GitHub Actions workflow at `.github/workflows/security.yml` and `deny.toml`.
  - [x] Clippy (pedantic + nursery) and rustfmt in CI; deny warnings
    - CI wired; repository remediation for warnings tracked separately.
  - [x] Forbid `unsafe` in crate (or gate behind feature if absolutely required)
    - Added `#![forbid(unsafe_code)]` at crate root.
  - [ ] Dependency pinning and minimal public surface review
  - [x] Implement a comprehensive security review process (e.g., threat modeling, attack surface analysis)
    - Added `SECURITY-THREAT-MODEL.md` initial draft.
  - [x] Add SECURITY.md with reporting and hygiene policy

- [x] Fuzzing and property tests
  - [x] `cargo fuzz` targets: filter parser, evaluator, update applier, CSV/NDJSON parsers
    - [x] filter parser
    - [x] evaluator
    - [x] update parser/applier
    - [x] CSV/NDJSON parsers
  - [x] Minimal seed corpora committed under `fuzz/corpus/*`
  - [x] Property tests
    - [x] Evaluator invariants (equality symmetry; integer order complement)
    - [x] CSV import inserts expected rows
    - [x] Multi-key sort stability
    - [x] Projection returns only selected fields; pagination bounds safety
  - [x] CI fuzz smoke workflow (`.github/workflows/fuzz-smoke.yml`)

- [x] Perform a complete review and update for both the `README.md` and `Project_Development.md` documentation.

- [x] Memory and concurrency safety
  - [x] Concurrency tests (basic loom model or stress tests) for lock ordering
    - Implemented a stress test (`tests/mod_concurrency.rs::concurrent_insert_read_update_stress`) that exercises parallel inserts/reads/updates and validates invariants; passed in the full suite.
  - [x] Cursor-based iteration in core paths to avoid large clones
    - Added a lazy, ID-based iteration path in `src/query.rs::find_docs` with `Collection::list_ids()` to avoid materializing full documents when projection/sort aren’t requested; validated by existing query tests.
  - [x] Optional sanitizer/miri runs in CI where feasible (nightly job)
  - [x] Ensure that the codebase is highly optimized and free of unnecessary allocations.
    - Streamed exports (NDJSON/CSV/BSON) iterate IDs and fetch on-demand, avoiding large Vec clones.
  - [x] Ensure that thread / CPU concurrency and asynchronous file I/O is being used properly to ensure optimal performance and reliability.
    - Added concurrent export test using spawn_blocking to isolate blocking I/O; file writes remain atomic with retries on Windows.

- [x] File I/O safety
  - [x] Use `tempfile::NamedTempFile` for atomic writes (avoid symlink races)
  - [x] Path normalization and validation; explicit permissions where applicable
    - New `fsutil::normalize_db_path` ensures `.db` extension and absolute path; used by `Database::new/open/close`.
    - New `fsutil::create_secure` creates files with restrictive permissions (0o600 on Unix; default ACLs on Windows).
  - [x] Retry/backoff strategy around Windows file locks, prefer short retries with jitter
  - [x] Expand file I/O hardening with explicit permissions on creation where applicable and targeted Windows retry/backoff around renames
    - Exports write to temp and atomically persist with Windows-friendly retries; WASP checkpoint writes directly on Windows to avoid rename sharing violations.
  - [x] Embed DB snapshot format version and magic; refuse newer versions and document the policy
    - `.db` snapshot now uses header: magic `NXL1` + `u32` version (current 1) + `DbSnapshot` payload; legacy raw `DbSnapshot` without a header is not supported in this initial build.
    - If a newer version is encountered, decoding returns `io::ErrorKind::Unsupported` (no panic). A new negative test asserts this behavior.
  - [x] Create tests as needed, run all tests, and fix any identified issues
    - Added `snapshot_newer_version_errors_gracefully` in `tests/mod_snapshot.rs` to validate error path for future-version snapshots.
  - [x] Update documentation as needed
    - README updated with snapshot format/versioning and compatibility policy.

- [x] Observability and abuse resistance
  - [x] Structured query logs with redaction for sensitive fields
    - Query path now emits structured slow-query lines (JSON) with fields: {ts, db, collection, filter_hash, duration_ms, limit, skip, slow} via `telemetry::log_query`.
  - [x] Rate limiting and quotas (per collection) via basic token-bucket; exposed in API/CLI
    - Added global and per-collection max result limit enforcement (default 10,000; overrides supported).
    - New CLI subcommands: telemetry-set-slow, telemetry-set-audit, telemetry-set-query-log, telemetry-set-max-global, telemetry-set-max-for, telemetry-rate-limit, telemetry-rate-remove.
    - Programmatic API: telemetry_set_db_name/query_log/audit_enabled, telemetry_set_max_results_global/_for, telemetry_configure_rate_limit/remove_rate_limit.
  - [x] Audit logging for all write operations (user, timestamp, changes) (Optional Feature)
    - Insert/Update/Delete emit audit records via `telemetry::log_audit`; off by default, toggle with `telemetry::set_audit_enabled(true)`.
  - [x] Query logging with user/session metadata (hook; user optional)
  - [x] Extra hardening (e.g., input validation, output encoding, regex timeouts)
  - [x] Query timeouts and max result size enforcement
  - [x] Add Prometheus/OpenMetrics export (optional feature) for cache/engine/query stats
    - Minimal text exposition via `telemetry::metrics_text()` for `nexus_*` counters.
  - [x] Add slow query log (configurable threshold)
    - Slow threshold via `NEXUS_SLOW_QUERY_MS` or API setter; logs include stable fields.
  - [x] Metrics naming stability documented (see README Modules/Logger + new Telemetry notes)
  - [x] Implemented a configurable logging system using log4rs plus per-DB scoped logs; added telemetry module for structured logs/metrics.
  - [x] Tests pass across the suite; added hooks are covered indirectly by existing query/write tests; no behavior regressions.
  - [x] Updated documentation as needed.

- [x] Fuzzing, Property Tests
  - [x] `cargo fuzz` targets (filters, evaluator, updates, CSV/NDJSON)
  - [x] Property tests for evaluator invariants, sort stability, projection, filesystem utils

- [x] File I/O and Snapshot Compatibility
  - [x] Atomic writes via `tempfile` and safe Windows replace
  - [x] Snapshot header: magic `NXL1`, version `u32` (current 1), payload `DbSnapshot`
  - [x] Newer snapshot versions return `Unsupported` errors (no panic)

- [x] Security and Observability
  - [x] Forbid `unsafe`
  - [x] `cargo audit` and `cargo deny` in CI; license/source policies
  - [x] Structured query logs with redaction; audit logs (toggle)
  - [x] Rate limits (token bucket) and result caps

- [x] Feature flags
  - [x] Publish supported runtime flags: `crypto-ecc`, `crypto-pqc` (stub), `open-metrics`, `regex` (mirrors Cargo feature), `cli-bin`
  - [x] Document supported build combinations (MVP build matrix) and deny unknown features in CI
  - [x] Expose compiled features and runtime flags in `info` output; document in README

- [x] Code Security, Supply Chain, and Fuzzing and property tests (again)
  - [x] Perform code security checks and identify issues and problems.
  - [x] Perform supply chain checks and identify issues and problems.
  - [x] Perform fuzzing and property checks and identify issues and problems.
  - [x] Perform pedantic/nursery cleanup and identify issues and problems.
  - [x] Implement any required changes and update the README and Project Development documentation.
  
  Notes:
  - Clippy warnings are clean with `-D warnings` across all targets/features; targeted fixes applied in `build.rs`, `cli.rs`, `import.rs`, `telemetry.rs`, and tests.
  - Property tests and the full test suite pass locally; long-running CSV inference property test remains green.
  - Added edge-case unit tests for query projection/sort/limit bounds and import CSV/NDJSON/BSON error handling.
  - Supply-chain checks are wired in CI (`cargo-audit`/`cargo-deny`); local runs are optional if the tools are installed.

- Run per-module tests and checks with `cargo fmt`, `cargo clippy`

- [x] Unit tests
  - [x] Colocate unit tests with source modules using `#[cfg(test)]` (e.g., `src/<module>/...` with `mod tests`)
  - [x] Add a small test-only support module for temp dirs and file helpers
  - [x] Cover core modules: utils, query (parse/eval/exec), api, crypto, import/export
  - [x] Ensure unit tests run via `cargo unit-tests` (alias for `cargo test --lib`) and are included in CI

---

## Sprint 7: Benchmarks (Optional Feature) & MVP

- [ ] Create the necessary hooks within the database engine to support benchmarking. Tests should include, but not be limited to:
  - [ ] Query execution time tracking
  - [ ] Index usage statistics
  - [ ] Cache hit/miss ratios
  - [ ] WASP performance metrics
  - [ ] Document size and growth rate tracking
  - [ ] Query result size tracking
  - [ ] Cache eviction statistics
  - [ ] Document read/write latency tracking
  - [ ] Cache performance metrics
  - [ ] WASP recovery time tracking
  - [ ] Any other relevant metrics

- [ ] Create benchmark tests and log their results
  - [x] Create benchmarks comparing WASP vs simple WAL
  - [ ] Create benchmarks for query performance, index usage, and cache efficiency

- [ ] Perform sanity-check of feature flags, the `cli`, `api`, `bin/nexuslite` and `feature_flags` modules.
  - [ ] Document any changes to features and feature-flags

- [ ] Finalize repo for use as a Rust crate, including:
  - [ ] Add `nexuslite` as a dependency in `Cargo.toml`.
  - [ ] Ensure all public APIs are documented and tested.
  - [ ] Ensure all features are properly gated with feature flags.
  - [ ] Ensure the code and comments are well-structured and follows Rust conventions.
  - [ ] Ensure the code is well-tested and has good test coverage.
  - [ ] Ensure the code is well-documented and has good documentation.

  - [ ] Additional Testing/CI
    - [ ] Mutation testing with `cargo-mutants` (Deferred until later in development to prioritize iteration speed.). When re-enabling, run locally with `cargo mutants -v` and wire a CI workflow.

- [ ] Complete mutation and fuzz testing and document benchmarks.
- [ ] Fix any identified issues and problems and update README and Project Development documentation.

- [ ] Create a "packages" folder for the repo that include packages for, but not limited to:
  - [ ] Python
  - [ ] JavaScript / TypeScript
  - [ ] Go
  - [ ] C and C++
  - [ ] Rust
  - [ ] Binary executables for Windows, Linux, and macOS

- [ ] Docs
  - [ ] Ensure that the codebase is properly documented as per Rust coding standards and best practices.
  - [ ] Add a "Deployment" section with guidelines for deploying the database.
  - [ ] Add a "Security Model" section to the documentation, outlining threat model, encryption and audit logging plans
  - [ ] Add a "Performance Tuning" section with cache, eviction, and index tuning tips.
  - [ ] Add a "Testing and QA" section with guidelines for writing tests and using CI tools.
  - [ ] Add a section for Transaction support exploration in the `Project_Development.md` documentation.
  - [ ] Add a section for compatibility policy and on-disk format versioning
  - [ ] Create API/CLI documentation using Rustdoc/mdBook and also make API/CLI documentation available as optional feature flag. Create an auto-generated CLI help/manpage. Metric names should also be inside of the documentation
  - [ ] Create a user guide (e.g., usage examples, tutorials).
  - [ ] Update Developer Documentation (Project_Development.md).
  - [ ] Update `README.md` documentation.

- [ ] Verify .gitignore entries

---
