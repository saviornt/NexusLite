# NexusLite - Project Development Roadmap

NexusLite is an embedded **NoSQL database engine**, inspired by the best features of **MongoDB** (document collections) and **Redis** (in-memory performance, TTL, and LRU caching). The goal is to provide a **lightweight, embeddable, efficient, and flexible** database engine similar to SQLite but for NoSQL workloads.

---

## AGILE Project Roadmap

We’ll follow an **iterative AGILE approach** where each sprint adds working, testable functionality.  
Future features will always build on stable, well-tested foundations.

## Linting & Coding Standards

- Run clippy locally and deny warnings to keep the codebase clean:

```powershell
cargo clippy -q --all-targets --all-features -- -D warnings
```

// Mutation testing: deferred
// To keep local iteration fast, mutation testing is currently parked. See the Benchmarks & MVP section for future enablement notes.

- Common lint to avoid: needless reference of operands in comparisons (`clippy::op-ref`).
  - Prefer `bytes[0..4] != SNAPSHOT_MAGIC` over `&bytes[0..4] != SNAPSHOT_MAGIC`.
- CI also denies warnings; only add targeted `#[allow(...)]` when justified and scoped.
- Design with concurrency in mind using `RwLock` from the start.
- Design with async in mind using `tokio` for both network and file-based async I/O.
- Design modules with error handling and logging using the `thiserror`, `log`, and `log4rs` crates.
- Testing & the use of `.unwrap()`, as per its documentation: Because this function may panic, its use is generally discouraged. Panics are meant for unrecoverable errors, and may abort the entire program. Instead, prefer to use the ? (try) operator, or pattern matching to handle the Err case explicitly, or call [unwrap_or], [unwrap_or_else], or [unwrap_or_default].

### Sprint 1 - Core In-Memory Engine

- [x] Developer Documentation (Project_Development.md).
- [x] Implement error handling and logging using the crates `log` and `log4rs`.
- [x] Implement `Document` module (`document.rs`)
  - Create, find, update, delete BSON-like documents.
  - When creating a new document, the document will be assigned a document UUID v4.
  - Documents will also store metadata that describes the document details.
  - There should be two types of documents: persistent and temporary.
  - Temporary document metadata will support an optional Time-To-Live (TTL) and are stored in a hidden collection and loaded into memory on startup.
  - Allow metadata (timestamps, versioning, or user tags) be optional extension points from the start. Future upgrades benefit from this flexibility.
- [x] Implement `Collection` module (`collection.rs`)
  - Manage sets of documents inside named collections.
  - Collections will maintain an index of document UUIDs.
  - Collections will also store vector index of each document.
  - A "hidden" collection also needs to be created called `_tempDocuments` that will contain ephemeral documents.
- [x] Implement `Engine` module (`engine.rs`)
  - Manage multiple collections.
  - Create, save, delete database files.
- [x] Implement Rust API calls to database engine (`lib.rs`)
  - Add builder patterns (e.g. `Document::builder().field(...).build()`), to make creation more fluent.
- [ ] Ensure `RwLock` use is properly scoped.
  - Benchmark read-heavy scenarios to spot deadlocks early.
- [x] Add unit & integration testing framework (`tests/` + `common/test_logger.rs`).
- [x] Generate Rust documentation (RustDoc) using `cargo doc`.
- [x] Perform tests and then troubleshoot and fix any issues.
  - Due to how logging works, we do not use a `mod_logging.rs` file since we cannot have 2 loggers be initialized at the same time.
  - Add tests around invalid UUIDs, empty collections, or creating duplicate collection names to prove resilience.
- [x] Update Developer Documentation (Project_Development.md).

### Sprint 2 - Cache Layer (Redis-inspired)

- [x] Implement a **Hybrid TTL & LRU eviction policy**.
  - [x] TTL has highest priority. Always evict entries whose TTL has expired before considering LRU-based eviction.
  - [x] Fallback to LRU sampling when no TTL-expired entries are found; sample size configurable via `max_samples`.
  - [x] Implemented approximation of LRU using tail sampling; tunable `max_samples` available.
  - [x] Strategy aligns with keeping freshness over recency.
  - [x] Separated sections in `cache.rs` combining TTL + LRU.

- [x] Include comprehensive metrics as part of the cache layer:
  - [x] Hit/miss counters
  - [x] Eviction counts by type (TTL vs LRU)
  - [x] Memory/latency stats

- [x] Give the system flexibility to tune eviction behavior:
  - [x] Runtime adjustable `max_samples`, `batch_size`, `capacity`, and eviction mode
  - [x] Per-collection overrides via `Engine::create_collection_with_config`

- [x] Implement a guard against thundering evictions:
  - [x] Eviction batching
  - [x] Eviction lock to prevent concurrent eviction cycles

- [x] Handle TTL expiration proactively
  - [x] Background sweeper with configurable interval
  - [x] Lazy expiration on access increments miss count

- [x] Allow configuration of TTL and LRU parameters at runtime.
  - [x] Eviction modes: `ttl-first`, `lru-only`, `ttl-only`, `hybrid`
  - [x] Per-collection override supported

- [x] Implement the **cache using the hybrid eviction policy** for documents.
  - [x] Lazy eviction + periodic low-priority background purging
  - [x] Purge trigger exposed for deterministic tests

- [x] Implement logic to load all ephemeral documents from the internal `_tempDocuments` collection into the cache on database startup.
- [x] Perform tests and then troubleshoot and fix any issues.

- [x] Perform unit tests for each scenario:
  - [x] TTL expiration evicts before LRU
  - [x] LRU sampling when no TTLs are expired
  - [x] Batching and lock under concurrent pressure
  - [x] Lazy-expiration counts as miss

- [x] Update Developer Documentation (Project_Development.md).

### Sprint 3 - Persistence

- [x] Implement a hybrid crash-consistent storage engine (`Write-Ahead Shadow-Paging` or `WASP`; `wasp.rs`) and make it the default backend.
- [x] Pluggable storage engine: swap between WAL and WASP for benchmarking.
- [x] Add a benchmark test comparing WAL vs WASP, saving results to `benchmarks/`.

- [x] Phase 0: Design and requirements for WASP:
  - [x] Define requirements/goals (ACID level, workload patterns, durability guarantees, concurrency model).
  - [x] Decide page size (e.g., 8–16 KB) and segment size targets (e.g., 64–256 MB).
    - Page size should be 8-16 KB that aligns to the device. Delta pages for tiny updates.
    - Segment size targets should be 64-256 MB and leveled compaction fan-out 8-10.
  - [x] Choose on-disk format endianness, alignment, and checksums.
  - [x] Implement block allocator / free space map abstraction.
  - [x] Build manifest structure (root pointer + active segments + WAL metadata).

- [x] Phase 1: Minimal CoW Engine
  - [x] Implement page format (headers, checksums, version ids).
  - [x] Implement copy-on-write B-tree or LSM-like node tree for data storage.
  - [x] Add manifest write and atomic pointer flip (double-buffered).
  - [x] Implement crash-safe read path (scan manifest → open latest root).
  - [x] Unit test: basic insert/read/delete, durability after crash simulation.

- [x] Phase 2: Tiny WAL Layer
  - [x] Design WAL record format: {txn id, page ids, checksums, new root id, epoch}.
  - [x] Add WAL append + fdatasync logic.
  - [x] Implement group commit batching.
  - [x] Integrate WAL into commit path (before manifest flip).
  - [x] Recovery logic: read manifest, replay WAL to finish incomplete CoW updates.
  - [x] Stress test: power-fail injection during updates. (basic test via append/recover)

- [x] Phase 3: Immutable Segment Store
  - [x] Define segment file format (sorted key ranges, fence keys, bloom filters).
  - [x] Add logic to seal cold data into segments (CoW → segment flush).
  - [x] Implement read path that merges CoW + segments.
  - [x] Add bloom filter acceleration for segment lookups.
  - [x] Unit test: query workload across mixed hot/cold data.

- [x] Phase 4: Compaction & Space Reclaim
  - [x] Implement background compaction engine (leveled or tiered).
  - [x] Add token-bucket throttling to cap IO usage. (future)
  - [x] Integrate with free space map to recycle old pages/segments.
  - [x] Add epoch-based GC for safe cleanup of obsolete data.
  - [x] Stress test: long-running workload without space leaks.

- [x] Phase 5: Concurrency & MVCC
  - [x] Add epoch-based snapshot tracking for readers.
  - [x] Implement MVCC visibility rules (readers see stable snapshot, writers advance epochs).
  - [x] Optimize for multiple concurrent readers, single writer (common embedded pattern).
  - [x] Benchmark concurrent read-write workloads. (future)

- [x] Phase 6: Durability & Integrity Hardening
  - [x] Add end-to-end checksums (pages, WAL, manifest, segments).
  - [x] Add torn-write protection (length-prefixed records, double-write slots).
  - [x] Optionally support copy-verify (read-after-write) for non-power-safe devices. (future)
  - [x] Build consistency checker tool (fsck-style).
  - [x] Fuzz test: corrupt WAL/pages/manifest, ensure graceful recovery.

- [x] Phase 7: Performance & Productionization
  - [x] Implement block cache for hot pages/segments.
  - [x] Add prefetch/pipelining for sequential scans.
  - [x] Optimize manifest updates (batch multiple commits per flip).
  - [x] Add statistics & metrics (WAL usage, compaction debt, cache hit ratio).
  - [x] Benchmark against baseline DBs (SQLite WAL, LMDB, RocksDB).

- [x] Implement collection snapshots. (stub)
- [x] Store the database in a **single file** (like SQLite) with a separate file for the WASP engine (`{db_name}.wasp` file).
- [x] Implement a periodic, configurable **checkpointing process** to merge the WASP into the main database file.
- [x] Perform tests and then troubleshoot and fix any issues.
- [x] Update Developer Documentation (Project_Development.md).

### Sprint 4 - Import & Export Features

- [x] Implement import features to import various data formats.
  - The importer should infer what data format is being imported.
  - Once inferred, it should import the data into the database properly formatted.
  - At a minimum, the importer should support CSV, JSON, BSON and Pandas DataFrame formats.
- [x] Implement export features to export to various data formats.
- [x] Perform tests and then troubleshoot and fix any issues.
- [x] Update Developer Documentation (Project_Development.md).

#### Detailed checklist for Sprint 4

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

### Sprint 5 - Query Engine, CLI & APIs

- [x] Core query engine with typed filters (no string-eval; injection-safe)
- [x] Public Rust APIs: find, count, update_many, delete_many, update_one, delete_one, cursor
- [x] Update operators: `$set`, `$inc`, `$unset` (validated and type-safe)
- [x] Projection, sort, pagination (limit/skip) and stable multi-key sort
- [x] CLI commands: find, count, update, delete (streaming NDJSON/CSV output)
  - Added single-document variants: update_one, delete_one
- [x] Baseline security: input validation, limits, lock-scoping
- [x] Tests: unit + integration
- [x] Documentation updates (Project_Development.md, README.md)

#### Detailed checklist for Sprint 5

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

### Sprint 6 - Optimization, Security Hardening, Additional Features

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
    - Regex remains feature-gated with length guards; timeouts best-effort via existing query deadline.
  - [x] Query timeouts and max result size enforcement
  - [x] Add Prometheus/OpenMetrics export (optional feature) for cache/engine/query stats
    - Minimal text exposition via `telemetry::metrics_text()` for `nexus_*` counters.
  - [x] Add slow query log (configurable threshold)
    - Slow threshold via `NEXUS_SLOW_QUERY_MS` or API setter; logs include stable fields.
  - [x] Metrics naming stability documented (see README Modules/Logger + new Telemetry notes)
  - [x] Implemented a configurable logging system using log4rs plus per-DB scoped logs; added telemetry module for structured logs/metrics.
  - [x] Tests pass across the suite; added hooks are covered indirectly by existing query/write tests; no behavior regressions.
  - [x] Updated documentation as needed.

- [ ] Feature flags
  - [ ] Publish supported feature flags: `crypto-ecc`, `crypto-pqc` (future), `prometheus` or `open-metrics`, `regex`, `cli-bin`
  - [ ] Document supported build combinations (MVP build matrix) and deny unknown features in CI
  - [ ] Expose compiled feature flags in `db info` and document them in mdBook/Rustdoc

- [ ] Code Security, Supply Chain, and Fuzzing and property tests (again)
  - [ ] Perform code security checks
  - [ ] Perform supply chain checks
  - [ ] Perform fuzzing and property checks
  - [ ] Perform pedantic/nursery cleanup
  - [ ] Implement any required changes as needed

- [ ] Docs
  - [ ] Ensure that the codebase is properly documented as per Rust coding standards and best practices.
  - [ ] Add a "Deployment" section with guidelines for deploying the database.
  - [ ] Add a "Security Model" section to the documentation, outlining threat model, encryption and audit logging plans
  - [ ] Add a "Performance Tuning" section with cache, eviction, and index tuning tips.
  - [ ] Add a "Testing and QA" section with guidelines for writing tests and using CI tools.
  - [ ] Add a section for Transaction support exploration in the `Project_Development.md` documentation.
  - [ ] Add a section for compatibility policy and on-disk format versioning
  - [ ] Create API/CLI documentation (e.g., Rustdoc/mdBook and available as optional feature flag) and auto-generated CLI help/manpage. Metric names should also be inside of the documentation
  - [ ] Create a user guide (e.g., usage examples, tutorials).
  - [ ] Update Developer Documentation (Project_Development.md).
  - [ ] Update `README.md` documentation.

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
  - [ ] Binary executable

- [ ] Verify .gitignore entries

---

## Future Enhancements and Optional Features

- Add stricter redaction for secrets detection using the key matcher list and pattern-based value masking across outputs.
- Add support for PQC encryption/decryption and signature verification of the database.
  - Use `pqcrypto-mlkem` for key encapsulation (`ml-kem-512`, `ml-kem-768`, `ml-kem-1024`).
  - Use `pqcrypto-sphincsplus` for signature verification (`128`, `192`, `256`-bit hash functions).
  - Provide Cargo feature flags to toggle encryption support.
  - Encrypt snapshots, WAL, and per-collection files.
  - Sign persisted data to ensure integrity.
- Add support for full, multi-document ACID transactions.
- Add Vector Map Indexing for searching through collections and documents.
  - Use the `hnsw` crate for efficient approximate nearest neighbor search.
  - Implement indexing on document fields for faster queries.
  - Support for multi-dimensional vectors and various distance metrics.
- Future Enhancements to the WASP recovery engine.
  - Add secondary indexes.
  - Support multi-writer concurrency (fine-grained latching).
  - Add encryption at rest (per-page or per-segment keys).
  - Implement online backup/checkpointing.
  - Consider pluggable compression for segments.
- Dynamic Library layer using C-ABI externs.

---

## Recent changes

- Query projection semantics now apply to returned payloads: when `FindOptions.projection` is set, the cursor yields documents containing only the selected fields. Sorting is still applied prior to projection.
- Property tests updated to use per-test temporary directories (`tempfile::tempdir()`), avoiding Windows file-lock contention during concurrent runs.

## Database Architecture

```mermaid
flowchart TD
    A[User Data <br> <i>JSON Formatted</i>] --> C[Cache <br> <i>cache.rs</i>]
    B[Imported Data <br> <i>import.rs</i>] --> C
    C --> D[WASP <br> <i>wasp.rs</i>]
    D --> E[Document <br> <i>document.rs</i>] --> H[Exported Data <br> <i>export.rs</i>]
    E --> F{Collection <br> <i>collection.rs</i>} --> H
    F --> G[Database <br> <i>engine.rs</i>] --> H
```

---

## Project Structure

The following is the current project structure, subject to change:

```text
nexus_lite
├── benchmarks\
│   ├── results\
│   ├── benchmark_wasp.rs
├── src\
│   ├── api.rs
│   ├── cache.rs
│   ├── cli.rs
│   ├── collection.rs
│   ├── crypto.rs
│   ├── document.rs
│   ├── engine.rs
│   ├── errors.rs
│   ├── export.rs
│   ├── import.rs
│   ├── index.rs
│   ├── lib.rs
│   ├── logger.rs
│   ├── query.rs
│   ├── types.rs
│   ├── wal.rs
│   └── wasp.rs
├── tests\
│   ├── common\
│   │   └── test_logger.rs
│   ├── integration.rs
│   ├── mod_api.rs
│   ├── mod_cache.rs
│   ├── mod_cli.rs
│   ├── mod_collection.rs
│   ├── mod_crypto.rs
│   ├── mod_document.rs
│   ├── mod_engine.rs
│   ├── mod_errors.rs
│   ├── mod_export.rs
│   ├── mod_import.rs
│   ├── mod_index.rs
│   ├── mod_lib.rs
│   ├── mod_query.rs
│   ├── mod_types.rs
│   ├── mod_wal.rs
│   └── mod_wasp.rs
├── .gitignore
├── Cargo.lock
├── Cargo.toml
├── README.md
└── Project_Development.md
```

---

## Modules (alignment)

Below is a quick reference for the modules and their current responsibilities.

- api.rs: Embedding-friendly helpers for DB open/new/close, CRUD, import/export, info report, and crypto helpers (ECC, PBE, encrypted checkpoint/restore, DB encrypt/decrypt).
- cache.rs: In-memory hybrid TTL-first + LRU cache with sweeper and metrics.
- cli.rs: Programmatic CLI dispatcher used by the binary; houses commands for import/export/query, admin, crypto, PBE DB toggles, and a signature verify helper.
- collection.rs: Collection abstraction managing documents, indexes, and cache wiring.
- crypto.rs: ECC (P-256) keygen/sign/verify; ECDH+HKDF→AES-256-GCM file crypto; Argon2id secret hashing; PBE (Argon2id→AES-256-GCM). PQC stubs included.
- document.rs: BSON-backed Document with metadata (type, timestamps, TTL) and helpers.
- engine.rs: Orchestrates collections and the storage backend (default WASP).
- errors.rs: thiserror-based `DbError` with IO/domain variants.
- export.rs: Streaming export (CSV/NDJSON/BSON) with redaction; Windows-safe atomic writes.
- import.rs: Streaming import (CSV/NDJSON/BSON) with auto-detect, sidecar errors, and TTL mapping.
- index.rs: Index descriptors, metadata persistence, versioning, and rebuild-on-mismatch.
- lib.rs: User-facing Database API and global engine/registry helpers.
- logger.rs: Scoped logger initialization next to DB with log4rs.
- query.rs: Typed filter/update engine with projection/sort/pagination; optional regex.
- types.rs: Core types (DocumentId, ops enums, metadata structures).
- wal.rs: Append-only WAL for benchmarking and historical engine.
- wasp.rs: Default storage engine (Write-Ahead Shadow-Paging) with CoW tree, WAL integration, segments, and compaction.

---

## On-disk snapshot format & compatibility

We introduced a lightweight header in the `.db` snapshot format:

- Magic: `NXL1` (4 bytes)
- Version: `u32` (currently 1)
- Payload: bincode `DbSnapshot`

Readers accept both the wrapped and legacy (payload-only) encodings. If the on-disk version is greater than the current, decoding returns `io::ErrorKind::Unsupported`. This is intentionally non-fatal for `Database::open/new` (best-effort index rebuild scanning), but the lower-level decode helper surfaces the error for tools/tests.
Readers now require the header; legacy payload-only snapshots are not supported. If the on-disk version is greater than the current, decoding returns `io::ErrorKind::Unsupported` (no panic).

---

## PQC roadmap and alignment

- Goals: Add hybrid PQC support while maintaining ECC paths. Keep crypto optional via feature flags and minimize public surface changes.
- KEM: Integrate ML-KEM (Kyber) via `pqcrypto-mlkem` for hybrid key exchange alongside P-256 ECDH; derive AEAD keys via HKDF.
- Signatures: Integrate SPHINCS+ via `pqcrypto-sphincsplus` for artifact/database signatures next to ECDSA.
- Phasing: start with encrypted checkpoint hybrid, then optional at-rest hybrid for `.db`/`.wasp`, then PQC signatures for `.sig` files.
- Tests: add vectors, round-trip, and tamper tests under `crypto-pqc` feature; CI matrix includes ECC-only and hybrid.
- Policy: signature enforcement selectable (warn vs hard-fail) in CLI and config; defaults conservative.

---

## Modules

### Document Module: document.rs

- Purpose: BSON-backed document with metadata, IDs, and TTL for ephemeral records.
- Features:
  - UUID v4 `DocumentId` assigned on creation
  - `DocumentType` (Persistent or Ephemeral)
  - Metadata: created_at, updated_at, optional TTL
  - `set_ttl`, `get_ttl`, `is_expired` helpers
  - `update` updates data and bumps `updated_at`

### Collection Module: collection.rs

- Purpose: Manage documents with a TTL-first + LRU cache and durable storage append.
- Features:
  - `new` and `new_with_config` to construct with cache capacity or config
  - `insert_document` writes to cache and appends Operation::Insert to storage
  - `find_document` reads from cache by ID
  - `update_document` upserts in cache and appends Operation::Update
  - `delete_document` evicts from cache and appends Operation::Delete
  - `get_all_documents` returns a snapshot Vec&lt;Document&gt; (clones; not streaming)
  - `cache_metrics` exposes cache metrics snapshot
  - Thread-safe via parking_lot::RwLock on storage

### Cache Module: cache.rs

- Purpose: In-memory cache with TTL-first plus LRU eviction to keep hot data fast.
- Features:
  - TTL expiration takes priority; lazy expiration on access
  - LRU sampling with configurable max_samples when no TTLs are expired
  - Eviction batching and guard to prevent thundering evictions
  - Background sweeper with configurable interval
  - Per-collection cache configuration and runtime tuning
  - Metrics: hits/misses, eviction counts, memory/latency stats

### WAL Module: wal.rs

- Purpose: Append-only write-ahead log to ensure durability and enable recovery.
- Features:
  - Append operations before commit for crash consistency
  - Read/replay log records on startup to rebuild state
  - Lightweight record format with basic integrity checks
  - Used as an alternative pluggable backend for benchmarking

### WASP Module: wasp.rs

- Purpose: Default persistence engine using Write-Ahead Shadow-Paging (WASP).
- Features:
  - Copy-on-write page tree with checksums
  - Double-buffered manifest with atomic pointer flip
  - Tiny WAL integration for commit ordering
  - Immutable segment store with bloom filters
  - Background compaction and space reclaim (GC)
  - Snapshot/MVCC-friendly read path

### Types Module: types.rs

- Purpose: Shared core types for IDs, operations, and metadata.
- Features:
  - Strongly-typed DocumentId (UUID v4)
  - Operation enums for insert/update/delete
  - Reusable structs/enums for cache and storage coordination

### Errors Module: errors.rs

- Purpose: Centralized error definitions with rich context.
- Features:
  - thiserror-based DbError for ergonomic error handling
  - Variants for IO, serialization, and domain errors (e.g., NoSuchCollection)
  - Consistent messages surfaced across modules and CLI

### Engine Module: engine.rs

- Purpose: Orchestrates collections and persistence backends.
- Features:
  - Create/get/delete collections; list collection names; rename collections
  - Pluggable storage: WAL or WASP (default via Engine::with_wasp)
  - Hidden `_tempDocuments` collection for ephemeral docs
  - On startup, loads ephemeral docs into cache when applicable
  - Thread-safe via parking_lot::RwLock

### Logger Module: logger.rs

- Purpose: Initialize structured logging for the system.
- Features:
  - log/log4rs setup via `logger::init()`
  - Configurable levels and appenders through `log4rs.yaml`

### Import Module: import.rs

- Purpose: Streaming data ingestion for CSV, NDJSON (JSON Lines), and BSON.
- Features:
  - Auto-detect format with explicit override
  - Per-format options: CSV (delimiter, headers, type inference), JSON (array_mode)
  - skip_errors with sidecar `.errors.jsonl` capturing failures
  - TTL mapping via `ttl_field` for ephemeral documents; persistent toggle
  - Progress logging and basic batching controls

### Export Module: export.rs

- Purpose: Streaming export of collections to CSV, NDJSON, or BSON.
- Features:
  - CSV with optional headers and custom delimiter
  - NDJSON line-by-line output for large files
  - BSON length-prefixed streaming writer
  - Writes to temp file then atomically replaces destination (Windows-safe)
  - Returns ExportReport with written counts

### API Module: api.rs

- Purpose: Provides a Rust API abstraction for embedding into apps.
- Features:
  - Convenience helpers around core engine operations
  - Query helpers: find/count, update/delete (many + one)
  - Import/Export helpers
  - DB/Collection management (FFI-friendly): open DB, create/list/delete/rename collections
  - Stable surface for embedding while internals evolve

### CLI Module: cli.rs

- Purpose: Provides CLI support for developers and database administration.
- Features:
  - Import/Export commands
  - Collection admin: create/delete/list/rename
  - Query commands: find/count/update/delete (+ update_one/delete_one)
  - Programmatic entrypoint `cli::run(engine, cmd)` returning reports

### Database Module: lib.rs

- Purpose: User-facing database wrapper around Engine with ergonomic helpers.
- Features:
  - `Database::new(name_or_path: Option<&str>)` creates `.db` (defaulting to `.db` extension) and `.wasp` if missing
  - `Database::open(name_or_path: &str)` opens existing `.db`, creating `.wasp` if missing; errors `Database Not Found` otherwise
  - `Database::close(name_or_path: Option<&str>)` unregisters/"closes" an open DB handle
  - Collection management: create/get/delete, list names
  - Document helpers: insert/update/delete
  - `nexus_lite::init()` to initialize logging

---

## Example Usage

### Quick Start Usage

```rust
use bson::doc;
use nexus_lite::document::{Document, DocumentType};
use nexus_lite::Database;

fn main() -> Result<(), Box<dyn std::error::Error>> {
  // Initialize system (logger, etc.)
  nexus_lite::init()?;

  // Create or open database (WASP-backed by default)
  // Use default name (nexuslite.db / nexuslite.wasp)
  let db = Database::new(None)?;

  // Create a collection
  db.create_collection("users");

  // Insert a document
  let user_doc = Document::new(doc!({"username": "alice", "age": 30}), DocumentType::Persistent);
  let doc_id = db.insert_document("users", user_doc)?;

  // Query document
  let users = db.get_collection("users").unwrap();
  let found = users.find_document(&doc_id).unwrap();
  println!("Found: {:?}", found);

  // Update document
  let updated = Document::new(doc!({"username": "alice", "age": 31}), DocumentType::Persistent);
  db.update_document("users", &doc_id, updated)?;

  // Delete document
  db.delete_document("users", &doc_id)?;
  Ok(())
}
```

---

### Import & Export Usage Examples

Programmatic usage:

```rust
use nexus_lite::engine::Engine;
use nexus_lite::import::{import_file, ImportOptions, ImportFormat};
use nexus_lite::export::{export_file, ExportOptions, ExportFormat};

fn main() -> Result<(), Box<dyn std::error::Error>> {
  let engine = Engine::with_wasp(std::path::PathBuf::from("nexus.wasp"))?;

  // Import NDJSON (auto-detected by extension)
  let mut iopts = ImportOptions::default();
  iopts.collection = "events".into();
  iopts.format = ImportFormat::Auto; // Csv/Ndjson/Bson also supported
  let _irep = import_file(&engine, "data/events.jsonl", &iopts)?;

  // Export collection as CSV
  let mut eopts = ExportOptions::default();
  eopts.format = ExportFormat::Csv;
  eopts.csv.write_headers = true;
  let _erep = export_file(&engine, "events", "export/events.csv", &eopts)?;
  Ok(())
}
```

Notes

- To continue past bad rows and log them, set `iopts.skip_errors = true` and `iopts.error_sidecar = Some("events.errors.jsonl".into())`.
- Pandas reads exported NDJSON via `pd.read_json('export/events.jsonl', lines=True)`.
