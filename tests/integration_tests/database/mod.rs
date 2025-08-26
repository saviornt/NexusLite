// Database-specific tests live here and include engine/index/paths/snapshot suites.
#[path = "mod_paths.rs"]
mod db_paths_tests;
#[path = "mod_engine.rs"]
mod engine_tests;
#[path = "mod_index.rs"]
mod index_tests;
#[path = "mod_snapshot_open.rs"]
mod snapshot_open_tests;
#[path = "mod_snapshot.rs"]
mod snapshot_tests;
