//! Administrative and observability APIs (info, logging, telemetry).

use crate::document::DocumentType;
use crate::engine::Engine;
use crate::errors::DbError;
use std::path::PathBuf;

// Build-time generated list of compiled features
#[allow(dead_code)]
mod built {
    include!(concat!(env!("OUT_DIR"), "/compiled_features.rs"));
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CollectionInfo {
    pub name: String,
    pub docs: usize,
    pub ephemeral: usize,
    pub persistent: usize,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub indexes: Vec<crate::index::IndexDescriptor>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct InfoReport {
    pub collections: Vec<CollectionInfo>,
    pub total_ephemeral: usize,
    pub total_persistent: usize,
    pub compiled_features: Vec<String>,
    pub runtime_flags: Vec<super::feature::FeatureFlagInfo>,
    pub package_name: String,
    pub package_version: String,
}

pub fn info(engine: &Engine) -> InfoReport {
    let mut out = Vec::new();
    let mut total_e = 0usize;
    let mut total_p = 0usize;
    for name in engine.list_collection_names() {
        if let Some(col) = engine.get_collection(&name) {
            let docs = col.get_all_documents();
            let mut e = 0usize;
            let mut p = 0usize;
            for d in &docs {
                match d.metadata.document_type {
                    DocumentType::Ephemeral => e += 1,
                    DocumentType::Persistent => p += 1,
                }
            }
            let m = col.cache_metrics();
            let idx = col.indexes.read().descriptors();
            if name == "_tempDocuments" {
                total_e += e + p;
            } else {
                total_p += e + p;
            }
            out.push(CollectionInfo {
                name: name.clone(),
                docs: docs.len(),
                ephemeral: e,
                persistent: p,
                cache_hits: m.hits,
                cache_misses: m.misses,
                indexes: idx,
            });
        }
    }
    let compiled = built::COMPILED_FEATURES.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let runtime = super::feature::feature_list();
    InfoReport {
        collections: out,
        total_ephemeral: total_e,
        total_persistent: total_p,
        compiled_features: compiled,
        runtime_flags: runtime,
        package_name: env!("CARGO_PKG_NAME").to_string(),
        package_version: env!("CARGO_PKG_VERSION").to_string(),
    }
}

// --- Logging configuration API ---

/// Configure logging globally (application/audit/metrics) for the process.
/// - dir: base directory for logs; if None, current directory
/// - level: error|warn|info|debug|trace (case-insensitive)
/// - retention: number of rolled files to keep (default 7)
pub fn log_configure(dir: Option<&std::path::Path>, level: Option<&str>, retention: Option<usize>) {
    crate::logger::configure_logging(dir, level, retention);
}

/// Configure logging from environment variables, if present.
/// Variables: NEXUSLITE_LOG_DIR, NEXUSLITE_LOG_LEVEL, NEXUSLITE_LOG_RETENTION
pub fn log_configure_from_env() {
    crate::logger::configure_from_env();
}

/// Initialize logging from a configuration file (log4rs.yaml) when present.
pub fn log_init_from_file() -> Result<(), DbError> {
    crate::logger::init().map_err(|e| DbError::Io(e.to_string()))
}

/// Initialize logging from a specific configuration file path.
pub fn log_init_from_file_path(path: &std::path::Path) -> Result<(), DbError> {
    crate::logger::init_path(path).map_err(|e| DbError::Io(e.to_string()))
}

// --- Telemetry/Observability configuration API ---

/// Set the database name for telemetry context (used in logs).
pub fn telemetry_set_db_name(db_name: &str) {
    crate::telemetry::set_db_name(db_name);
}

/// Configure query log path and optional slow-query threshold and structured JSON toggle.
pub fn telemetry_set_query_log(
    path: PathBuf,
    slow_query_ms: Option<u64>,
    structured_json: Option<bool>,
) {
    crate::telemetry::set_query_log(path, slow_query_ms, structured_json);
}

/// Enable or disable audit logging.
pub fn telemetry_set_audit_enabled(enabled: bool) {
    crate::telemetry::set_audit_enabled(enabled);
}

/// Set global max result limit and per-collection overrides.
pub fn telemetry_set_max_results_global(limit: usize) {
    crate::telemetry::set_max_result_limit_global(limit);
}
pub fn telemetry_set_max_results_for(collection: &str, limit: usize) {
    crate::telemetry::set_max_result_limit_for(collection, limit);
}

/// Configure per-collection token bucket rate limit.
pub fn telemetry_configure_rate_limit(collection: &str, capacity: u64, refill_per_sec: u64) {
    crate::telemetry::configure_rate_limit(collection, capacity, refill_per_sec);
}
/// Remove a per-collection rate limit.
pub fn telemetry_remove_rate_limit(collection: &str) {
    crate::telemetry::remove_rate_limit(collection);
}

/// Set default per-collection rate limit used when not explicitly configured.
pub fn telemetry_set_default_rate_limit(capacity: u64, refill_per_sec: u64) {
    crate::telemetry::set_default_rate_limit(capacity, refill_per_sec);
}
