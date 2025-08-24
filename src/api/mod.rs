// Submodules for clear separation of concerns
pub mod admin;
pub mod collections;
pub mod db;

use crate::document::{Document, DocumentType};
use crate::engine::Engine;
use crate::errors::DbError;
use crate::export::{export_file, ExportOptions};
use crate::import::{import_file, ImportOptions};
use crate::query::{self, Filter, FindOptions, UpdateDoc};
use std::path::Path;
use std::path::PathBuf;
// Build-time generated list of compiled features
#[allow(dead_code)]
mod built {
    include!(concat!(env!("OUT_DIR"), "/compiled_features.rs"));
}

// Programmatic API: thin helpers intended for embedding (e.g., via FFI/Python)

// --- Database management (FFI-friendly) ---
pub fn db_open(db_path: &str) -> Result<crate::Database, DbError> {
    // Detect PBE-encrypted .db/.wasp and prompt for creds if needed (env first)
    let pb = std::path::PathBuf::from(db_path);
    let wasp = pb.with_extension("wasp");
    let pbe_db = crate::crypto::pbe_is_encrypted(&pb);
    let pbe_wasp = wasp.exists() && crate::crypto::pbe_is_encrypted(&wasp);
    if pbe_db || pbe_wasp {
        // Programmatic API is non-interactive: require env vars when PBE-encrypted
        let username = std::env::var("NEXUSLITE_USERNAME").unwrap_or_default();
        let password = std::env::var("NEXUSLITE_PASSWORD").unwrap_or_default();
        if username.is_empty() || password.is_empty() {
            return Err(DbError::Io(
                "PBE-encrypted DB: set NEXUSLITE_USERNAME and NEXUSLITE_PASSWORD".into(),
            ));
        }
        crate::api::decrypt_db_with_password(pb.as_path(), &username, &password)?;
    }
    crate::Database::open(db_path).map_err(|e| DbError::Io(e.to_string()))
}

// (interactive helper removed; API is non-interactive)

pub fn db_new(db_path: Option<&str>) -> Result<crate::Database, DbError> {
    crate::Database::new(db_path).map_err(|e| DbError::Io(e.to_string()))
}

pub fn db_close(db_path: Option<&str>) -> Result<(), DbError> {
    crate::Database::close(db_path)
}

pub fn db_create_collection(db: &crate::Database, name: &str) {
    let _ = db.create_collection(name);
}

pub fn db_list_collections(db: &crate::Database) -> Vec<String> {
    db.list_collection_names()
}

pub fn db_delete_collection(db: &crate::Database, name: &str) -> bool {
    db.delete_collection(name)
}

pub fn db_rename_collection(db: &crate::Database, old: &str, new: &str) -> Result<(), DbError> {
    db.rename_collection(old, new)
}

pub fn find(
    engine: &Engine,
    collection: &str,
    filter: &Filter,
    opts: &FindOptions,
) -> Result<Vec<bson::Document>, DbError> {
    let col = engine
        .get_collection(collection)
        .ok_or_else(|| DbError::NoSuchCollection(collection.to_string()))?;
    let cur = query::find_docs_rate_limited(&col, filter, opts)?;
    Ok(cur.to_vec().into_iter().map(|d| d.data.0).collect())
}

pub fn count(engine: &Engine, collection: &str, filter: &Filter) -> Result<usize, DbError> {
    let col = engine
        .get_collection(collection)
        .ok_or_else(|| DbError::NoSuchCollection(collection.to_string()))?;
    if !crate::telemetry::try_consume_token(&col.name_str(), 1) {
        crate::telemetry::log_rate_limited(collection, "count");
        let ra = crate::telemetry::retry_after_ms(&col.name_str(), 1);
        return Err(DbError::RateLimitedWithRetry { retry_after_ms: ra });
    }
    query::count_docs_rate_limited(&col, filter)
}

pub fn update_many(
    engine: &Engine,
    collection: &str,
    filter: &Filter,
    update: &UpdateDoc,
) -> Result<crate::query::UpdateReport, DbError> {
    let col = engine
        .get_collection(collection)
        .ok_or_else(|| DbError::NoSuchCollection(collection.to_string()))?;
    Ok(query::update_many(&col, filter, update))
}

pub fn update_one(
    engine: &Engine,
    collection: &str,
    filter: &Filter,
    update: &UpdateDoc,
) -> Result<crate::query::UpdateReport, DbError> {
    let col = engine
        .get_collection(collection)
        .ok_or_else(|| DbError::NoSuchCollection(collection.to_string()))?;
    Ok(query::update_one(&col, filter, update))
}

pub fn delete_many(
    engine: &Engine,
    collection: &str,
    filter: &Filter,
) -> Result<crate::query::DeleteReport, DbError> {
    let col = engine
        .get_collection(collection)
        .ok_or_else(|| DbError::NoSuchCollection(collection.to_string()))?;
    Ok(query::delete_many(&col, filter))
}

pub fn delete_one(
    engine: &Engine,
    collection: &str,
    filter: &Filter,
) -> Result<crate::query::DeleteReport, DbError> {
    let col = engine
        .get_collection(collection)
        .ok_or_else(|| DbError::NoSuchCollection(collection.to_string()))?;
    Ok(query::delete_one(&col, filter))
}

pub fn import(
    engine: &Engine,
    file: PathBuf,
    opts: &mut ImportOptions,
) -> Result<crate::import::ImportReport, DbError> {
    import_file(engine, file, opts).map_err(|e| DbError::Io(e.to_string()))
}

pub fn export(
    engine: &Engine,
    collection: &str,
    file: PathBuf,
    opts: &mut ExportOptions,
) -> Result<crate::export::ExportReport, DbError> {
    export_file(engine, collection, file, opts).map_err(|e| DbError::Io(e.to_string()))
}

// Convenience: JSON parsing frontends
pub fn parse_filter_json(json: &str) -> Result<Filter, DbError> {
    query::parse_filter_json(json)
}
pub fn parse_update_json(json: &str) -> Result<UpdateDoc, DbError> {
    query::parse_update_json(json)
}

// Document creation helper (Persistent or Ephemeral)
pub fn create_document(
    engine: &Engine,
    collection: Option<&str>,
    json: &str,
    ephemeral: bool,
    ttl_secs: Option<u64>,
) -> Result<crate::types::DocumentId, DbError> {
    let target = if ephemeral {
        "_tempDocuments".to_string()
    } else {
        collection.ok_or(DbError::NoSuchCollection("<none>".into()))?.to_string()
    };
    let col =
        engine.get_collection(&target).unwrap_or_else(|| engine.create_collection(target.clone()));
    let val: serde_json::Value =
        serde_json::from_str(json).map_err(|e| DbError::Io(e.to_string()))?;
    let bdoc: bson::Document = bson::to_document(&val).map_err(|e| DbError::Io(e.to_string()))?;
    let mut doc = Document::new(
        bdoc,
        if ephemeral { DocumentType::Ephemeral } else { DocumentType::Persistent },
    );
    if ephemeral && let Some(s) = ttl_secs {
        doc.set_ttl(std::time::Duration::from_secs(s));
    }
    Ok(col.insert_document(doc))
}

// Info/metrics report
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
    pub runtime_flags: Vec<FeatureFlagInfo>,
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
    // Compiled features: from build script output
    let compiled = built::COMPILED_FEATURES.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let runtime = feature_list();
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

// --- Crypto helpers (optional usage) ---
pub fn crypto_generate_p256() -> (String, String) {
    crate::crypto::generate_p256_keypair_pem()
}

pub fn crypto_sign_file(priv_pem: &str, input: &Path) -> Result<Vec<u8>, DbError> {
    crate::crypto::sign_file_p256(priv_pem, input).map_err(|e| DbError::Io(e.to_string()))
}

pub fn crypto_verify_file(pub_pem: &str, input: &Path, sig_der: &[u8]) -> Result<bool, DbError> {
    crate::crypto::verify_file_p256(pub_pem, input, sig_der).map_err(|e| DbError::Io(e.to_string()))
}

pub fn crypto_encrypt_file(pub_pem: &str, input: &Path, output: &Path) -> Result<(), DbError> {
    crate::crypto::encrypt_file_p256(pub_pem, input, output).map_err(|e| DbError::Io(e.to_string()))
}

pub fn crypto_decrypt_file(priv_pem: &str, input: &Path, output: &Path) -> Result<(), DbError> {
    crate::crypto::decrypt_file_p256(priv_pem, input, output)
        .map_err(|e| DbError::Io(e.to_string()))
}

/// Write an encrypted checkpoint of the database .db snapshot.
pub fn checkpoint_encrypted(
    db: &crate::Database,
    output: &Path,
    recipient_pub_pem: &str,
) -> Result<(), DbError> {
    // 1) Snapshot to temp
    let tmp = output.with_extension("tmp.db");
    db.checkpoint(&tmp)?;
    // 2) Encrypt to requested output
    crate::crypto::encrypt_file_p256(recipient_pub_pem, &tmp, output)
        .map_err(|e| DbError::Io(e.to_string()))?;
    // 3) Cleanup temp
    let _ = std::fs::remove_file(tmp);
    Ok(())
}

/// Restore database snapshot from an encrypted file.
pub fn restore_encrypted(
    db_path: &Path,
    encrypted: &Path,
    recipient_priv_pem: &str,
) -> Result<(), DbError> {
    // 1) Decrypt to a temp snapshot
    let tmp = encrypted.with_extension("dec.db");
    crate::crypto::decrypt_file_p256(recipient_priv_pem, encrypted, &tmp)
        .map_err(|e| DbError::Io(e.to_string()))?;
    // 2) Move into place as the .db snapshot
    std::fs::copy(&tmp, db_path).map_err(|e| DbError::Io(e.to_string()))?;
    let _ = std::fs::remove_file(tmp);
    Ok(())
}

// --- Password-based DB encryption helpers ---

/// Encrypt both the .db (snapshot) and .wasp files using username+password.
/// If files are already PBE-encrypted, this will overwrite them with new params.
pub fn encrypt_db_with_password(
    db_path: &Path,
    username: &str,
    password: &str,
) -> Result<(), DbError> {
    let wasp_path = db_path.with_extension("wasp");
    // .db: ensure checkpoint snapshot exists, then wrap with PBE
    let tmp_plain = db_path.with_extension("tmp.plain.db");
    if db_path.exists() {
        std::fs::copy(db_path, &tmp_plain).map_err(|e| DbError::Io(e.to_string()))?;
    } else {
        // If no db yet, create empty
        std::fs::File::create(&tmp_plain).map_err(|e| DbError::Io(e.to_string()))?;
    }
    let tmp_enc = db_path.with_extension("tmp.enc.db");
    crate::crypto::pbe_encrypt_file(username, password, &tmp_plain, &tmp_enc, None)
        .map_err(|e| DbError::Io(e.to_string()))?;
    std::fs::rename(&tmp_enc, db_path).map_err(|e| DbError::Io(e.to_string()))?;
    let _ = std::fs::remove_file(&tmp_plain);

    // .wasp: wrap existing file if present, else create empty encrypted
    if wasp_path.exists() {
        let tmp_plain_w = wasp_path.with_extension("tmp.plain.wasp");
        std::fs::copy(&wasp_path, &tmp_plain_w).map_err(|e| DbError::Io(e.to_string()))?;
        let tmp_enc_w = wasp_path.with_extension("tmp.enc.wasp");
        crate::crypto::pbe_encrypt_file(username, password, &tmp_plain_w, &tmp_enc_w, None)
            .map_err(|e| DbError::Io(e.to_string()))?;
        std::fs::rename(&tmp_enc_w, &wasp_path).map_err(|e| DbError::Io(e.to_string()))?;
        let _ = std::fs::remove_file(&tmp_plain_w);
    } else {
        let tmp_plain_w = wasp_path.with_extension("tmp.plain.wasp");
        std::fs::File::create(&tmp_plain_w).map_err(|e| DbError::Io(e.to_string()))?;
        let tmp_enc_w = wasp_path.with_extension("tmp.enc.wasp");
        crate::crypto::pbe_encrypt_file(username, password, &tmp_plain_w, &tmp_enc_w, None)
            .map_err(|e| DbError::Io(e.to_string()))?;
        std::fs::rename(&tmp_enc_w, &wasp_path).map_err(|e| DbError::Io(e.to_string()))?;
        let _ = std::fs::remove_file(&tmp_plain_w);
    }
    Ok(())
}

/// Decrypt both the .db and .wasp files using username+password. This removes PBE encryption.
pub fn decrypt_db_with_password(
    db_path: &Path,
    username: &str,
    password: &str,
) -> Result<(), DbError> {
    let wasp_path = db_path.with_extension("wasp");
    if crate::crypto::pbe_is_encrypted(db_path) {
        let tmp_out = db_path.with_extension("tmp.dec.db");
        crate::crypto::pbe_decrypt_file(username, password, db_path, &tmp_out)
            .map_err(|e| DbError::Io(e.to_string()))?;
        std::fs::rename(&tmp_out, db_path).map_err(|e| DbError::Io(e.to_string()))?;
    }
    if wasp_path.exists() && crate::crypto::pbe_is_encrypted(&wasp_path) {
        let tmp_out_w = wasp_path.with_extension("tmp.dec.wasp");
        crate::crypto::pbe_decrypt_file(username, password, &wasp_path, &tmp_out_w)
            .map_err(|e| DbError::Io(e.to_string()))?;
        std::fs::rename(&tmp_out_w, &wasp_path).map_err(|e| DbError::Io(e.to_string()))?;
    }
    Ok(())
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

// --- Feature Flags API ---

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FeatureFlagInfo {
    pub name: String,
    pub enabled: bool,
    pub description: String,
}

pub fn feature_list() -> Vec<FeatureFlagInfo> {
    crate::feature_flags::list()
        .into_iter()
        .map(|f| FeatureFlagInfo { name: f.name, enabled: f.enabled, description: f.description })
        .collect()
}

pub fn feature_set(name: &str, enabled: bool) -> Result<(), DbError> {
    if crate::feature_flags::set(name, enabled) {
        Ok(())
    } else {
        Err(DbError::QueryError(format!("unknown feature flag: {name}")))
    }
}

pub fn feature_get(name: &str) -> Option<FeatureFlagInfo> {
    crate::feature_flags::get(name).map(|f| FeatureFlagInfo {
        name: f.name,
        enabled: f.enabled,
        description: f.description,
    })
}

/// Initialize runtime feature flags from environment variables.
/// Returns the list of flags after initialization for convenience.
pub fn init_from_env() -> Vec<FeatureFlagInfo> {
    crate::feature_flags::init_from_env();
    feature_list()
}

/// Convenience: enable/disable PQC runtime flag (stub behavior for now).
pub fn crypto_set_pqc_enabled(enabled: bool) {
    let _ = crate::feature_flags::set("crypto-pqc", enabled);
}

/// Check if PQC runtime flag is enabled.
pub fn crypto_is_pqc_enabled() -> bool {
    crate::feature_flags::is_enabled("crypto-pqc")
}
