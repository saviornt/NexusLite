//! Collections/query helpers and import/export wrappers.

use crate::document::Document;
use crate::engine::Engine;
use crate::errors::DbError;
use crate::export::{ExportOptions, export_file};
use crate::import::{ImportOptions, import_file};
use crate::query::{self, Filter, FindOptions, UpdateDoc};
use std::path::Path;

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

/// Import data from a file path into the target collection.
pub fn import<P: AsRef<Path>>(
    engine: &Engine,
    file: P,
    opts: &ImportOptions,
) -> Result<crate::import::ImportReport, DbError> {
    import_file(engine, file, opts).map_err(|e| DbError::Io(e.to_string()))
}

/// Export a collection to a destination path.
pub fn export<P: AsRef<Path>>(
    engine: &Engine,
    collection: &str,
    file: P,
    opts: &ExportOptions,
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
    let bdoc: bson::Document = crate::utils::json::parse_json_to_bson_document(json)
        .map_err(|e| DbError::Io(e.to_string()))?;
    let mut doc = Document::new(
        bdoc,
        if ephemeral {
            crate::document::DocumentType::Ephemeral
        } else {
            crate::document::DocumentType::Persistent
        },
    );
    if ephemeral && let Some(s) = ttl_secs {
        doc.set_ttl(std::time::Duration::from_secs(s));
    }
    Ok(col.insert_document(doc))
}
