use crate::engine::Engine;
use crate::errors::DbError;
use crate::export::{export_file, ExportOptions};
use crate::import::{import_file, ImportOptions};
use crate::query::{self, FindOptions, Filter, UpdateDoc};
use std::path::PathBuf;
use crate::document::{Document, DocumentType};

// Programmatic API: thin helpers intended for embedding (e.g., via FFI/Python)

// --- Database management (FFI-friendly) ---
pub fn db_open(db_path: &str) -> Result<crate::Database, DbError> {
    crate::Database::open(db_path).map_err(|e| DbError::Io(e.to_string()))
}

pub fn db_new(db_path: Option<&str>) -> Result<crate::Database, DbError> {
    crate::Database::new(db_path).map_err(|e| DbError::Io(e.to_string()))
}

pub fn db_close(db_path: Option<&str>) -> Result<(), DbError> {
    crate::Database::close(db_path)
}

pub fn db_create_collection(db: &crate::Database, name: &str) {
    db.create_collection(name);
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

pub fn find(engine: &Engine, collection: &str, filter: &Filter, opts: &FindOptions) -> Result<Vec<bson::Document>, DbError> {
    let col = engine.get_collection(collection).ok_or_else(|| DbError::NoSuchCollection(collection.to_string()))?;
    let cur = query::find_docs(&col, filter, opts);
    Ok(cur.to_vec().into_iter().map(|d| d.data.0).collect())
}

pub fn count(engine: &Engine, collection: &str, filter: &Filter) -> Result<usize, DbError> {
    let col = engine.get_collection(collection).ok_or_else(|| DbError::NoSuchCollection(collection.to_string()))?;
    Ok(query::count_docs(&col, filter))
}

pub fn update_many(engine: &Engine, collection: &str, filter: &Filter, update: &UpdateDoc) -> Result<crate::query::UpdateReport, DbError> {
    let col = engine.get_collection(collection).ok_or_else(|| DbError::NoSuchCollection(collection.to_string()))?;
    Ok(query::update_many(&col, filter, update))
}

pub fn update_one(engine: &Engine, collection: &str, filter: &Filter, update: &UpdateDoc) -> Result<crate::query::UpdateReport, DbError> {
    let col = engine.get_collection(collection).ok_or_else(|| DbError::NoSuchCollection(collection.to_string()))?;
    Ok(query::update_one(&col, filter, update))
}

pub fn delete_many(engine: &Engine, collection: &str, filter: &Filter) -> Result<crate::query::DeleteReport, DbError> {
    let col = engine.get_collection(collection).ok_or_else(|| DbError::NoSuchCollection(collection.to_string()))?;
    Ok(query::delete_many(&col, filter))
}

pub fn delete_one(engine: &Engine, collection: &str, filter: &Filter) -> Result<crate::query::DeleteReport, DbError> {
    let col = engine.get_collection(collection).ok_or_else(|| DbError::NoSuchCollection(collection.to_string()))?;
    Ok(query::delete_one(&col, filter))
}

pub fn import(engine: &Engine, file: PathBuf, opts: &mut ImportOptions) -> Result<crate::import::ImportReport, DbError> {
    import_file(engine, file, opts).map_err(|e| DbError::Io(e.to_string()))
}

pub fn export(engine: &Engine, collection: &str, file: PathBuf, opts: &mut ExportOptions) -> Result<crate::export::ExportReport, DbError> {
    export_file(engine, collection, file, opts).map_err(|e| DbError::Io(e.to_string()))
}

// Convenience: JSON parsing frontends
pub fn parse_filter_json(json: &str) -> Result<Filter, DbError> { query::parse_filter_json(json) }
pub fn parse_update_json(json: &str) -> Result<UpdateDoc, DbError> { query::parse_update_json(json) }

// Document creation helper (Persistent or Ephemeral)
pub fn create_document(engine: &Engine, collection: Option<&str>, json: &str, ephemeral: bool, ttl_secs: Option<u64>) -> Result<crate::types::DocumentId, DbError> {
    let target = if ephemeral { "_tempDocuments".to_string() } else { collection.ok_or(DbError::NoSuchCollection("<none>".into()))?.to_string() };
    let col = engine.get_collection(&target).unwrap_or_else(|| engine.create_collection(target.clone()));
    let val: serde_json::Value = serde_json::from_str(json).map_err(|e| DbError::Io(e.to_string()))?;
    let bdoc: bson::Document = bson::to_document(&val).map_err(|e| DbError::Io(e.to_string()))?;
    let mut doc = Document::new(bdoc, if ephemeral { DocumentType::Ephemeral } else { DocumentType::Persistent });
    if ephemeral {
        if let Some(s) = ttl_secs { doc.set_ttl(std::time::Duration::from_secs(s)); }
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
}

pub fn info(engine: &Engine) -> InfoReport {
    let mut out = Vec::new();
    let mut total_e = 0usize; let mut total_p = 0usize;
    for name in engine.list_collection_names() {
        if let Some(col) = engine.get_collection(&name) {
            let docs = col.get_all_documents();
            let mut e = 0usize; let mut p = 0usize;
            for d in &docs { match d.metadata.document_type { DocumentType::Ephemeral => e += 1, DocumentType::Persistent => p += 1 } }
            let m = col.cache_metrics();
            let idx = col.indexes.read().descriptors();
            if name == "_tempDocuments" { total_e += e + p; } else { total_p += e + p; }
            out.push(CollectionInfo { name: name.clone(), docs: docs.len(), ephemeral: e, persistent: p, cache_hits: m.hits, cache_misses: m.misses, indexes: idx });
        }
    }
    InfoReport { collections: out, total_ephemeral: total_e, total_persistent: total_p }
}
