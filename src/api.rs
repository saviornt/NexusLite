use crate::engine::Engine;
use crate::errors::DbError;
use crate::export::{export_file, ExportOptions};
use crate::import::{import_file, ImportOptions};
use crate::query::{self, FindOptions, Filter, UpdateDoc};
use std::path::PathBuf;

// Programmatic API: thin helpers intended for embedding (e.g., via FFI/Python)

// --- Database management (FFI-friendly) ---
pub fn db_open(db_path: &str) -> Result<crate::Database, DbError> {
    crate::Database::open(db_path).map_err(|e| DbError::Io(e.to_string()))
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
