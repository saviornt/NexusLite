use crate::engine::Engine;
use crate::export::{export_file, ExportFormat, ExportOptions};
use crate::import::{import_file, ImportFormat, ImportOptions};
use crate::query::{self, FindOptions, Order, SortSpec};
use std::path::PathBuf;

pub enum Command {
    // Database & Collections management
    DbCreate { db_path: Option<PathBuf> },
    DbOpen { db_path: PathBuf },
    DbClose { db_path: PathBuf },
    ColCreate { name: String },
    ColDelete { name: String },
    ColList,
    ColRename { old: String, new: String },
    Import { collection: String, file: PathBuf, format: Option<String> },
    Export { collection: String, file: PathBuf, format: Option<String> },
    // Query subcommands (programmatic)
    QueryFind { collection: String, filter_json: String, project: Option<String>, sort: Option<String>, limit: Option<usize>, skip: Option<usize> },
    QueryCount { collection: String, filter_json: String },
    QueryUpdate { collection: String, filter_json: String, update_json: String },
    QueryDelete { collection: String, filter_json: String },
    QueryUpdateOne { collection: String, filter_json: String, update_json: String },
    QueryDeleteOne { collection: String, filter_json: String },
    // Document creation
    CreateDocument { collection: Option<String>, json: String, ephemeral: bool, ttl_secs: Option<u64> },
    // Ephemeral admin
    ListEphemeral,
    PurgeEphemeral { all: bool },
}

fn parse_format_input(s: &Option<String>) -> Option<String> { s.as_ref().map(|x| x.to_lowercase()) }

fn parse_import_format(s: &Option<String>) -> ImportFormat {
    match parse_format_input(s).as_deref() {
        Some("csv") => ImportFormat::Csv,
        Some("bson") => ImportFormat::Bson,
        Some("ndjson") | Some("json") | Some("jsonl") => ImportFormat::Ndjson,
        _ => ImportFormat::Auto,
    }
}

fn parse_export_format(s: &Option<String>) -> ExportFormat {
    match parse_format_input(s).as_deref() {
        Some("csv") => ExportFormat::Csv,
        Some("bson") => ExportFormat::Bson,
        _ => ExportFormat::Ndjson,
    }
}

pub fn run(engine: &Engine, cmd: Command) -> Result<(), Box<dyn std::error::Error>> {
    match cmd {
        Command::DbCreate { db_path } => {
            let path_str = db_path.as_ref().and_then(|p| p.to_str());
            let db = crate::Database::new(path_str)?;
            println!("created={}.db name={}", db.name(), db.name());
            Ok(())
        }
        Command::DbOpen { db_path } => {
            let p = db_path.to_str().ok_or("invalid path")?;
            let db = crate::Database::open(p)?;
            println!("opened name={}", db.name());
            Ok(())
        }
        Command::DbClose { db_path } => {
            let p = db_path.to_str().ok_or("invalid path")?;
            crate::Database::close(Some(p))?;
            println!("closed path={}", p);
            Ok(())
        }
        Command::ColCreate { name } => {
            engine.create_collection(name);
            Ok(())
        }
        Command::ColDelete { name } => {
            let _ = engine.delete_collection(&name);
            Ok(())
        }
        Command::ColList => {
            let names = engine.list_collection_names();
            for n in names { println!("{}", n); }
            Ok(())
        }
        Command::ColRename { old, new } => {
            engine.rename_collection(&old, &new)?;
            Ok(())
        }
        Command::Import { collection, file, format } => {
            let mut opts = ImportOptions::default();
            opts.collection = collection;
            opts.format = parse_import_format(&format);
            let _report = import_file(engine, file, &opts)?;
            Ok(())
        }
        Command::Export { collection, file, format } => {
            let mut opts = ExportOptions::default();
            opts.format = parse_export_format(&format);
            let _report = export_file(engine, &collection, file, &opts)?;
            Ok(())
        }
        Command::QueryFind { collection, filter_json, project, sort, limit, skip } => {
            let col = engine.get_collection(&collection).ok_or_else(|| crate::errors::DbError::NoSuchCollection(collection.clone()))?;
            let filter = query::parse_filter_json(&filter_json)?;
            let mut opts = FindOptions::default();
            if let Some(p) = project { opts.projection = Some(p.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()); }
            if let Some(s) = sort {
                let mut specs = Vec::new();
                for part in s.split(',') {
                    let part = part.trim(); if part.is_empty() { continue; }
                    let (order, field) = if let Some(rest) = part.strip_prefix('-') { (Order::Desc, rest) } else if let Some(rest) = part.strip_prefix('+') { (Order::Asc, rest) } else { (Order::Asc, part) };
                    specs.push(SortSpec { field: field.to_string(), order });
                }
                if !specs.is_empty() { opts.sort = Some(specs); }
            }
            opts.limit = limit; opts.skip = skip;
            let cursor = query::find_docs(&col, &filter, &opts);
            // Stream as NDJSON to stdout
            for doc in cursor.to_vec() {
                let line = serde_json::to_string(&doc.data.0)?;
                println!("{}", line);
            }
            Ok(())
        }
        Command::QueryCount { collection, filter_json } => {
            let col = engine.get_collection(&collection).ok_or_else(|| crate::errors::DbError::NoSuchCollection(collection.clone()))?;
            let filter = query::parse_filter_json(&filter_json)?;
            let n = query::count_docs(&col, &filter);
            println!("{}", n);
            Ok(())
        }
        Command::QueryUpdate { collection, filter_json, update_json } => {
            let col = engine.get_collection(&collection).ok_or_else(|| crate::errors::DbError::NoSuchCollection(collection.clone()))?;
            let filter = query::parse_filter_json(&filter_json)?;
            let update = query::parse_update_json(&update_json)?;
            let r = query::update_many(&col, &filter, &update);
            println!("{{\"matched\":{},\"modified\":{}}}", r.matched, r.modified);
            Ok(())
        }
        Command::QueryDelete { collection, filter_json } => {
            let col = engine.get_collection(&collection).ok_or_else(|| crate::errors::DbError::NoSuchCollection(collection.clone()))?;
            let filter = query::parse_filter_json(&filter_json)?;
            let r = query::delete_many(&col, &filter);
            println!("{{\"deleted\":{}}}", r.deleted);
            Ok(())
        }
        Command::QueryUpdateOne { collection, filter_json, update_json } => {
            let col = engine.get_collection(&collection).ok_or_else(|| crate::errors::DbError::NoSuchCollection(collection.clone()))?;
            let filter = query::parse_filter_json(&filter_json)?;
            let update = query::parse_update_json(&update_json)?;
            let r = query::update_one(&col, &filter, &update);
            println!("{{\"matched\":{},\"modified\":{}}}", r.matched, r.modified);
            Ok(())
        }
        Command::QueryDeleteOne { collection, filter_json } => {
            let col = engine.get_collection(&collection).ok_or_else(|| crate::errors::DbError::NoSuchCollection(collection.clone()))?;
            let filter = query::parse_filter_json(&filter_json)?;
            let r = query::delete_one(&col, &filter);
            println!("{{\"deleted\":{}}}", r.deleted);
            Ok(())
        }
        Command::CreateDocument { collection, json, ephemeral, ttl_secs } => {
            // Determine target collection
            let target = if ephemeral { "_tempDocuments".to_string() } else { collection.ok_or("collection is required for persistent document")? };
            let col = engine
                .get_collection(&target)
                .unwrap_or_else(|| engine.create_collection(target.clone()));
            // Parse JSON into BSON document
            let value: serde_json::Value = serde_json::from_str(&json)?;
            let bdoc: bson::Document = bson::to_document(&value)?;
            let mut doc = crate::document::Document::new(bdoc, if ephemeral { crate::document::DocumentType::Ephemeral } else { crate::document::DocumentType::Persistent });
            if ephemeral {
                if let Some(secs) = ttl_secs { doc.set_ttl(std::time::Duration::from_secs(secs)); }
            }
            let id = col.insert_document(doc);
            println!("{}", id.0);
            Ok(())
        }
        Command::ListEphemeral => {
            if let Some(col) = engine.get_collection("_tempDocuments") {
                for d in col.get_all_documents() {
                    let line = serde_json::to_string(&d.data.0)?;
                    println!("{}", line);
                }
            }
            Ok(())
        }
        Command::PurgeEphemeral { all } => {
            if let Some(col) = engine.get_collection("_tempDocuments") {
                let docs = col.get_all_documents();
                for d in docs {
                    if all || d.is_expired() { let _ = col.delete_document(&d.id); }
                }
            }
            Ok(())
        }
    }
}
