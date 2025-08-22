use crate::engine::Engine;
use crate::export::{export_file, ExportFormat, ExportOptions};
use crate::import::{import_file, ImportFormat, ImportOptions};
use crate::query::{self, FindOptions, Order, SortSpec};
use std::path::PathBuf;
use std::io::Write;

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
    ExportR { collection: String, file: PathBuf, format: Option<String>, redact_fields: Option<Vec<String>> },
    // Query subcommands (programmatic)
    QueryFind { collection: String, filter_json: String, project: Option<String>, sort: Option<String>, limit: Option<usize>, skip: Option<usize> },
    QueryFindR { collection: String, filter_json: String, project: Option<String>, sort: Option<String>, limit: Option<usize>, skip: Option<usize>, redact_fields: Option<Vec<String>> },
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
    // Crypto ops
    CryptoKeygenP256 { out_priv: Option<PathBuf>, out_pub: Option<PathBuf> },
    CryptoSignFile { key_priv: PathBuf, input: PathBuf, out_sig: Option<PathBuf> },
    CryptoVerifyFile { key_pub: PathBuf, input: PathBuf, sig: PathBuf },
    CryptoEncryptFile { key_pub: PathBuf, input: PathBuf, output: PathBuf },
    CryptoDecryptFile { key_priv: PathBuf, input: PathBuf, output: PathBuf },
    // Encrypted checkpoint/restore
    CheckpointEncrypted { db_path: PathBuf, key_pub: PathBuf, output: PathBuf },
    RestoreEncrypted { db_path: PathBuf, key_priv: PathBuf, input: PathBuf },
    // PBE encryption toggles
    EncryptDbPbe { db_path: PathBuf, username: String },
    DecryptDbPbe { db_path: PathBuf, username: String },
    /// Verify .db.sig and .wasp.sig using a public key PEM; prints results.
    VerifyDbSigs { db_path: PathBuf, key_pub_pem: String },
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
            // Detect PBE-encrypted DB/WASP and require username/password via env
            let p = db_path.to_str().ok_or("invalid path")?;
            let wasp_path = db_path.with_extension("wasp");
            let pbe_db = crate::crypto::pbe_is_encrypted(&db_path);
            let pbe_wasp = wasp_path.exists() && crate::crypto::pbe_is_encrypted(&wasp_path);
            if pbe_db || pbe_wasp {
                let username = std::env::var("NEXUSLITE_USERNAME").map_err(|_| "PBE-encrypted DB: set NEXUSLITE_USERNAME and NEXUSLITE_PASSWORD")?;
                let password = std::env::var("NEXUSLITE_PASSWORD").map_err(|_| "PBE-encrypted DB: set NEXUSLITE_USERNAME and NEXUSLITE_PASSWORD")?;
                crate::api::decrypt_db_with_password(db_path.as_path(), &username, &password)?;
            }
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
            let opts = ImportOptions { collection, format: parse_import_format(&format), ..Default::default() };
            let _report = import_file(engine, file, &opts)?;
            Ok(())
        }
        Command::Export { collection, file, format } => {
            let opts = ExportOptions { format: parse_export_format(&format), ..Default::default() };
            let _report = export_file(engine, &collection, file, &opts)?;
            Ok(())
        }
        Command::ExportR { collection, file, format, redact_fields } => {
            let mut opts = ExportOptions { format: parse_export_format(&format), ..Default::default() };
            if let Some(fields) = redact_fields && !fields.is_empty() { opts.redact_fields = Some(fields); }
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
        Command::QueryFindR { collection, filter_json, project, sort, limit, skip, redact_fields } => {
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
            for mut doc in cursor.to_vec() {
                if let Some(fields) = &redact_fields {
                    for f in fields {
                        if doc.data.0.contains_key(f) {
                            doc.data.0.insert(f.clone(), bson::Bson::String("***REDACTED***".into()));
                        }
                    }
                }
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
            if ephemeral && let Some(secs) = ttl_secs { doc.set_ttl(std::time::Duration::from_secs(secs)); }
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
        Command::CryptoKeygenP256 { out_priv, out_pub } => {
            let (priv_pem, pub_pem) = crate::api::crypto_generate_p256();
            if let Some(p) = out_priv {
                std::fs::write(&p, priv_pem.as_bytes())?;
                println!("written: {}", p.to_string_lossy());
            } else {
                println!("PRIVATE_PEM_BEGIN");
                print!("{}", priv_pem);
                if !priv_pem.ends_with('\n') { println!(); }
                println!("PRIVATE_PEM_END");
            }
            if let Some(p) = out_pub {
                std::fs::write(&p, pub_pem.as_bytes())?;
                println!("written: {}", p.to_string_lossy());
            } else {
                println!("PUBLIC_PEM_BEGIN");
                print!("{}", pub_pem);
                if !pub_pem.ends_with('\n') { println!(); }
                println!("PUBLIC_PEM_END");
            }
            Ok(())
        }
        Command::CryptoSignFile { key_priv, input, out_sig } => {
            let priv_pem = std::fs::read_to_string(&key_priv)?;
            let sig = crate::api::crypto_sign_file(&priv_pem, &input)?;
            if let Some(p) = out_sig {
                let mut f = std::fs::File::create(&p)?;
                f.write_all(&sig)?;
                println!("written: {}", p.to_string_lossy());
            } else {
                // print hex DER sig
                let hex = sig.iter().map(|b| format!("{:02x}", b)).collect::<String>();
                println!("{}", hex);
            }
            Ok(())
        }
        Command::CryptoVerifyFile { key_pub, input, sig } => {
            let pub_pem = std::fs::read_to_string(&key_pub)?;
            let sig_bytes = std::fs::read(&sig)?;
            let ok = crate::api::crypto_verify_file(&pub_pem, &input, &sig_bytes)?;
            println!("verified={}", ok);
            Ok(())
        }
        Command::CryptoEncryptFile { key_pub, input, output } => {
            let pub_pem = std::fs::read_to_string(&key_pub)?;
            crate::api::crypto_encrypt_file(&pub_pem, input.as_path(), output.as_path())?;
            println!("encrypted: {} -> {}", input.to_string_lossy(), output.to_string_lossy());
            Ok(())
        }
        Command::CryptoDecryptFile { key_priv, input, output } => {
            let priv_pem = std::fs::read_to_string(&key_priv)?;
            crate::api::crypto_decrypt_file(&priv_pem, input.as_path(), output.as_path())?;
            println!("decrypted: {} -> {}", input.to_string_lossy(), output.to_string_lossy());
            Ok(())
        }
        Command::CheckpointEncrypted { db_path, key_pub, output } => {
            let db_path_str = db_path.to_str().ok_or("invalid db path")?;
            let db = crate::Database::open(db_path_str)?;
            let pub_pem = std::fs::read_to_string(&key_pub)?;
            crate::api::checkpoint_encrypted(&db, output.as_path(), &pub_pem)?;
            println!("checkpoint_encrypted: {}", output.to_string_lossy());
            Ok(())
        }
        Command::RestoreEncrypted { db_path, key_priv, input } => {
            let priv_pem = std::fs::read_to_string(&key_priv)?;
            crate::api::restore_encrypted(db_path.as_path(), input.as_path(), &priv_pem)?;
            println!("restored_encrypted: {}", db_path.to_string_lossy());
            Ok(())
        }
        Command::EncryptDbPbe { db_path, username } => {
            let password = std::env::var("NEXUSLITE_PASSWORD").map_err(|_| "missing NEXUSLITE_PASSWORD env")?;
            crate::api::encrypt_db_with_password(db_path.as_path(), &username, &password)?;
            println!("encrypted (PBE): {}", db_path.to_string_lossy());
            Ok(())
        }
        Command::DecryptDbPbe { db_path, username } => {
            let password = std::env::var("NEXUSLITE_PASSWORD").map_err(|_| "missing NEXUSLITE_PASSWORD env")?;
            crate::api::decrypt_db_with_password(db_path.as_path(), &username, &password)?;
            println!("decrypted (PBE): {}", db_path.to_string_lossy());
            Ok(())
        }
        Command::VerifyDbSigs { db_path, key_pub_pem } => {
            let wasp = db_path.with_extension("wasp");
            let db_sig = db_path.with_extension("db.sig");
            let wasp_sig = wasp.with_extension("wasp.sig");
            let mut failed = false;
            if db_sig.exists() && let Ok(sig) = std::fs::read(&db_sig)
                && !crate::api::crypto_verify_file(&key_pub_pem, &db_path, &sig).unwrap_or(false) {
                eprintln!("SIGNATURE VERIFICATION FAILED: {}", db_path.display());
                failed = true;
            }
            if wasp.exists() && wasp_sig.exists() && let Ok(sig) = std::fs::read(&wasp_sig)
                && !crate::api::crypto_verify_file(&key_pub_pem, &wasp, &sig).unwrap_or(false) {
                eprintln!("SIGNATURE VERIFICATION FAILED: {}", wasp.display());
                failed = true;
            }
            if failed { Err("signature verification failed".into()) } else { Ok(()) }
        }
    }
}
