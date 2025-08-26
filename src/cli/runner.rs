use crate::engine::Engine;
use crate::errors::DbError;
use crate::export::{ExportOptions, export_file};
use crate::import::{ImportOptions, import_file};
use crate::query::{self, FindOptions, Order, SortSpec};
use std::io::Write;

use super::command::Command;
use super::util::{parse_export_format, parse_import_format};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum OutputMode {
    Human,
    Plain,
    Json,
}

pub fn run_with_format(
    engine: &Engine,
    cmd: Command,
    mode: OutputMode,
) -> Result<(), Box<dyn std::error::Error>> {
    match cmd {
        Command::Info => {
            let report = crate::api::info(engine);
            let json_report = serde_json::to_string_pretty(&report).unwrap_or_else(|_| "{}".into());
            match mode {
                OutputMode::Json => println!("{json_report}"),
                OutputMode::Plain => println!(
                    "collections={} total_persistent={} total_ephemeral={}",
                    report.collections.len(),
                    report.total_persistent,
                    report.total_ephemeral
                ),
                OutputMode::Human => println!("{json_report}"),
            }
            Ok(())
        }
        Command::DbCreate { db_path } => {
            let path_str = db_path.as_ref().and_then(|p| p.to_str());
            let db = crate::Database::new(path_str)?;
            let db_name = db.name();
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({"action":"created","name": db_name});
                    println!("{json}");
                }
                OutputMode::Plain => println!("created {db_name}"),
                OutputMode::Human => println!("created={db_name}.db name={db_name}"),
            }
            Ok(())
        }
        Command::DbOpen { db_path } => {
            // duplicate core of run's open handling without PBE (handled by main/runner run)
            let p = db_path.to_str().ok_or("invalid path")?;
            let db = crate::Database::open(p)?;
            let db_name = db.name();
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({"action":"opened","name": db_name});
                    println!("{json}");
                }
                OutputMode::Plain => println!("opened {db_name}"),
                OutputMode::Human => println!("opened name={db_name}"),
            }
            Ok(())
        }
        Command::DbClose { db_path } => {
            let p = db_path.to_str().ok_or("invalid path")?;
            crate::Database::close(Some(p))?;
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({"action":"closed","path": p});
                    println!("{json}");
                }
                OutputMode::Plain => println!("closed {p}"),
                OutputMode::Human => println!("closed path={p}"),
            }
            Ok(())
        }
        Command::ColList => {
            let names = engine.list_collection_names();
            match mode {
                OutputMode::Json => {
                    let names_json = serde_json::to_string(&names)?;
                    println!("{names_json}");
                }
                _ => {
                    for n in names {
                        println!("{n}");
                    }
                }
            }
            Ok(())
        }
        Command::QueryCount { collection, filter_json } => {
            let col = engine
                .get_collection(&collection)
                .ok_or_else(|| crate::errors::DbError::NoSuchCollection(collection.clone()))?;
            if !crate::telemetry::try_consume_token(&col.name_str(), 1) {
                crate::telemetry::log_rate_limited(&collection, "count");
                let ra = crate::telemetry::retry_after_ms(&col.name_str(), 1);
                return Err(Box::new(DbError::RateLimitedWithRetry { retry_after_ms: ra }));
            }
            let filter = query::parse_filter_json(&filter_json)?;
            let n = query::count_docs(&col, &filter);
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({"count": n});
                    println!("{json}");
                }
                _ => println!("{n}"),
            }
            Ok(())
        }
        Command::QueryUpdate { collection, filter_json, update_json } => {
            let col = engine
                .get_collection(&collection)
                .ok_or_else(|| crate::errors::DbError::NoSuchCollection(collection.clone()))?;
            let filter = query::parse_filter_json(&filter_json)?;
            let update = query::parse_update_json(&update_json)?;
            let r = query::update_many(&col, &filter, &update);
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({"matched": r.matched, "modified": r.modified});
                    println!("{json}");
                }
                _ => println!("{{\"matched\":{},\"modified\":{}}}", r.matched, r.modified),
            }
            Ok(())
        }
        Command::QueryDelete { collection, filter_json } => {
            let col = engine
                .get_collection(&collection)
                .ok_or_else(|| crate::errors::DbError::NoSuchCollection(collection.clone()))?;
            let filter = query::parse_filter_json(&filter_json)?;
            let r = query::delete_many(&col, &filter);
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({"deleted": r.deleted});
                    println!("{json}");
                }
                _ => println!("{{\"deleted\":{}}}", r.deleted),
            }
            Ok(())
        }
        Command::QueryUpdateOne { collection, filter_json, update_json } => {
            let col = engine
                .get_collection(&collection)
                .ok_or_else(|| crate::errors::DbError::NoSuchCollection(collection.clone()))?;
            let filter = query::parse_filter_json(&filter_json)?;
            let update = query::parse_update_json(&update_json)?;
            let r = query::update_one(&col, &filter, &update);
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({"matched": r.matched, "modified": r.modified});
                    println!("{json}");
                }
                _ => println!("{{\"matched\":{},\"modified\":{}}}", r.matched, r.modified),
            }
            Ok(())
        }
        Command::QueryDeleteOne { collection, filter_json } => {
            let col = engine
                .get_collection(&collection)
                .ok_or_else(|| crate::errors::DbError::NoSuchCollection(collection.clone()))?;
            let filter = query::parse_filter_json(&filter_json)?;
            let r = query::delete_one(&col, &filter);
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({"deleted": r.deleted});
                    println!("{json}");
                }
                _ => println!("{{\"deleted\":{}}}", r.deleted),
            }
            Ok(())
        }
        Command::CreateDocument { collection, json, ephemeral, ttl_secs } => {
            // Determine target collection
            let target = if ephemeral {
                "_tempDocuments".to_string()
            } else {
                collection.ok_or("collection is required for persistent document")?
            };
            let col = engine
                .get_collection(&target)
                .unwrap_or_else(|| engine.create_collection(target.clone()));
            // Parse JSON into BSON document
            let bdoc: bson::Document = crate::utils::json::parse_json_to_bson_document(&json)?;
            let mut doc = crate::document::Document::new(
                bdoc,
                if ephemeral {
                    crate::document::DocumentType::Ephemeral
                } else {
                    crate::document::DocumentType::Persistent
                },
            );
            if ephemeral && let Some(secs) = ttl_secs {
                doc.set_ttl(std::time::Duration::from_secs(secs));
            }
            let id = col.insert_document(doc);
            match mode {
                OutputMode::Json => {
                    let id0 = id.0;
                    let json = serde_json::json!({"id": id0});
                    println!("{json}");
                }
                _ => {
                    let id0 = id.0;
                    println!("{id0}");
                }
            }
            Ok(())
        }
        Command::RecoveryAutoRecover { enabled } => {
            crate::api::recovery_set_auto_recover(enabled);
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({"recovery_auto_recover": enabled});
                    println!("{json}");
                }
                _ => println!("recovery_auto_recover={enabled}"),
            }
            Ok(())
        }
        Command::RecoveryAutoRecoverGet => {
            let v = crate::api::recovery_auto_recover();
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({"recovery_auto_recover": v});
                    println!("{json}");
                }
                _ => println!("recovery_auto_recover={v}"),
            }
            Ok(())
        }
        Command::FeatureList => {
            let list = crate::api::feature_list();
            match mode {
                OutputMode::Json => {
                    let list_json = serde_json::to_string(&list)?;
                    println!("{list_json}");
                }
                _ => {
                    for f in list {
                        println!("{}\tenabled={}\t{}", f.name, f.enabled, f.description);
                    }
                }
            }
            Ok(())
        }
        Command::FeatureEnable { name } => {
            crate::api::feature_enable(&name)?;
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({"feature": name, "enabled": true});
                    println!("{json}");
                }
                _ => println!("feature {name}=enabled"),
            }
            Ok(())
        }
        Command::FeatureDisable { name } => {
            crate::api::feature_disable(&name)?;
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({"feature": name, "enabled": false});
                    println!("{json}");
                }
                _ => println!("feature {name}=disabled"),
            }
            Ok(())
        }
        Command::CryptoVerifyFile { key_pub, input, sig } => {
            let pub_pem = std::fs::read_to_string(&key_pub)?;
            let sig_bytes = std::fs::read(&sig)?;
            let ok = crate::api::crypto_verify_file(&pub_pem, &input, &sig_bytes)?;
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({"verified": ok});
                    println!("{json}");
                }
                _ => println!("verified={ok}"),
            }
            Ok(())
        }
        Command::CryptoEncryptFile { key_pub, input, output } => {
            let pub_pem = std::fs::read_to_string(&key_pub)?;
            crate::api::crypto_encrypt_file(&pub_pem, input.as_path(), output.as_path())?;
            let input_string = input.to_string_lossy();
            let output_string = output.to_string_lossy();
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({
                        "encrypted": true,
                        "input": input_string,
                        "output": output_string
                    });
                    println!("{json}");
                }
                _ => println!("encrypted: {input_string} -> {output_string}"),
            }
            Ok(())
        }
        Command::CryptoDecryptFile { key_priv, input, output } => {
            let priv_pem = std::fs::read_to_string(&key_priv)?;
            crate::api::crypto_decrypt_file(&priv_pem, input.as_path(), output.as_path())?;
            let input_string = input.to_string_lossy();
            let output_string = output.to_string_lossy();
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({
                        "decrypted": true,
                        "input": input_string,
                        "output": output_string
                    });
                    println!("{json}");
                }
                _ => println!("decrypted: {input_string} -> {output_string}"),
            }
            Ok(())
        }
        Command::Version => {
            let engine = Engine::new(std::env::temp_dir().join("nexuslite_features.wal"))
                .map_err(|e| format!("engine: {e}"))?;
            let report = crate::api::info(&engine);
            match mode {
                OutputMode::Json => {
                    let json = serde_json::to_string_pretty(&report)?;
                    println!("{json}");
                }
                _ => {
                    println!("package: {} {}", report.package_name, report.package_version);
                    println!(
                        "compiled_features: {}",
                        if report.compiled_features.is_empty() {
                            "<none>".into()
                        } else {
                            report.compiled_features.join(",")
                        }
                    );
                    println!("runtime_flags:");
                    for f in report.runtime_flags {
                        println!("  {}\tenabled={}\t{}", f.name, f.enabled, f.description);
                    }
                }
            }
            Ok(())
        }
        Command::FeatureInfo { name } => {
            if let Some(f) = crate::api::feature_info(&name) {
                match mode {
                    OutputMode::Json => {
                        let json = serde_json::to_string(&f)?;
                        println!("{json}");
                    }
                    _ => {
                        println!("{}\tenabled={}\t{}", f.name, f.enabled, f.description);
                        if let Some(opts) = f.options {
                            for (k, v) in opts {
                                println!("  {}={} ", k, v);
                            }
                        }
                    }
                }
                Ok(())
            } else {
                Err(format!("unknown feature flag: {}", name).into())
            }
        }
        Command::LogConfig { dir, level, retention } => {
            crate::api::log_configure(dir.as_deref(), level.as_deref(), retention);
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({"log_configured": true});
                    println!("{json}");
                }
                _ => println!("log_configured"),
            }
            Ok(())
        }
        Command::RecoveryValidateResilience { path } => {
            let ok = crate::recovery::recover::validate_resilience(&path)?;
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({"validate_resilience": ok, "path": path.to_string_lossy()});
                    println!("{json}");
                }
                _ => println!("validate_resilience={} path={}", ok, path.to_string_lossy()),
            }
            Ok(())
        }
        Command::CheckpointEncrypted { db_path, key_pub, output } => {
            let db_path_str = db_path.to_str().ok_or("invalid db path")?;
            let db = crate::Database::open(db_path_str)?;
            let pub_pem = std::fs::read_to_string(&key_pub)?;
            crate::api::checkpoint_encrypted(&db, output.as_path(), &pub_pem)?;
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({"checkpoint_encrypted": true, "output": output.to_string_lossy()});
                    println!("{json}");
                }
                _ => println!("checkpoint_encrypted: {}", output.to_string_lossy()),
            }
            Ok(())
        }
        Command::RestoreEncrypted { db_path, key_priv, input } => {
            let priv_pem = std::fs::read_to_string(&key_priv)?;
            crate::api::restore_encrypted(db_path.as_path(), input.as_path(), &priv_pem)?;
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({"restored_encrypted": true, "db_path": db_path.to_string_lossy()});
                    println!("{json}");
                }
                _ => println!("restored_encrypted: {}", db_path.to_string_lossy()),
            }
            Ok(())
        }
        Command::EncryptDbPbe { db_path, username } => {
            let password = std::env::var("NEXUSLITE_PASSWORD")
                .map_err(|_| "missing NEXUSLITE_PASSWORD env")?;
            crate::api::encrypt_db_with_password(db_path.as_path(), &username, &password)?;
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({"encrypted_pbe": true, "db": db_path.to_string_lossy()});
                    println!("{json}");
                }
                _ => println!("encrypted (PBE): {}", db_path.to_string_lossy()),
            }
            Ok(())
        }
        Command::DecryptDbPbe { db_path, username } => {
            let password = std::env::var("NEXUSLITE_PASSWORD")
                .map_err(|_| "missing NEXUSLITE_PASSWORD env")?;
            crate::api::decrypt_db_with_password(db_path.as_path(), &username, &password)?;
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({"decrypted_pbe": true, "db": db_path.to_string_lossy()});
                    println!("{json}");
                }
                _ => println!("decrypted (PBE): {}", db_path.to_string_lossy()),
            }
            Ok(())
        }
        Command::TelemetrySetSlow { ms } => {
            crate::telemetry::set_slow_query_ms(ms);
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({"slow_query_ms": ms});
                    println!("{json}");
                }
                _ => println!("slow_query_ms={ms}"),
            }
            Ok(())
        }
        Command::TelemetrySetAudit { enabled } => {
            crate::telemetry::set_audit_enabled(enabled);
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({"audit": enabled});
                    println!("{json}");
                }
                _ => println!("audit={enabled}"),
            }
            Ok(())
        }
        Command::TelemetrySetQueryLog { path, slow_ms, structured } => {
            crate::telemetry::set_query_log(path.clone(), slow_ms, structured);
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({
                        "query_log": path.to_string_lossy(),
                        "slow_ms": slow_ms,
                        "structured": structured
                    });
                    println!("{json}");
                }
                _ => println!("query_log={}", path.to_string_lossy()),
            }
            Ok(())
        }
        Command::TelemetrySetMaxGlobal { limit } => {
            crate::telemetry::set_max_result_limit_global(limit);
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({"max_results_global": limit});
                    println!("{json}");
                }
                _ => println!("max_results_global={limit}"),
            }
            Ok(())
        }
        Command::TelemetrySetMaxFor { collection, limit } => {
            crate::telemetry::set_max_result_limit_for(&collection, limit);
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({"collection": collection, "max_results": limit});
                    println!("{json}");
                }
                _ => println!("max_results[{collection}]={limit}"),
            }
            Ok(())
        }
        Command::TelemetryRateLimit { collection, capacity, refill_per_sec } => {
            crate::telemetry::configure_rate_limit(&collection, capacity, refill_per_sec);
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({
                        "collection": collection,
                        "capacity": capacity,
                        "refill_per_sec": refill_per_sec
                    });
                    println!("{json}");
                }
                _ => println!("rate_limit[{collection}]=cap:{capacity} rps:{refill_per_sec}"),
            }
            Ok(())
        }
        Command::TelemetryRateRemove { collection } => {
            crate::telemetry::remove_rate_limit(&collection);
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({"collection": collection, "removed": true});
                    println!("{json}");
                }
                _ => println!("rate_limit removed [{collection}]"),
            }
            Ok(())
        }
        Command::TelemetryRateDefault { capacity, refill_per_sec } => {
            crate::telemetry::set_default_rate_limit(capacity, refill_per_sec);
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({"capacity": capacity, "refill_per_sec": refill_per_sec});
                    println!("{json}");
                }
                _ => println!("rate_limit_default cap:{capacity} rps:{refill_per_sec}"),
            }
            Ok(())
        }
        Command::DoctorSummary { wasp_access, config_files, env_secret_keys, advice, status } => {
            match mode {
                OutputMode::Json => {
                    let json = serde_json::json!({
                        "wasp_access": wasp_access.as_ref().map(|(ok, p)| serde_json::json!({"ok": ok, "path": p})),
                        "config_files": config_files.iter().map(|(path, st)| serde_json::json!({"path": path, "status": st})).collect::<Vec<_>>(),
                        "env_secrets": env_secret_keys,
                        "advice": advice,
                        "status": status,
                    });
                    println!("{}", serde_json::to_string_pretty(&json)?);
                }
                OutputMode::Plain => {
                    if let Some((ok, p)) = wasp_access {
                        println!("wasp_access:{} path:{}", ok, p);
                    } else {
                        println!("no_db_specified");
                    }
                    println!("config_scanned:{}", config_files.len());
                    for (p, st) in config_files {
                        println!("config_file:{} status:{}", p, st);
                    }
                    if !env_secret_keys.is_empty() {
                        println!("env_secrets:{}", env_secret_keys.len());
                        for k in env_secret_keys {
                            println!("env:{}=REDACTED", k);
                        }
                    }
                    println!("status:{}", status);
                    println!("advice:{}", advice);
                }
                OutputMode::Human => {
                    if let Some((ok, p)) = wasp_access {
                        println!("wasp_access:{} path:{}", ok, p);
                    } else {
                        println!("no_db_specified");
                    }
                    println!("config_scanned:{}", config_files.len());
                    for (p, st) in config_files {
                        println!("config_file:{} status:{}", p, st);
                    }
                    if !env_secret_keys.is_empty() {
                        println!(
                            "env_secrets: {} entries (values REDACTED)",
                            env_secret_keys.len()
                        );
                        for k in env_secret_keys {
                            println!("env:{}=REDACTED", k);
                        }
                    }
                    println!("status:{}", status);
                    println!("advice: {}", advice);
                }
            }
            Ok(())
        }
        // Fallback to default runner for other commands (behavior unchanged)
        other => run(engine, other),
    }
}

pub fn run(engine: &Engine, cmd: Command) -> Result<(), Box<dyn std::error::Error>> {
    match cmd {
        Command::Info => {
            let report = crate::api::info(engine);
            println!("{}", serde_json::to_string_pretty(&report).unwrap_or_else(|_| "{}".into()));
            Ok(())
        }
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
                let username = std::env::var("NEXUSLITE_USERNAME").map_err(
                    |_| "PBE-encrypted DB: set NEXUSLITE_USERNAME and NEXUSLITE_PASSWORD",
                )?;
                let password = std::env::var("NEXUSLITE_PASSWORD").map_err(
                    |_| "PBE-encrypted DB: set NEXUSLITE_USERNAME and NEXUSLITE_PASSWORD",
                )?;
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
            for n in names {
                println!("{}", n);
            }
            Ok(())
        }
        Command::ColRename { old, new } => {
            engine.rename_collection(&old, &new)?;
            Ok(())
        }
        Command::Import { collection, file, format } => {
            let opts = ImportOptions {
                collection,
                format: parse_import_format(&format),
                ..Default::default()
            };
            let _report = import_file(engine, file, &opts)?;
            Ok(())
        }
        Command::Export { collection, file, format, redact_fields, filter_json, limit } => {
            let mut opts =
                ExportOptions { format: parse_export_format(&format), ..Default::default() };
            if let Some(fields) = redact_fields
                && !fields.is_empty()
            {
                opts.redact_fields = Some(fields);
            }
            if let Some(fj) = filter_json {
                let f = query::parse_filter_json(&fj)?;
                opts.filter = Some(f);
            }
            opts.limit = limit;
            let _report = export_file(engine, &collection, file, &opts)?;
            Ok(())
        }
        Command::QueryFind { collection, filter_json, project, sort, limit, skip } => {
            let col = engine
                .get_collection(&collection)
                .ok_or_else(|| crate::errors::DbError::NoSuchCollection(collection.clone()))?;
            if !crate::telemetry::try_consume_token(&col.name_str(), 1) {
                crate::telemetry::log_rate_limited(&collection, "find");
                let ra = crate::telemetry::retry_after_ms(&col.name_str(), 1);
                return Err(Box::new(DbError::RateLimitedWithRetry { retry_after_ms: ra }));
            }
            let filter = query::parse_filter_json(&filter_json)?;
            let mut opts = FindOptions::default();
            if let Some(p) = project {
                opts.projection = Some(
                    p.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect(),
                );
            }
            if let Some(s) = sort {
                let mut specs = Vec::new();
                for part in s.split(',') {
                    let part = part.trim();
                    if part.is_empty() {
                        continue;
                    }
                    let (order, field) = if let Some(rest) = part.strip_prefix('-') {
                        (Order::Desc, rest)
                    } else if let Some(rest) = part.strip_prefix('+') {
                        (Order::Asc, rest)
                    } else {
                        (Order::Asc, part)
                    };
                    specs.push(SortSpec { field: field.to_string(), order });
                }
                if !specs.is_empty() {
                    opts.sort = Some(specs);
                }
            }
            opts.limit = limit;
            opts.skip = skip;
            let cursor = query::find_docs(&col, &filter, &opts);
            // Stream as NDJSON to stdout
            for doc in cursor.to_vec() {
                let line = serde_json::to_string(&doc.data.0)?;
                println!("{}", line);
            }
            Ok(())
        }
        Command::QueryFindR {
            collection,
            filter_json,
            project,
            sort,
            limit,
            skip,
            redact_fields,
        } => {
            let col = engine
                .get_collection(&collection)
                .ok_or_else(|| crate::errors::DbError::NoSuchCollection(collection.clone()))?;
            if !crate::telemetry::try_consume_token(&col.name_str(), 1) {
                crate::telemetry::log_rate_limited(&collection, "find");
                let ra = crate::telemetry::retry_after_ms(&col.name_str(), 1);
                return Err(Box::new(DbError::RateLimitedWithRetry { retry_after_ms: ra }));
            }
            let filter = query::parse_filter_json(&filter_json)?;
            let mut opts = FindOptions::default();
            if let Some(p) = project {
                opts.projection = Some(
                    p.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect(),
                );
            }
            if let Some(s) = sort {
                let mut specs = Vec::new();
                for part in s.split(',') {
                    let part = part.trim();
                    if part.is_empty() {
                        continue;
                    }
                    let (order, field) = if let Some(rest) = part.strip_prefix('-') {
                        (Order::Desc, rest)
                    } else if let Some(rest) = part.strip_prefix('+') {
                        (Order::Asc, rest)
                    } else {
                        (Order::Asc, part)
                    };
                    specs.push(SortSpec { field: field.to_string(), order });
                }
                if !specs.is_empty() {
                    opts.sort = Some(specs);
                }
            }
            opts.limit = limit;
            opts.skip = skip;
            let cursor = query::find_docs(&col, &filter, &opts);
            for mut doc in cursor.to_vec() {
                if let Some(fields) = &redact_fields {
                    for f in fields {
                        if doc.data.0.contains_key(f) {
                            doc.data
                                .0
                                .insert(f.clone(), bson::Bson::String("***REDACTED***".into()));
                        }
                    }
                }
                let line = serde_json::to_string(&doc.data.0)?;
                println!("{}", line);
            }
            Ok(())
        }
        Command::QueryCount { collection, filter_json } => {
            let col = engine
                .get_collection(&collection)
                .ok_or_else(|| crate::errors::DbError::NoSuchCollection(collection.clone()))?;
            if !crate::telemetry::try_consume_token(&col.name_str(), 1) {
                crate::telemetry::log_rate_limited(&collection, "count");
                let ra = crate::telemetry::retry_after_ms(&col.name_str(), 1);
                return Err(Box::new(DbError::RateLimitedWithRetry { retry_after_ms: ra }));
            }
            let filter = query::parse_filter_json(&filter_json)?;
            let n = query::count_docs(&col, &filter);
            println!("{}", n);
            Ok(())
        }
        Command::QueryUpdate { collection, filter_json, update_json } => {
            let col = engine
                .get_collection(&collection)
                .ok_or_else(|| crate::errors::DbError::NoSuchCollection(collection.clone()))?;
            let filter = query::parse_filter_json(&filter_json)?;
            let update = query::parse_update_json(&update_json)?;
            let r = query::update_many(&col, &filter, &update);
            println!("{{\"matched\":{},\"modified\":{}}}", r.matched, r.modified);
            Ok(())
        }
        Command::QueryDelete { collection, filter_json } => {
            let col = engine
                .get_collection(&collection)
                .ok_or_else(|| crate::errors::DbError::NoSuchCollection(collection.clone()))?;
            let filter = query::parse_filter_json(&filter_json)?;
            let r = query::delete_many(&col, &filter);
            println!("{{\"deleted\":{}}}", r.deleted);
            Ok(())
        }
        Command::QueryUpdateOne { collection, filter_json, update_json } => {
            let col = engine
                .get_collection(&collection)
                .ok_or_else(|| crate::errors::DbError::NoSuchCollection(collection.clone()))?;
            let filter = query::parse_filter_json(&filter_json)?;
            let update = query::parse_update_json(&update_json)?;
            let r = query::update_one(&col, &filter, &update);
            println!("{{\"matched\":{},\"modified\":{}}}", r.matched, r.modified);
            Ok(())
        }
        Command::QueryDeleteOne { collection, filter_json } => {
            let col = engine
                .get_collection(&collection)
                .ok_or_else(|| crate::errors::DbError::NoSuchCollection(collection.clone()))?;
            let filter = query::parse_filter_json(&filter_json)?;
            let r = query::delete_one(&col, &filter);
            println!("{{\"deleted\":{}}}", r.deleted);
            Ok(())
        }
        Command::CreateDocument { collection, json, ephemeral, ttl_secs } => {
            // Determine target collection
            let target = if ephemeral {
                "_tempDocuments".to_string()
            } else {
                collection.ok_or("collection is required for persistent document")?
            };
            let col = engine
                .get_collection(&target)
                .unwrap_or_else(|| engine.create_collection(target.clone()));
            // Parse JSON into BSON document
            let bdoc: bson::Document = crate::utils::json::parse_json_to_bson_document(&json)?;
            let mut doc = crate::document::Document::new(
                bdoc,
                if ephemeral {
                    crate::document::DocumentType::Ephemeral
                } else {
                    crate::document::DocumentType::Persistent
                },
            );
            if ephemeral && let Some(secs) = ttl_secs {
                doc.set_ttl(std::time::Duration::from_secs(secs));
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
                    if all || d.is_expired() {
                        let _ = col.delete_document(&d.id);
                    }
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
                if !priv_pem.ends_with('\n') {
                    println!();
                }
                println!("PRIVATE_PEM_END");
            }
            if let Some(p) = out_pub {
                std::fs::write(&p, pub_pem.as_bytes())?;
                println!("written: {}", p.to_string_lossy());
            } else {
                println!("PUBLIC_PEM_BEGIN");
                print!("{}", pub_pem);
                if !pub_pem.ends_with('\n') {
                    println!();
                }
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
        Command::TelemetrySetSlow { ms } => {
            crate::telemetry::set_slow_query_ms(ms);
            println!("slow_query_ms={}", ms);
            Ok(())
        }
        Command::TelemetrySetAudit { enabled } => {
            crate::telemetry::set_audit_enabled(enabled);
            println!("audit={}", enabled);
            Ok(())
        }
        Command::TelemetrySetQueryLog { path, slow_ms, structured } => {
            crate::telemetry::set_query_log(path.clone(), slow_ms, structured);
            println!("query_log={}", path.to_string_lossy());
            Ok(())
        }
        Command::TelemetrySetMaxGlobal { limit } => {
            crate::telemetry::set_max_result_limit_global(limit);
            println!("max_results_global={}", limit);
            Ok(())
        }
        Command::TelemetrySetMaxFor { collection, limit } => {
            crate::telemetry::set_max_result_limit_for(&collection, limit);
            println!("max_results[{}]={}", collection, limit);
            Ok(())
        }
        Command::TelemetryRateLimit { collection, capacity, refill_per_sec } => {
            crate::telemetry::configure_rate_limit(&collection, capacity, refill_per_sec);
            println!("rate_limit[{}]=cap:{} rps:{}", collection, capacity, refill_per_sec);
            Ok(())
        }
        Command::TelemetryRateRemove { collection } => {
            crate::telemetry::remove_rate_limit(&collection);
            println!("rate_limit removed [{}]", collection);
            Ok(())
        }
        Command::TelemetryRateDefault { capacity, refill_per_sec } => {
            crate::telemetry::set_default_rate_limit(capacity, refill_per_sec);
            println!("rate_limit_default cap:{} rps:{}", capacity, refill_per_sec);
            Ok(())
        }
        Command::RecoveryAutoRecover { enabled } => {
            crate::api::recovery_set_auto_recover(enabled);
            println!("recovery_auto_recover={}", enabled);
            Ok(())
        }
        Command::RecoveryAutoRecoverGet => {
            let v = crate::api::recovery_auto_recover();
            println!("recovery_auto_recover={}", v);
            Ok(())
        }
        Command::RecoveryValidateResilience { path } => {
            let ok = crate::recovery::recover::validate_resilience(&path)?;
            println!("validate_resilience={} path={} ", ok, path.to_string_lossy());
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
            let password = std::env::var("NEXUSLITE_PASSWORD")
                .map_err(|_| "missing NEXUSLITE_PASSWORD env")?;
            crate::api::encrypt_db_with_password(db_path.as_path(), &username, &password)?;
            println!("encrypted (PBE): {}", db_path.to_string_lossy());
            Ok(())
        }
        Command::DecryptDbPbe { db_path, username } => {
            let password = std::env::var("NEXUSLITE_PASSWORD")
                .map_err(|_| "missing NEXUSLITE_PASSWORD env")?;
            crate::api::decrypt_db_with_password(db_path.as_path(), &username, &password)?;
            println!("decrypted (PBE): {}", db_path.to_string_lossy());
            Ok(())
        }
        Command::VerifyDbSigs { db_path, key_pub_pem } => {
            let wasp = db_path.with_extension("wasp");
            let db_sig = db_path.with_extension("db.sig");
            let wasp_sig = wasp.with_extension("wasp.sig");
            let mut failed = false;
            if db_sig.exists()
                && let Ok(sig) = std::fs::read(&db_sig)
                && !crate::api::crypto_verify_file(&key_pub_pem, &db_path, &sig).unwrap_or(false)
            {
                eprintln!("SIGNATURE VERIFICATION FAILED: {}", db_path.display());
                failed = true;
            }
            if wasp.exists()
                && wasp_sig.exists()
                && let Ok(sig) = std::fs::read(&wasp_sig)
                && !crate::api::crypto_verify_file(&key_pub_pem, &wasp, &sig).unwrap_or(false)
            {
                eprintln!("SIGNATURE VERIFICATION FAILED: {}", wasp.display());
                failed = true;
            }
            if failed { Err("signature verification failed".into()) } else { Ok(()) }
        }
        Command::FeatureList => {
            let list = crate::api::feature_list();
            for f in list {
                println!("{}\tenabled={}\t{}", f.name, f.enabled, f.description);
            }
            Ok(())
        }
        Command::FeatureEnable { name } => {
            crate::api::feature_enable(&name)?;
            println!("feature {}=enabled", name);
            Ok(())
        }
        Command::FeatureDisable { name } => {
            crate::api::feature_disable(&name)?;
            println!("feature {}=disabled", name);
            Ok(())
        }
        Command::Version => {
            let engine = Engine::new(std::env::temp_dir().join("nexuslite_features.wal"))
                .map_err(|e| format!("engine: {e}"))?;
            let report = crate::api::info(&engine);
            println!("package: {} {}", report.package_name, report.package_version);
            println!(
                "compiled_features: {}",
                if report.compiled_features.is_empty() {
                    "<none>".into()
                } else {
                    report.compiled_features.join(",")
                }
            );
            println!("runtime_flags:");
            for f in report.runtime_flags {
                println!("  {}\tenabled={}\t{}", f.name, f.enabled, f.description);
            }
            Ok(())
        }
        Command::Check => {
            // Accept only known flags; fail if an unexpected flag appears in the registry
            let known = [
                "crypto",
                "open-metrics",
                "regex",
                "cli-bin",
                "db-logging",
                "telemetry-adv",
                "recovery",
                "doctor",
                "repl",
            ];
            let list = crate::api::feature_list();
            let mut unknown: Vec<String> = Vec::new();
            for f in list {
                if !known.contains(&f.name.as_str()) {
                    unknown.push(f.name);
                }
            }
            if unknown.is_empty() {
                Ok(())
            } else {
                Err(format!("unknown feature flags: {}", unknown.join(",")).into())
            }
        }
        Command::FeatureInfo { name } => {
            if let Some(f) = crate::api::feature_info(&name) {
                println!("{}\tenabled={}\t{}", f.name, f.enabled, f.description);
                if let Some(opts) = f.options {
                    for (k, v) in opts {
                        println!("  {}={} ", k, v);
                    }
                }
                Ok(())
            } else {
                Err(format!("unknown feature flag: {}", name).into())
            }
        }
        Command::LogConfig { dir, level, retention } => {
            crate::api::log_configure(dir.as_deref(), level.as_deref(), retention);
            println!("log_configured");
            Ok(())
        }
        Command::DoctorSummary { wasp_access, config_files, env_secret_keys, advice, status } => {
            if let Some((ok, p)) = wasp_access {
                println!("wasp_access:{} path:{}", ok, p);
            } else {
                println!("no_db_specified");
            }
            println!("config_scanned:{}", config_files.len());
            for (p, st) in config_files {
                println!("config_file:{} status:{}", p, st);
            }
            if !env_secret_keys.is_empty() {
                println!("env_secrets: {} entries (values REDACTED)", env_secret_keys.len());
                for k in env_secret_keys {
                    println!("env:{}=REDACTED", k);
                }
                println!("status:warning");
            }
            println!("advice: {}", advice);
            println!("status:{}", status);
            Ok(())
        }
    }
}
