use clap::{Parser, Subcommand};
use nexus_lite::{engine::Engine, cli as prog_cli};
use std::path::PathBuf;
use serde::{Serialize, Deserialize};
use nexus_lite::document::DocumentType;
use std::collections::VecDeque;
use std::io::{IsTerminal, Write};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct AppConfig {
    db_path: Option<PathBuf>,
    log_config: Option<PathBuf>,
    default_collection: Option<String>,
    // Signature verification policy: "warn" or "fail"
    sig_policy: Option<String>,
}

fn load_config(cli_cfg: Option<PathBuf>) -> AppConfig {
    // Precedence: CLI > env > config files > defaults
    // 1) Start with defaults
    let mut cfg = AppConfig::default();
    // 2) Load from config files (~/.config/nexuslite.toml, ./.nexusliterc, custom path)
    let mut paths: Vec<PathBuf> = vec![];
    if let Some(p) = &cli_cfg { paths.push(p.clone()); }
    if let Ok(p) = std::env::var("NEXUSLITE_CONFIG") { paths.push(PathBuf::from(p)); }
    if let Ok(home) = std::env::var("USERPROFILE").or_else(|_| std::env::var("HOME")) {
        let home_pb = PathBuf::from(home);
        paths.push(home_pb.join(".nexusliterc"));
        paths.push(home_pb.join(".config").join("nexuslite.toml"));
    }
    if let Ok(cur) = std::env::current_dir() { paths.push(cur.join("nexuslite.toml")); }
    for p in paths {
        if p.exists() {
            if let Ok(s) = std::fs::read_to_string(&p) {
                if let Ok(file_cfg) = toml::from_str::<AppConfig>(&s) {
                    if cfg.db_path.is_none() { cfg.db_path = file_cfg.db_path; }
                    if cfg.log_config.is_none() { cfg.log_config = file_cfg.log_config; }
                    if cfg.default_collection.is_none() { cfg.default_collection = file_cfg.default_collection; }
                    if cfg.sig_policy.is_none() { cfg.sig_policy = file_cfg.sig_policy; }
                }
            }
        }
    }
    // 3) Environment variables
    if cfg.db_path.is_none() {
        if let Ok(s) = std::env::var("NEXUSLITE_DB") { cfg.db_path = Some(PathBuf::from(s)); }
    }
    if cfg.log_config.is_none() {
        if let Ok(s) = std::env::var("NEXUSLITE_LOG_CONFIG") { cfg.log_config = Some(PathBuf::from(s)); }
    }
    if cfg.default_collection.is_none() {
        if let Ok(s) = std::env::var("NEXUSLITE_DEFAULT_COLLECTION") { cfg.default_collection = Some(s); }
    }
    if cfg.sig_policy.is_none() {
        if let Ok(s) = std::env::var("NEXUSLITE_SIG_POLICY") { cfg.sig_policy = Some(s); }
    }
    cfg
}

fn find_config_paths(cli_cfg: &Option<PathBuf>) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = vec![];
    if let Some(p) = cli_cfg { paths.push(p.clone()); }
    if let Ok(p) = std::env::var("NEXUSLITE_CONFIG") { paths.push(PathBuf::from(p)); }
    if let Ok(home) = std::env::var("USERPROFILE").or_else(|_| std::env::var("HOME")) {
        let home_pb = PathBuf::from(home);
        paths.push(home_pb.join(".nexusliterc"));
        paths.push(home_pb.join(".config").join("nexuslite.toml"));
    }
    if let Ok(cur) = std::env::current_dir() { paths.push(cur.join("nexuslite.toml")); }
    paths
}

fn is_secret_key(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    k.contains("password") || k.contains("passwd") || k.contains("secret") || k.contains("token") || k.contains("apikey") || k.contains("api_key") || k.contains("private_key")
}

fn scan_toml_for_secret_keys(val: &toml::Value) -> Vec<String> {
    let mut secrets = Vec::new();
    let mut q = VecDeque::new();
    q.push_back((String::new(), val));
    while let Some((prefix, v)) = q.pop_front() {
        match v {
            toml::Value::Table(map) => {
                for (k, vv) in map {
                    let full = if prefix.is_empty() { k.clone() } else { format!("{}.{k}", prefix) };
                    if is_secret_key(k) { secrets.push(full.clone()); }
                    q.push_back((full, vv));
                }
            }
            toml::Value::Array(arr) => {
                for (i, vv) in arr.iter().enumerate() {
                    q.push_back((format!("{}[{}]", prefix, i), vv));
                }
            }
            _ => {}
        }
    }
    secrets
}

#[derive(Parser, Debug)]
#[command(name = "nexuslite", version, about = "NexusLite database CLI", long_about=None)]
struct Cli {
    /// Path to a config file (TOML)
    #[arg(long, help = "Path to a config file (TOML). If omitted, defaults are used.")]
    config: Option<PathBuf>,
    /// Override DB path (takes precedence over config)
    #[arg(long, help = "Override database path (e.g., mydb.db). Takes precedence over config/env.")]
    db: Option<PathBuf>,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    // DB and collections
    #[command(name = "new-db", about = "Create a new database (creates .db and .wasp if missing)")]
    NewDb {
        #[arg(help = "Database file path (e.g., mydb.db). If omitted, defaults are used.")]
        path: Option<PathBuf>
    },
    #[command(name = "open-db", about = "Open an existing database and initialize the engine")]
    OpenDb {
        #[arg(help = "Database file path to open (e.g., mydb.db)")]
    path: PathBuf,
    #[arg(long, help = "Verify .db.sig and .wasp.sig with the provided public key")]
    verify_sig: bool,
    #[arg(long, requires = "verify_sig", help = "Path to ECDSA P-256 public key PEM for signature verification")]
    pubkey: Option<PathBuf>,
    #[arg(long, requires = "verify_sig", help = "Treat signature failures as warnings instead of errors")]
    sig_warn: bool,
    },
    #[command(name = "close-db", about = "Close/unregister a previously opened database handle")]
    CloseDb {
        #[arg(help = "Database file path to close (e.g., mydb.db)")]
        path: PathBuf
    },
    #[command(name = "create-collection", about = "Create a collection in the current database")]
    ColCreate {
        #[arg(help = "Collection name to create")]
        name: String
    },
    #[command(name = "delete-collection", about = "Delete a collection from the current database")]
    ColDelete {
        #[arg(help = "Collection name to delete")]
        name: String
    },
    #[command(name = "list-collections", about = "List all collections in the current database")]
    ColList,
    #[command(name = "rename-collection", about = "Rename a collection in the current database")]
    ColRename {
        #[arg(help = "Existing collection name")]
        old: String,
        #[arg(help = "New collection name")]
        new: String
    },
    // Import/Export
    #[command(about = "Import data into a collection from a file (NDJSON/CSV/BSON)")]
    Import {
        #[arg(help = "Target collection name; falls back to default_collection if set in config")]
        collection: Option<String>,
        #[arg(help = "Path to input file")]
        file: PathBuf,
        #[arg(help = "Format override: ndjson|csv|bson; defaults to auto-detect")]
        format: Option<String>
    },
    #[command(about = "Export a collection to a file (NDJSON/CSV/BSON)")]
    Export {
        #[arg(help = "Collection name to export; falls back to default_collection if set in config")]
        collection: Option<String>,
        #[arg(help = "Path to output file")]
        file: PathBuf,
        #[arg(help = "Format: ndjson|csv|bson; defaults to ndjson")]
    format: Option<String>,
    #[arg(long, value_delimiter = ',', help = "Comma-separated list of top-level fields to redact/mask in outputs (NDJSON/CSV)")]
    redact: Option<Vec<String>>,
    },
    // Query
    #[command(about = "Find documents matching a filter; prints NDJSON to stdout")]
    Find {
        #[arg(help = "Collection name; falls back to default_collection if set in config")]
        collection: Option<String>,
        #[arg(help = "Filter JSON (e.g., {\"age\": {\"$gte\": 21}})")]
        filter: String,
        #[arg(help = "Projection fields comma-separated (e.g., name,age)")]
        project: Option<String>,
        #[arg(help = "Sort spec comma-separated; -age,+name")]
        sort: Option<String>,
        #[arg(help = "Limit results")]
        limit: Option<usize>,
        #[arg(help = "Skip N results")]
    skip: Option<usize>,
    #[arg(long, value_delimiter = ',', help = "Comma-separated list of top-level fields to redact in output")]
    redact: Option<Vec<String>>,
    },
    #[command(about = "Count documents matching a filter")]
    Count {
        #[arg(help = "Collection name; falls back to default_collection if set in config")]
        collection: Option<String>,
        #[arg(help = "Filter JSON")]
        filter: String
    },
    #[command(about = "Update all documents matching a filter")]
    Update {
        #[arg(help = "Collection name; falls back to default_collection if set in config")]
        collection: Option<String>,
        #[arg(help = "Filter JSON")]
        filter: String,
        #[arg(help = "Update JSON (e.g., {\"$set\": {\"age\": 42}})")]
        update: String
    },
    #[command(about = "Delete all documents matching a filter")]
    Delete {
        #[arg(help = "Collection name; falls back to default_collection if set in config")]
        collection: Option<String>,
        #[arg(help = "Filter JSON")]
        filter: String
    },
    #[command(name = "update-document", about = "Update a single document matching a filter")]
    UpdateOne {
        #[arg(help = "Collection name; falls back to default_collection if set in config")]
        collection: Option<String>,
        #[arg(help = "Filter JSON")]
        filter: String,
        #[arg(help = "Update JSON")]
        update: String
    },
    #[command(name = "delete-document", about = "Delete a single document matching a filter")]
    DeleteOne {
        #[arg(help = "Collection name; falls back to default_collection if set in config")]
        collection: Option<String>,
        #[arg(help = "Filter JSON")]
        filter: String
    },
    #[command(name = "create-document", about = "Create a document; default persistent unless --ephemeral is set")]
    CreateDocument {
        #[arg(help = "Collection name for persistent docs; ignored when --ephemeral (uses _tempDocuments)")]
        collection: Option<String>,
        #[arg(help = "Document JSON to insert (e.g., {\"name\":\"a\"})")] 
        json: String,
        #[arg(long, help = "Create as ephemeral (stored in _tempDocuments)" )]
        ephemeral: bool,
        #[arg(long, help = "TTL seconds for ephemeral docs (optional)")] 
        ttl: Option<u64>,
        #[arg(long, help = "Read JSON from STDIN instead of the json argument")]
        stdin: bool,
    },
    #[command(name = "list-ephemeral", about = "List ephemeral documents from _tempDocuments as NDJSON")]
    ListEphemeral,
    #[command(name = "purge-ephemeral", about = "Purge ephemeral documents; default only expired; use --all to purge all")]
    PurgeEphemeral {
        #[arg(long, help = "Purge all ephemeral documents, not just expired ones")]
        all: bool,
    },
    // Info/Doctor
    #[command(about = "Print database statistics (collections, cache metrics)")]
    Info,
    #[command(about = "Run basic health checks (file access, permissions)")]
    Doctor,
    #[command(name = "shell", about = "Start an interactive NexusLite shell (REPL)")]
    Shell,
    // Crypto
    #[command(name = "crypto-keygen", about = "Generate a P-256 ECDSA keypair (PEM)")]
    CryptoKeygenP256 {
        #[arg(long, help = "Write private key PEM to this path; stdout if omitted")]
        out_priv: Option<PathBuf>,
        #[arg(long, help = "Write public key PEM to this path; stdout if omitted")]
        out_pub: Option<PathBuf>,
    },
    #[command(name = "crypto-sign", about = "Sign a file using ECDSA P-256 (DER signature)")]
    CryptoSignFile {
        #[arg(help = "Path to private key PEM")]
        key_priv: PathBuf,
        #[arg(help = "Input file to sign")]
        input: PathBuf,
        #[arg(long, help = "Write signature bytes to this file; prints hex to stdout if omitted")]
        out_sig: Option<PathBuf>,
    },
    #[command(name = "crypto-verify", about = "Verify a file signature (ECDSA P-256 DER)")]
    CryptoVerifyFile {
        #[arg(help = "Path to public key PEM")]
        key_pub: PathBuf,
        #[arg(help = "Input file to verify")]
        input: PathBuf,
        #[arg(help = "Path to signature bytes (DER)")]
        sig: PathBuf,
    },
    #[command(name = "crypto-encrypt", about = "Encrypt a file using ECDH(P-256)+AES-256-GCM")]
    CryptoEncryptFile {
        #[arg(help = "Recipient public key PEM")]
        key_pub: PathBuf,
        #[arg(help = "Input file")]
        input: PathBuf,
        #[arg(help = "Output encrypted file")]
        output: PathBuf,
    },
    #[command(name = "crypto-decrypt", about = "Decrypt a file using ECDH(P-256)+AES-256-GCM")]
    CryptoDecryptFile {
        #[arg(help = "Recipient private key PEM")]
        key_priv: PathBuf,
        #[arg(help = "Input encrypted file")]
        input: PathBuf,
        #[arg(help = "Output decrypted file")]
        output: PathBuf,
    },
    #[command(name = "checkpoint-encrypted", about = "Create an encrypted DB snapshot using recipient public key")]
    CheckpointEncrypted {
        #[arg(help = "Path to DB .db file")]
        db_path: PathBuf,
        #[arg(help = "Recipient public key PEM path")]
        key_pub: PathBuf,
        #[arg(help = "Output encrypted snapshot file")]
        output: PathBuf,
    },
    #[command(name = "restore-encrypted", about = "Restore DB from an encrypted snapshot using recipient private key")]
    RestoreEncrypted {
        #[arg(help = "Path to DB .db file to write")]
        db_path: PathBuf,
        #[arg(help = "Recipient private key PEM path")]
        key_priv: PathBuf,
        #[arg(help = "Input encrypted snapshot file")]
        input: PathBuf,
    },
    #[command(name = "encrypt-db", about = "Encrypt an existing DB (.db and .wasp) with username/password (env NEXUSLITE_PASSWORD)")]
    EncryptDbPbe {
        #[arg(help = "Path to DB .db file")]
        db_path: PathBuf,
        #[arg(help = "Username to bind to the encryption header")]
        username: String,
    },
    #[command(name = "decrypt-db", about = "Decrypt a PBE-encrypted DB (.db and .wasp) with username/password (env NEXUSLITE_PASSWORD)")]
    DecryptDbPbe {
        #[arg(help = "Path to DB .db file")]
        db_path: PathBuf,
        #[arg(help = "Username used during encryption")]
        username: String,
    },
}

fn ensure_engine(db_override: &Option<PathBuf>, cfg: &AppConfig) -> Result<Engine, Box<dyn std::error::Error>> {
    // Use WASP-backed engine by default when a db was specified; else fallback to WAL-in-temp for quick usage
    if let Some(db) = db_override.as_ref().or_else(|| cfg.db_path.as_ref()) {
        let wasp = db.with_extension("wasp");
        Engine::with_wasp(wasp)
    } else {
        let tmp = std::env::temp_dir().join("nexuslite_cli_wal.log");
        Engine::new(tmp)
    }
}

fn main() {
    let cli = Cli::parse();
    let cfg = load_config(cli.config.clone());
    let engine = match ensure_engine(&cli.db, &cfg) {
        Ok(e) => e,
        Err(e) => { eprintln!("error: {}", e); std::process::exit(1); }
    };
    let def_col = cfg.default_collection.clone();

    let r = match cli.command {
        Commands::NewDb { path } => prog_cli::run(&engine, prog_cli::Command::DbCreate { db_path: path }),
        Commands::OpenDb { path, verify_sig, pubkey, sig_warn } => {
            fn read_line_stdin() -> String { let mut s = String::new(); let _ = std::io::stdin().read_line(&mut s); if s.ends_with('\n') { s.pop(); if s.ends_with('\r') { s.pop(); } } s }
            // If DB/WASP are PBE-encrypted, ensure we have credentials; prompt if interactive terminal.
            let wasp_path = path.with_extension("wasp");
            let pbe_db = nexus_lite::crypto::pbe_is_encrypted(&path);
            let pbe_wasp = wasp_path.exists() && nexus_lite::crypto::pbe_is_encrypted(&wasp_path);
            let mut res: Result<(), Box<dyn std::error::Error>> = Ok(());
            if pbe_db || pbe_wasp {
                let mut username = std::env::var("NEXUSLITE_USERNAME").unwrap_or_default();
                let mut password = std::env::var("NEXUSLITE_PASSWORD").unwrap_or_default();
                if (username.is_empty() || password.is_empty()) && std::io::stdin().is_terminal() {
                    if username.is_empty() { eprint!("Username: "); let _ = std::io::stderr().flush(); username = read_line_stdin(); }
                    if password.is_empty() { password = rpassword::prompt_password("Password: ").unwrap_or_default(); }
                }
                if username.is_empty() || password.is_empty() {
                    res = Err("PBE-encrypted DB: set NEXUSLITE_USERNAME and NEXUSLITE_PASSWORD or run interactively".into());
                }
                if res.is_ok() {
                    if let Err(e) = nexus_lite::api::decrypt_db_with_password(&path.as_path(), &username, &password) {
                        res = Err(e.into());
                    }
                }
            }
            if res.is_ok() {
                res = prog_cli::run(&engine, prog_cli::Command::DbOpen { db_path: path.clone() });
            }
            if res.is_ok() && verify_sig {
                if let Some(pk) = pubkey {
                    if let Ok(pub_pem) = std::fs::read_to_string(&pk) {
                        let wasp = path.with_extension("wasp");
                        let db_sig = path.with_extension("db.sig");
                        let wasp_sig = wasp.with_extension("wasp.sig");
                        let mut failed = false;
                        if db_sig.exists() {
                            if let Ok(sig) = std::fs::read(&db_sig) {
                                if !nexus_lite::api::crypto_verify_file(&pub_pem, &path, &sig).unwrap_or(false) {
                                    eprintln!("SIGNATURE VERIFICATION FAILED: {}", path.display());
                                    failed = true;
                                }
                            }
                        }
                        if wasp.exists() && wasp_sig.exists() {
                            if let Ok(sig) = std::fs::read(&wasp_sig) {
                                if !nexus_lite::api::crypto_verify_file(&pub_pem, &wasp, &sig).unwrap_or(false) {
                                    eprintln!("SIGNATURE VERIFICATION FAILED: {}", wasp.display());
                                    failed = true;
                                }
                            }
                        }
                        // Determine policy: CLI flag overrides config; default is fail
                        let warn = sig_warn || matches!(cfg.sig_policy.as_deref(), Some("warn"));
                        if failed && !warn { res = Err("signature verification failed".into()); }
                    }
                }
            }
            res
        },
        Commands::CloseDb { path } => prog_cli::run(&engine, prog_cli::Command::DbClose { db_path: path }),
        Commands::ColCreate { name } => prog_cli::run(&engine, prog_cli::Command::ColCreate { name }),
        Commands::ColDelete { name } => prog_cli::run(&engine, prog_cli::Command::ColDelete { name }),
        Commands::ColList => prog_cli::run(&engine, prog_cli::Command::ColList),
        Commands::ColRename { old, new } => prog_cli::run(&engine, prog_cli::Command::ColRename { old, new }),
        Commands::Import { collection, file, format } => {
            let c = collection.or_else(|| def_col.clone()).unwrap_or_else(|| "default".into());
            prog_cli::run(&engine, prog_cli::Command::Import { collection: c, file, format })
        }
        Commands::Export { collection, file, format, redact } => {
            let c = collection.or_else(|| def_col.clone()).unwrap_or_else(|| "default".into());
            if let Some(fields) = redact { prog_cli::run(&engine, prog_cli::Command::ExportR { collection: c, file, format, redact_fields: Some(fields) }) } else { prog_cli::run(&engine, prog_cli::Command::Export { collection: c, file, format }) }
        }
        Commands::Find { collection, filter, project, sort, limit, skip, redact } => {
            let c = collection.or_else(|| def_col.clone()).unwrap_or_else(|| "default".into());
            if let Some(fields) = redact { prog_cli::run(&engine, prog_cli::Command::QueryFindR { collection: c, filter_json: filter, project, sort, limit, skip, redact_fields: Some(fields) }) } else { prog_cli::run(&engine, prog_cli::Command::QueryFind { collection: c, filter_json: filter, project, sort, limit, skip }) }
        }
        Commands::Count { collection, filter } => {
            let c = collection.or_else(|| def_col.clone()).unwrap_or_else(|| "default".into());
            prog_cli::run(&engine, prog_cli::Command::QueryCount { collection: c, filter_json: filter })
        }
        Commands::Update { collection, filter, update } => {
            let c = collection.or_else(|| def_col.clone()).unwrap_or_else(|| "default".into());
            prog_cli::run(&engine, prog_cli::Command::QueryUpdate { collection: c, filter_json: filter, update_json: update })
        }
        Commands::Delete { collection, filter } => {
            let c = collection.or_else(|| def_col.clone()).unwrap_or_else(|| "default".into());
            prog_cli::run(&engine, prog_cli::Command::QueryDelete { collection: c, filter_json: filter })
        }
        Commands::UpdateOne { collection, filter, update } => {
            let c = collection.or(def_col.clone()).unwrap_or_else(|| "default".into());
            prog_cli::run(&engine, prog_cli::Command::QueryUpdateOne { collection: c, filter_json: filter, update_json: update })
        }
        Commands::DeleteOne { collection, filter } => {
            let c = collection.or(def_col.clone()).unwrap_or_else(|| "default".into());
            prog_cli::run(&engine, prog_cli::Command::QueryDeleteOne { collection: c, filter_json: filter })
        }
        Commands::CreateDocument { collection, json, ephemeral, ttl, stdin } => {
            // Support reading JSON from STDIN if requested
            let payload_res: Result<String, Box<dyn std::error::Error>> = if stdin {
                use std::io::Read;
                let mut buf = String::new();
                let mut handle = std::io::stdin();
                match handle.read_to_string(&mut buf) {
                    Ok(_) => Ok(buf),
                    Err(e) => Err(Box::new(e)),
                }
            } else { Ok(json) };
            match payload_res {
                Ok(payload) => prog_cli::run(&engine, prog_cli::Command::CreateDocument { collection, json: payload, ephemeral, ttl_secs: ttl }),
                Err(e) => Err(e),
            }
        }
        Commands::ListEphemeral => prog_cli::run(&engine, prog_cli::Command::ListEphemeral),
        Commands::PurgeEphemeral { all } => prog_cli::run(&engine, prog_cli::Command::PurgeEphemeral { all }),
    Commands::CryptoKeygenP256 { out_priv, out_pub } => prog_cli::run(&engine, prog_cli::Command::CryptoKeygenP256 { out_priv, out_pub }),
    Commands::CryptoSignFile { key_priv, input, out_sig } => prog_cli::run(&engine, prog_cli::Command::CryptoSignFile { key_priv, input, out_sig }),
    Commands::CryptoVerifyFile { key_pub, input, sig } => prog_cli::run(&engine, prog_cli::Command::CryptoVerifyFile { key_pub, input, sig }),
    Commands::CryptoEncryptFile { key_pub, input, output } => prog_cli::run(&engine, prog_cli::Command::CryptoEncryptFile { key_pub, input, output }),
    Commands::CryptoDecryptFile { key_priv, input, output } => prog_cli::run(&engine, prog_cli::Command::CryptoDecryptFile { key_priv, input, output }),
    Commands::CheckpointEncrypted { db_path, key_pub, output } => prog_cli::run(&engine, prog_cli::Command::CheckpointEncrypted { db_path, key_pub, output }),
    Commands::RestoreEncrypted { db_path, key_priv, input } => prog_cli::run(&engine, prog_cli::Command::RestoreEncrypted { db_path, key_priv, input }),
    Commands::EncryptDbPbe { db_path, username } => prog_cli::run(&engine, prog_cli::Command::EncryptDbPbe { db_path, username }),
    Commands::DecryptDbPbe { db_path, username } => prog_cli::run(&engine, prog_cli::Command::DecryptDbPbe { db_path, username }),
        Commands::Info => {
            // Print basic stats; for now, collection names and cache metrics
            let names = engine.list_collection_names();
            eprintln!("collections: {}", names.len());
            let mut ephemeral_total = 0usize;
            let mut persistent_total = 0usize;
            for n in names {
                if let Some(c) = engine.get_collection(&n) {
                    let docs = c.get_all_documents();
                    let mut eph = 0usize; let mut per = 0usize;
                    for d in &docs { match d.metadata.document_type { DocumentType::Ephemeral => eph += 1, DocumentType::Persistent => per += 1 } }
                    if n == "_tempDocuments" { ephemeral_total += eph + per; } else { persistent_total += per + eph; }
                    let m = c.cache_metrics();
                    println!("{}: docs={}, ephemeral={}, persistent={}, hits={}, misses={}", n, docs.len(), eph, per, m.hits, m.misses);
                }
            }
            eprintln!("totals: ephemeral={}, persistent={}", ephemeral_total, persistent_total);
            Ok(())
        }
        Commands::Doctor => {
            // Basic fs checks; try to open db path if provided
            if let Some(db) = cli.db.clone().or_else(|| cfg.db_path.clone()) {
                let wasp = db.with_extension("wasp");
                let ok = std::fs::OpenOptions::new().read(true).write(true).create(true).open(&wasp).is_ok();
                println!("wasp_access:{} path:{}", ok, wasp.display());
            } else {
                println!("no_db_specified");
            }
            // Config hygiene: scan config files for secret-like keys and prefer env vars
            let paths = find_config_paths(&cli.config);
            println!("config_scanned:{}", paths.len());
            for p in paths {
                if p.exists() {
                    match std::fs::read_to_string(&p) {
                        Ok(s) => {
                            match s.parse::<toml::Value>() {
                                Ok(val) => {
                                    let secrets = scan_toml_for_secret_keys(&val);
                                    if secrets.is_empty() {
                                        println!("config_file:{} status:ok", p.display());
                                    } else {
                                        println!("config_file:{} status:warning secret_keys:{} (values REDACTED)", p.display(), secrets.join(","));
                                    }
                                }
                                Err(_) => {
                                    println!("config_file:{} status:parse_error", p.display());
                                }
                            }
                        }
                        Err(_) => println!("config_file:{} status:unreadable", p.display()),
                    }
                }
            }
            // Env var hygiene: list secret-like keys but redact values
            let mut env_secret_keys: Vec<String> = Vec::new();
            for (k, _v) in std::env::vars() {
                if is_secret_key(&k) { env_secret_keys.push(k); }
            }
            if !env_secret_keys.is_empty() {
                env_secret_keys.sort();
                println!("env_secrets: {} entries (values REDACTED)", env_secret_keys.len());
                for k in env_secret_keys { println!("env:{}=REDACTED", k); }
            }
            println!("advice: prefer environment variables for secrets; avoid storing secrets in config files");
            Ok(())
        }
        Commands::Shell => {
            use std::io::{self, Write};
            let mut stdout = io::stdout();
            let stdin = io::stdin();
            let mut line = String::new();
            println!("Type 'help' for commands. 'exit' to quit.");
            loop {
                line.clear();
                write!(stdout, "> ").ok(); stdout.flush().ok();
                if stdin.read_line(&mut line).is_err() { break; }
                let trimmed = line.trim();
                if trimmed.is_empty() { continue; }
                if trimmed.eq_ignore_ascii_case("exit") || trimmed.eq_ignore_ascii_case("quit") { break; }
                // Commands: help, info, config, list-collections, list-ephemeral, purge-ephemeral [all],
                // find <col> <filter>, count <col> <filter>, update <col> <filter> <update>, delete <col> <filter>,
                // create-document <col> <json> [ephemeral] [ttl=SECS]
                if trimmed.eq_ignore_ascii_case("help") {
                    println!("Commands:\n  info\n  config\n  list-collections\n  list-ephemeral\n  purge-ephemeral [all]\n  find <collection> <filter-json>\n  count <collection> <filter-json>\n  update <collection> <filter-json> <update-json>\n  delete <collection> <filter-json>\n  create-document <collection> <json> [ephemeral] [ttl=SECS]\n  exit");
                    continue;
                }
                if trimmed.eq_ignore_ascii_case("info") {
                    let report = nexus_lite::api::info(&engine);
                    println!("{}", serde_json::to_string_pretty(&report).unwrap_or_else(|_| "{}".into()));
                    continue;
                }
                if trimmed.eq_ignore_ascii_case("config") {
                    let as_json = serde_json::to_string_pretty(&cfg).unwrap_or_else(|_| "{}".into());
                    // Redact common secret-like substrings just in case
                    let redacted = as_json
                        .replace("password", "***REDACTED***")
                        .replace("secret", "***REDACTED***")
                        .replace("token", "***REDACTED***")
                        .replace("api_key", "***REDACTED***")
                        .replace("apikey", "***REDACTED***");
                    println!("{}", redacted);
                    continue;
                }
                if trimmed.eq_ignore_ascii_case("list-collections") {
                    for n in engine.list_collection_names() { println!("{}", n); }
                    continue;
                }
                if trimmed.eq_ignore_ascii_case("list-ephemeral") {
                    prog_cli::run(&engine, prog_cli::Command::ListEphemeral).ok();
                    continue;
                }
                if trimmed.eq_ignore_ascii_case("purge-ephemeral") || trimmed.eq_ignore_ascii_case("purge-ephemeral all") {
                    let all = trimmed.ends_with(" all");
                    prog_cli::run(&engine, prog_cli::Command::PurgeEphemeral { all }).ok();
                    continue;
                }
                if let Some(rest) = trimmed.strip_prefix("find ") {
                    // format: find <collection> <json>
                    let mut parts = rest.splitn(2, ' ');
                    if let (Some(col), Some(fjson)) = (parts.next(), parts.next()) {
                        prog_cli::run(&engine, prog_cli::Command::QueryFind { collection: col.to_string(), filter_json: fjson.to_string(), project: None, sort: None, limit: None, skip: None }).ok();
                        continue;
                    }
                }
                if let Some(rest) = trimmed.strip_prefix("count ") {
                    if let Some((col, fjson)) = rest.split_once(' ') {
                        prog_cli::run(&engine, prog_cli::Command::QueryCount { collection: col.to_string(), filter_json: fjson.to_string() }).ok();
                        continue;
                    }
                }
                if let Some(rest) = trimmed.strip_prefix("delete ") {
                    if let Some((col, fjson)) = rest.split_once(' ') {
                        prog_cli::run(&engine, prog_cli::Command::QueryDelete { collection: col.to_string(), filter_json: fjson.to_string() }).ok();
                        continue;
                    }
                }
                if let Some(rest) = trimmed.strip_prefix("update ") {
                    // format: update <collection> <filter-json> <update-json>
                    let mut parts = rest.splitn(3, ' ');
                    if let (Some(col), Some(fjson), Some(uj)) = (parts.next(), parts.next(), parts.next()) {
                        prog_cli::run(&engine, prog_cli::Command::QueryUpdate { collection: col.to_string(), filter_json: fjson.to_string(), update_json: uj.to_string() }).ok();
                        continue;
                    }
                }
                if let Some(rest) = trimmed.strip_prefix("create-document ") {
                    // format: create-document <collection> <json> [ephemeral] [ttl=SECS]
                    let mut parts = rest.split_whitespace();
                    if let (Some(col), Some(json_start)) = (parts.next(), parts.next()) {
                        // json may contain spaces; reconstruct from the original rest string
                        // Find the position of the json in rest and take until next option or end
                        let mut json = json_start.to_string();
                        // crude: if json doesn't end with '}', keep appending tokens until it does
                        for t in parts.clone() { if json.ends_with('}') { break; } json.push(' '); json.push_str(t); }
                        let mut ephemeral = false; let mut ttl = None::<u64>;
                        for t in parts {
                            if t.eq_ignore_ascii_case("ephemeral") { ephemeral = true; }
                            if let Some(v) = t.strip_prefix("ttl=") { if let Ok(secs) = v.parse::<u64>() { ttl = Some(secs); } }
                        }
                        prog_cli::run(&engine, prog_cli::Command::CreateDocument { collection: Some(col.to_string()), json, ephemeral, ttl_secs: ttl }).ok();
                        continue;
                    }
                }
                println!("unrecognized: {}", trimmed);
            }
            Ok(())
        }
    };
    if let Err(e) = r { eprintln!("error: {}", e); std::process::exit(1); }
}
