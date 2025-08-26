#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines, clippy::cognitive_complexity)]

use clap::{Parser, Subcommand};
use nexuslite::{cli as prog_cli, engine::Engine};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::io::{IsTerminal, Write};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct AppConfig {
    db_path: Option<PathBuf>,
    log_config: Option<PathBuf>,
    default_collection: Option<String>,
    // Signature verification policy: "warn" or "fail"
    sig_policy: Option<String>,
}

fn load_config(cli_cfg: Option<&PathBuf>) -> AppConfig {
    // Precedence: CLI > env > config files > defaults
    // 1) Start with defaults
    let mut cfg = AppConfig::default();
    // 2) Load from config files (~/.config/nexuslite.toml, ./.nexusliterc, custom path)
    let mut paths: Vec<PathBuf> = vec![];
    if let Some(p) = cli_cfg {
        paths.push(p.clone());
    }
    if let Ok(p) = std::env::var("NEXUSLITE_CONFIG") {
        paths.push(PathBuf::from(p));
    }
    if let Ok(home) = std::env::var("USERPROFILE").or_else(|_| std::env::var("HOME")) {
        let home_pb = PathBuf::from(home);
        paths.push(home_pb.join(".nexusliterc"));
        paths.push(home_pb.join(".config").join("nexuslite.toml"));
    }
    if let Ok(cur) = std::env::current_dir() {
        paths.push(cur.join("nexuslite.toml"));
    }
    for p in paths {
        if p.exists()
            && let Ok(s) = std::fs::read_to_string(&p)
            && let Ok(file_cfg) = toml::from_str::<AppConfig>(&s)
        {
            if cfg.db_path.is_none() {
                cfg.db_path = file_cfg.db_path;
            }
            if cfg.log_config.is_none() {
                cfg.log_config = file_cfg.log_config;
            }
            if cfg.default_collection.is_none() {
                cfg.default_collection = file_cfg.default_collection;
            }
            if cfg.sig_policy.is_none() {
                cfg.sig_policy = file_cfg.sig_policy;
            }
        }
    }
    // 3) Environment variables
    if cfg.db_path.is_none()
        && let Ok(s) = std::env::var("NEXUSLITE_DB")
    {
        cfg.db_path = Some(PathBuf::from(s));
    }
    if cfg.log_config.is_none()
        && let Ok(s) = std::env::var("NEXUSLITE_LOG_CONFIG")
    {
        cfg.log_config = Some(PathBuf::from(s));
    }
    if cfg.default_collection.is_none()
        && let Ok(s) = std::env::var("NEXUSLITE_DEFAULT_COLLECTION")
    {
        cfg.default_collection = Some(s);
    }
    if cfg.sig_policy.is_none()
        && let Ok(s) = std::env::var("NEXUSLITE_SIG_POLICY")
    {
        cfg.sig_policy = Some(s);
    }
    cfg
}

fn find_config_paths(cli_cfg: Option<&PathBuf>) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = vec![];
    if let Some(p) = cli_cfg {
        paths.push(p.clone());
    }
    if let Ok(p) = std::env::var("NEXUSLITE_CONFIG") {
        paths.push(PathBuf::from(p));
    }
    if let Ok(home) = std::env::var("USERPROFILE").or_else(|_| std::env::var("HOME")) {
        let home_pb = PathBuf::from(home);
        paths.push(home_pb.join(".nexusliterc"));
        paths.push(home_pb.join(".config").join("nexuslite.toml"));
    }
    if let Ok(cur) = std::env::current_dir() {
        paths.push(cur.join("nexuslite.toml"));
    }
    paths
}

fn is_secret_key(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    k.contains("password")
        || k.contains("passwd")
        || k.contains("secret")
        || k.contains("token")
        || k.contains("apikey")
        || k.contains("api_key")
        || k.contains("private_key")
}

fn scan_toml_for_secret_keys(val: &toml::Value) -> Vec<String> {
    let mut secrets = Vec::new();
    let mut q = VecDeque::new();
    q.push_back((String::new(), val));
    while let Some((prefix, v)) = q.pop_front() {
        match v {
            toml::Value::Table(map) => {
                for (k, vv) in map {
                    let full = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
                    if is_secret_key(k) {
                        secrets.push(full.clone());
                    }
                    q.push_back((full, vv));
                }
            }
            toml::Value::Array(arr) => {
                for (i, vv) in arr.iter().enumerate() {
                    q.push_back((format!("{prefix}[{i}]"), vv));
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
    #[arg(
        long,
        help = "Override database path (e.g., mydb.db). Takes precedence over config/env."
    )]
    db: Option<PathBuf>,
    /// Output as JSON for status-like commands
    #[arg(long, help = "Output as JSON for status-like commands", conflicts_with = "plain")]
    json: bool,
    /// Output as plain key/value or minimal values for status-like commands
    #[arg(long, help = "Output as plain text for status-like commands", conflicts_with = "json")]
    plain: bool,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum FeatureCommands {
    #[command(name = "list", about = "List feature flags and their status")]
    List,
    #[command(name = "enable", about = "Enable a runtime feature flag")]
    Enable { name: String },
    #[command(name = "disable", about = "Disable a runtime feature flag")]
    Disable { name: String },
    #[command(name = "info", about = "Show information about a feature flag")]
    Info { name: String },
}

#[derive(Subcommand, Debug)]
enum RecoveryCommands {
    #[command(
        name = "auto-recover",
        about = "Get or set automatic recovery; omit --enabled to get current value"
    )]
    AutoRecover {
        #[arg(long, value_name = "true|false", help = "Set auto-recover to true or false")]
        enabled: Option<bool>,
    },
}

#[derive(Subcommand, Debug)]
enum DbCommands {
    #[command(name = "new", about = "Create a new database (creates .db and .wasp if missing)")]
    New {
        #[arg(help = "Database file path (e.g., mydb.db). If omitted, defaults are used.")]
        path: Option<PathBuf>,
    },
    #[command(name = "open", about = "Open an existing database and initialize the engine")]
    Open {
        #[arg(help = "Database file path to open (e.g., mydb.db)")]
        path: PathBuf,
        #[arg(long, help = "Verify .db.sig and .wasp.sig with the provided public key")]
        verify_sig: bool,
        #[arg(
            long,
            requires = "verify_sig",
            help = "Path to ECDSA P-256 public key PEM for signature verification"
        )]
        pubkey: Option<PathBuf>,
        #[arg(
            long,
            requires = "verify_sig",
            help = "Treat signature failures as warnings instead of errors"
        )]
        sig_warn: bool,
    },
    #[command(name = "close", about = "Close/unregister a previously opened database handle")]
    Close {
        #[arg(help = "Database file path to close (e.g., mydb.db)")]
        path: PathBuf,
    },
}

#[derive(Subcommand, Debug)]
enum CollectionCommands {
    #[command(name = "create", about = "Create a collection in the current database")]
    Create {
        #[arg(help = "Collection name to create")]
        name: String,
    },
    #[command(name = "delete", about = "Delete a collection from the current database")]
    Delete {
        #[arg(help = "Collection name to delete")]
        name: String,
    },
    #[command(name = "list", about = "List all collections in the current database")]
    List,
    #[command(name = "rename", about = "Rename a collection in the current database")]
    Rename {
        #[arg(help = "Existing collection name")]
        old: String,
        #[arg(help = "New collection name")]
        new: String,
    },
}

#[derive(Subcommand, Debug)]
enum QueryCommands {
    #[command(name = "find", about = "Find documents matching a filter")]
    Find {
        #[arg(help = "Target collection name; falls back to default_collection if set in config")]
        collection: Option<String>,
        #[arg(long, help = "Filter expression in JSON string form", default_value = "{}")]
        filter: String,
        #[arg(long, help = "Projection JSON, e.g. {\"_id\":0}")]
        project: Option<String>,
        #[arg(long, help = "Sort JSON, e.g. {\"x\":1}")]
        sort: Option<String>,
        #[arg(long, help = "Max results")]
        limit: Option<usize>,
        #[arg(long, help = "Skip first N results")]
        skip: Option<usize>,
        #[arg(long, help = "Fields to redact from output (repeatable)")]
        redact: Option<Vec<String>>,
    },
    #[command(name = "count", about = "Count documents matching a filter")]
    Count {
        #[arg(help = "Target collection name; falls back to default_collection if set in config")]
        collection: Option<String>,
        #[arg(long, help = "Filter expression in JSON string form", default_value = "{}")]
        filter: String,
    },
    #[command(
        name = "update",
        about = "Update documents matching a filter with an update specification"
    )]
    Update {
        #[arg(help = "Target collection name; falls back to default_collection if set in config")]
        collection: Option<String>,
        #[arg(long, help = "Filter expression in JSON string form")]
        filter: String,
        #[arg(long, help = "Update expression in JSON string form")]
        update: String,
    },
    #[command(name = "delete", about = "Delete documents matching a filter")]
    Delete {
        #[arg(help = "Target collection name; falls back to default_collection if set in config")]
        collection: Option<String>,
        #[arg(long, help = "Filter expression in JSON string form")]
        filter: String,
    },
    #[command(name = "update-one", about = "Update a single document matching a filter")]
    UpdateOne {
        #[arg(help = "Target collection name; falls back to default_collection if set in config")]
        collection: Option<String>,
        #[arg(long, help = "Filter expression in JSON string form")]
        filter: String,
        #[arg(long, help = "Update expression in JSON string form")]
        update: String,
    },
    #[command(name = "delete-one", about = "Delete a single document matching a filter")]
    DeleteOne {
        #[arg(help = "Target collection name; falls back to default_collection if set in config")]
        collection: Option<String>,
        #[arg(long, help = "Filter expression in JSON string form")]
        filter: String,
    },
}

#[derive(Subcommand, Debug)]
enum Commands {
    // Grouped subcommands (best-practice nouns/verbs)
    #[command(name = "feature", about = "Manage runtime features (list/info/enable/disable)")]
    Feature {
        #[command(subcommand)]
        cmd: FeatureCommands,
    },
    #[command(name = "recovery", about = "Recovery controls (auto-recover)")]
    Recovery {
        #[command(subcommand)]
        cmd: RecoveryCommands,
    },
    #[command(name = "db", about = "Database management (new/open/close)")]
    Db {
        #[command(subcommand)]
        cmd: DbCommands,
    },
    #[command(name = "collection", about = "Collection management (create/delete/list/rename)")]
    Collection {
        #[command(subcommand)]
        cmd: CollectionCommands,
    },
    #[command(name = "query", about = "Query operations (find/count/update/delete)")]
    Query {
        #[command(subcommand)]
        cmd: QueryCommands,
    },
    // Legacy single-level commands that do not yet have grouped equivalents remain below
    // Import/Export
    #[command(about = "Import data into a collection from a file (NDJSON/CSV/BSON)")]
    Import {
        #[arg(help = "Target collection name; falls back to default_collection if set in config")]
        collection: Option<String>,
        #[arg(help = "Path to input file")]
        file: PathBuf,
        #[arg(help = "Format override: ndjson|csv|bson; defaults to auto-detect")]
        format: Option<String>,
    },
    #[command(about = "Export a collection to a file (NDJSON/CSV/BSON)")]
    Export {
        #[arg(
            help = "Collection name to export; falls back to default_collection if set in config"
        )]
        collection: Option<String>,
        #[arg(help = "Path to output file")]
        file: PathBuf,
        #[arg(help = "Format: ndjson|csv|bson; defaults to ndjson")]
        format: Option<String>,
        #[arg(long, help = "Filter JSON to select documents (optional)")]
        filter: Option<String>,
        #[arg(long, help = "Limit number of documents exported (optional)")]
        limit: Option<usize>,
        #[arg(
            long,
            value_delimiter = ',',
            help = "Comma-separated list of top-level fields to redact/mask in outputs (NDJSON/CSV)"
        )]
        redact: Option<Vec<String>>,
    },
    #[command(
        name = "create-document",
        about = "Create a document; default persistent unless --ephemeral is set"
    )]
    CreateDocument {
        #[arg(
            help = "Collection name for persistent docs; ignored when --ephemeral (uses _tempDocuments)"
        )]
        collection: Option<String>,
        #[arg(help = "Document JSON to insert (e.g., {\"name\":\"a\"})")]
        json: String,
        #[arg(long, help = "Create as ephemeral (stored in _tempDocuments)")]
        ephemeral: bool,
        #[arg(long, help = "TTL seconds for ephemeral docs (optional)")]
        ttl: Option<u64>,
        #[arg(long, help = "Read JSON from STDIN instead of the json argument")]
        stdin: bool,
    },
    #[command(
        name = "list-ephemeral",
        about = "List ephemeral documents from _tempDocuments as NDJSON"
    )]
    ListEphemeral,
    #[command(
        name = "purge-ephemeral",
        about = "Purge ephemeral documents; default only expired; use --all to purge all"
    )]
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
    #[command(
        name = "checkpoint-encrypted",
        about = "Create an encrypted DB snapshot using recipient public key"
    )]
    CheckpointEncrypted {
        #[arg(help = "Path to DB .db file")]
        db_path: PathBuf,
        #[arg(help = "Recipient public key PEM path")]
        key_pub: PathBuf,
        #[arg(help = "Output encrypted snapshot file")]
        output: PathBuf,
    },
    #[command(
        name = "restore-encrypted",
        about = "Restore DB from an encrypted snapshot using recipient private key"
    )]
    RestoreEncrypted {
        #[arg(help = "Path to DB .db file to write")]
        db_path: PathBuf,
        #[arg(help = "Recipient private key PEM path")]
        key_priv: PathBuf,
        #[arg(help = "Input encrypted snapshot file")]
        input: PathBuf,
    },
    #[command(
        name = "encrypt-db",
        about = "Encrypt an existing DB (.db and .wasp) with username/password (env NEXUSLITE_PASSWORD)"
    )]
    EncryptDbPbe {
        #[arg(help = "Path to DB .db file")]
        db_path: PathBuf,
        #[arg(help = "Username to bind to the encryption header")]
        username: String,
    },
    #[command(
        name = "decrypt-db",
        about = "Decrypt a PBE-encrypted DB (.db and .wasp) with username/password (env NEXUSLITE_PASSWORD)"
    )]
    DecryptDbPbe {
        #[arg(help = "Path to DB .db file")]
        db_path: PathBuf,
        #[arg(help = "Username used during encryption")]
        username: String,
    },
    // Telemetry/observability
    #[command(name = "telemetry-set-slow", about = "Set slow query threshold in milliseconds")]
    TelemetrySetSlow {
        #[arg(help = "Milliseconds threshold for slow query logging")]
        ms: u64,
    },
    #[command(name = "telemetry-set-audit", about = "Enable or disable audit logging")]
    TelemetrySetAudit {
        #[arg(help = "true/false to enable or disable audit logging")]
        enabled: bool,
    },
    #[command(name = "telemetry-set-query-log", about = "Configure query log path and options")]
    TelemetrySetQueryLog {
        #[arg(help = "Path to write query logs (JSON lines when structured)")]
        path: PathBuf,
        #[arg(long, help = "Optional slow query threshold in ms (overrides globally)")]
        slow_ms: Option<u64>,
        #[arg(long, help = "Structured JSON output true/false (default true)")]
        structured: Option<bool>,
    },
    #[command(name = "telemetry-set-max-global", about = "Set global max result size for queries")]
    TelemetrySetMaxGlobal {
        #[arg(help = "Maximum number of results for queries (global cap)")]
        limit: usize,
    },
    #[command(
        name = "telemetry-set-max-for",
        about = "Set max result size for a specific collection"
    )]
    TelemetrySetMaxFor {
        #[arg(help = "Collection name")]
        collection: String,
        #[arg(help = "Maximum number of results for this collection")]
        limit: usize,
    },
    #[command(
        name = "telemetry-rate-limit",
        about = "Configure a per-collection token bucket rate limit"
    )]
    TelemetryRateLimit {
        #[arg(help = "Collection name")]
        collection: String,
        #[arg(help = "Bucket capacity (max tokens)")]
        capacity: u64,
        #[arg(help = "Refill tokens per second")]
        refill_per_sec: u64,
    },
    #[command(name = "telemetry-rate-remove", about = "Remove a per-collection rate limit")]
    TelemetryRateRemove {
        #[arg(help = "Collection name")]
        collection: String,
    },
    #[command(name = "telemetry-rate-default", about = "Set default per-collection rate limit")]
    TelemetryRateDefault {
        #[arg(help = "Bucket capacity (max tokens)")]
        capacity: u64,
        #[arg(help = "Refill tokens per second")]
        refill_per_sec: u64,
    },
    #[command(name = "version", about = "Show version and compiled/runtime features")]
    Version,
    #[command(name = "check", about = "Run quick configuration checks")]
    Check,
    #[command(
        name = "log-config",
        about = "Configure logging directory/level/retention (applies immediately)"
    )]
    LogConfig {
        #[arg(long, help = "Base directory for logs (default: .)")]
        dir: Option<PathBuf>,
        #[arg(long, help = "Global log level: error|warn|info|debug|trace (default: info)")]
        level: Option<String>,
        #[arg(long, help = "Number of rolled files to keep for each logger (default: 7)")]
        retention: Option<usize>,
    },
}

fn ensure_engine(
    db_override: Option<&PathBuf>,
    cfg: &AppConfig,
) -> Result<Engine, Box<dyn std::error::Error>> {
    // Always use WASP-backed engine; if no DB provided, use a temp WASP file.
    let wasp_path = db_override
        .or(cfg.db_path.as_ref())
        .map(|db| db.with_extension("wasp"))
        .unwrap_or_else(|| std::env::temp_dir().join("nexuslite_cli.wasp"));
    Engine::with_wasp(wasp_path)
}

fn main() {
    let cli = Cli::parse();
    // Initialize runtime feature flags from environment.
    nexuslite::api::init_from_env();
    let cfg = load_config(cli.config.as_ref());
    let engine = match ensure_engine(cli.db.as_ref(), &cfg) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    };
    let def_col = cfg.default_collection.clone();
    let mode = if cli.json {
        prog_cli::OutputMode::Json
    } else if cli.plain {
        prog_cli::OutputMode::Plain
    } else {
        prog_cli::OutputMode::Human
    };

    let r = match cli.command {
        Commands::Feature { cmd } => match cmd {
            FeatureCommands::List => {
                prog_cli::run_with_format(&engine, prog_cli::Command::FeatureList, mode)
            }
            FeatureCommands::Enable { name } => {
                prog_cli::run_with_format(&engine, prog_cli::Command::FeatureEnable { name }, mode)
            }
            FeatureCommands::Disable { name } => {
                prog_cli::run_with_format(&engine, prog_cli::Command::FeatureDisable { name }, mode)
            }
            FeatureCommands::Info { name } => {
                prog_cli::run_with_format(&engine, prog_cli::Command::FeatureInfo { name }, mode)
            }
        },
        Commands::Recovery { cmd } => match cmd {
            RecoveryCommands::AutoRecover { enabled } => match enabled {
                Some(v) => prog_cli::run_with_format(
                    &engine,
                    prog_cli::Command::RecoveryAutoRecover { enabled: v },
                    mode,
                ),
                None => prog_cli::run_with_format(
                    &engine,
                    prog_cli::Command::RecoveryAutoRecoverGet,
                    mode,
                ),
            },
        },
        Commands::Db { cmd } => match cmd {
            DbCommands::New { path } => prog_cli::run_with_format(
                &engine,
                prog_cli::Command::DbCreate { db_path: path },
                mode,
            ),
            DbCommands::Open { path, verify_sig, pubkey, sig_warn } => {
                fn read_line_stdin() -> String {
                    let mut s = String::new();
                    let _ = std::io::stdin().read_line(&mut s);
                    if s.ends_with('\n') {
                        s.pop();
                        if s.ends_with('\r') {
                            s.pop();
                        }
                    }
                    s
                }
                // If DB/WASP are PBE-encrypted, ensure we have credentials; prompt if interactive terminal.
                let wasp_path = path.with_extension("wasp");
                let pbe_db = nexuslite::crypto::pbe_is_encrypted(&path);
                let pbe_wasp =
                    wasp_path.exists() && nexuslite::crypto::pbe_is_encrypted(&wasp_path);
                let mut res: Result<(), Box<dyn std::error::Error>> = Ok(());
                if pbe_db || pbe_wasp {
                    let mut username = std::env::var("NEXUSLITE_USERNAME").unwrap_or_default();
                    let mut password = std::env::var("NEXUSLITE_PASSWORD").unwrap_or_default();
                    // Allow prompting only if we are in an interactive terminal
                    if (username.is_empty() || password.is_empty())
                        && std::io::stdin().is_terminal()
                    {
                        // In test builds, include friendly defaults in prompt text to aid manual runs.
                        #[cfg(test)]
                        const PROMPT_USER: &str = "Username (admin): ";
                        #[cfg(test)]
                        const PROMPT_PASS: &str = "Password (password): ";
                        #[cfg(not(test))]
                        const PROMPT_USER: &str = "Username: ";
                        #[cfg(not(test))]
                        const PROMPT_PASS: &str = "Password: ";
                        if username.is_empty() {
                            eprint!("{}", PROMPT_USER);
                            let _ = std::io::stderr().flush();
                            username = read_line_stdin();
                        }
                        if password.is_empty() {
                            password = rpassword::prompt_password(PROMPT_PASS).unwrap_or_default();
                        }
                    }
                    if username.is_empty() || password.is_empty() {
                        res = Err("PBE-encrypted DB: set NEXUSLITE_USERNAME and NEXUSLITE_PASSWORD or run interactively".into());
                    }
                    if res.is_ok()
                        && let Err(e) = nexuslite::api::decrypt_db_with_password(
                            path.as_path(),
                            &username,
                            &password,
                        )
                    {
                        res = Err(e.into());
                    }
                }
                if res.is_ok() {
                    res = prog_cli::run_with_format(
                        &engine,
                        prog_cli::Command::DbOpen { db_path: path.clone() },
                        mode,
                    );
                }
                if res.is_ok()
                    && verify_sig
                    && let Some(pk) = pubkey
                    && let Ok(pub_pem) = std::fs::read_to_string(&pk)
                {
                    let wasp = path.with_extension("wasp");
                    let db_sig = path.with_extension("db.sig");
                    let wasp_sig = wasp.with_extension("wasp.sig");
                    let mut failed = false;
                    if db_sig.exists()
                        && let Ok(sig) = std::fs::read(&db_sig)
                        && !nexuslite::api::crypto_verify_file(&pub_pem, &path, &sig)
                            .unwrap_or(false)
                    {
                        eprintln!("SIGNATURE VERIFICATION FAILED: {}", path.display());
                        failed = true;
                    }
                    if wasp.exists()
                        && wasp_sig.exists()
                        && let Ok(sig) = std::fs::read(&wasp_sig)
                        && !nexuslite::api::crypto_verify_file(&pub_pem, &wasp, &sig)
                            .unwrap_or(false)
                    {
                        eprintln!("SIGNATURE VERIFICATION FAILED: {}", wasp.display());
                        failed = true;
                    }
                    // Determine policy: CLI flag overrides config; default is fail
                    let warn = sig_warn || matches!(cfg.sig_policy.as_deref(), Some("warn"));
                    if failed && !warn {
                        res = Err("signature verification failed".into());
                    }
                }
                res
            }
            DbCommands::Close { path } => prog_cli::run_with_format(
                &engine,
                prog_cli::Command::DbClose { db_path: path },
                mode,
            ),
        },
        Commands::Collection { cmd } => match cmd {
            CollectionCommands::Create { name } => {
                prog_cli::run(&engine, prog_cli::Command::ColCreate { name })
            }
            CollectionCommands::Delete { name } => {
                prog_cli::run(&engine, prog_cli::Command::ColDelete { name })
            }
            CollectionCommands::List => {
                prog_cli::run_with_format(&engine, prog_cli::Command::ColList, mode)
            }
            CollectionCommands::Rename { old, new } => {
                prog_cli::run(&engine, prog_cli::Command::ColRename { old, new })
            }
        },
        Commands::Query { cmd } => match cmd {
            QueryCommands::Find { collection, filter, project, sort, limit, skip, redact } => {
                let c = collection.or_else(|| def_col.clone()).unwrap_or_else(|| "default".into());
                if let Some(fields) = redact {
                    prog_cli::run(
                        &engine,
                        prog_cli::Command::QueryFindR {
                            collection: c,
                            filter_json: filter,
                            project,
                            sort,
                            limit,
                            skip,
                            redact_fields: Some(fields),
                        },
                    )
                } else {
                    prog_cli::run(
                        &engine,
                        prog_cli::Command::QueryFind {
                            collection: c,
                            filter_json: filter,
                            project,
                            sort,
                            limit,
                            skip,
                        },
                    )
                }
            }
            QueryCommands::Count { collection, filter } => {
                let c = collection.or_else(|| def_col.clone()).unwrap_or_else(|| "default".into());
                prog_cli::run_with_format(
                    &engine,
                    prog_cli::Command::QueryCount { collection: c, filter_json: filter },
                    mode,
                )
            }
            QueryCommands::Update { collection, filter, update } => {
                let c = collection.or_else(|| def_col.clone()).unwrap_or_else(|| "default".into());
                prog_cli::run_with_format(
                    &engine,
                    prog_cli::Command::QueryUpdate {
                        collection: c,
                        filter_json: filter,
                        update_json: update,
                    },
                    mode,
                )
            }
            QueryCommands::Delete { collection, filter } => {
                let c = collection.or_else(|| def_col.clone()).unwrap_or_else(|| "default".into());
                prog_cli::run_with_format(
                    &engine,
                    prog_cli::Command::QueryDelete { collection: c, filter_json: filter },
                    mode,
                )
            }
            QueryCommands::UpdateOne { collection, filter, update } => {
                let c = collection.or_else(|| def_col.clone()).unwrap_or_else(|| "default".into());
                prog_cli::run_with_format(
                    &engine,
                    prog_cli::Command::QueryUpdateOne {
                        collection: c,
                        filter_json: filter,
                        update_json: update,
                    },
                    mode,
                )
            }
            QueryCommands::DeleteOne { collection, filter } => {
                let c = collection.or_else(|| def_col.clone()).unwrap_or_else(|| "default".into());
                prog_cli::run_with_format(
                    &engine,
                    prog_cli::Command::QueryDeleteOne { collection: c, filter_json: filter },
                    mode,
                )
            }
        },
        Commands::Info => prog_cli::run_with_format(&engine, prog_cli::Command::Info, mode),
        Commands::Version => prog_cli::run_with_format(&engine, prog_cli::Command::Version, mode),
        Commands::Check => prog_cli::run(&engine, prog_cli::Command::Check),
        Commands::LogConfig { dir, level, retention } => prog_cli::run_with_format(
            &engine,
            prog_cli::Command::LogConfig { dir, level, retention },
            mode,
        ),
        Commands::Import { collection, file, format } => {
            let c = collection.or_else(|| def_col.clone()).unwrap_or_else(|| "default".into());
            prog_cli::run(&engine, prog_cli::Command::Import { collection: c, file, format })
        }
        Commands::Export { collection, file, format, filter, limit, redact } => {
            let c = collection.or_else(|| def_col.clone()).unwrap_or_else(|| "default".into());
            prog_cli::run(
                &engine,
                prog_cli::Command::Export {
                    collection: c,
                    file,
                    format,
                    redact_fields: redact,
                    filter_json: filter,
                    limit,
                },
            )
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
            } else {
                Ok(json)
            };
            match payload_res {
                Ok(payload) => prog_cli::run(
                    &engine,
                    prog_cli::Command::CreateDocument {
                        collection,
                        json: payload,
                        ephemeral,
                        ttl_secs: ttl,
                    },
                ),
                Err(e) => Err(e),
            }
        }
        Commands::ListEphemeral => prog_cli::run(&engine, prog_cli::Command::ListEphemeral),
        Commands::PurgeEphemeral { all } => {
            prog_cli::run(&engine, prog_cli::Command::PurgeEphemeral { all })
        }
        Commands::CryptoKeygenP256 { out_priv, out_pub } => {
            prog_cli::run(&engine, prog_cli::Command::CryptoKeygenP256 { out_priv, out_pub })
        }
        Commands::CryptoSignFile { key_priv, input, out_sig } => {
            prog_cli::run(&engine, prog_cli::Command::CryptoSignFile { key_priv, input, out_sig })
        }
        Commands::CryptoVerifyFile { key_pub, input, sig } => prog_cli::run_with_format(
            &engine,
            prog_cli::Command::CryptoVerifyFile { key_pub, input, sig },
            mode,
        ),
        Commands::CryptoEncryptFile { key_pub, input, output } => {
            prog_cli::run(&engine, prog_cli::Command::CryptoEncryptFile { key_pub, input, output })
        }
        Commands::CryptoDecryptFile { key_priv, input, output } => {
            prog_cli::run(&engine, prog_cli::Command::CryptoDecryptFile { key_priv, input, output })
        }
        Commands::CheckpointEncrypted { db_path, key_pub, output } => prog_cli::run(
            &engine,
            prog_cli::Command::CheckpointEncrypted { db_path, key_pub, output },
        ),
        Commands::RestoreEncrypted { db_path, key_priv, input } => {
            prog_cli::run(&engine, prog_cli::Command::RestoreEncrypted { db_path, key_priv, input })
        }
        Commands::EncryptDbPbe { db_path, username } => {
            prog_cli::run(&engine, prog_cli::Command::EncryptDbPbe { db_path, username })
        }
        Commands::DecryptDbPbe { db_path, username } => {
            prog_cli::run(&engine, prog_cli::Command::DecryptDbPbe { db_path, username })
        }
        Commands::TelemetrySetSlow { ms } => {
            prog_cli::run_with_format(&engine, prog_cli::Command::TelemetrySetSlow { ms }, mode)
        }
        Commands::TelemetrySetAudit { enabled } => prog_cli::run_with_format(
            &engine,
            prog_cli::Command::TelemetrySetAudit { enabled },
            mode,
        ),
        Commands::TelemetrySetQueryLog { path, slow_ms, structured } => prog_cli::run_with_format(
            &engine,
            prog_cli::Command::TelemetrySetQueryLog { path, slow_ms, structured },
            mode,
        ),
        Commands::TelemetrySetMaxGlobal { limit } => prog_cli::run_with_format(
            &engine,
            prog_cli::Command::TelemetrySetMaxGlobal { limit },
            mode,
        ),
        Commands::TelemetrySetMaxFor { collection, limit } => prog_cli::run_with_format(
            &engine,
            prog_cli::Command::TelemetrySetMaxFor { collection, limit },
            mode,
        ),
        Commands::TelemetryRateLimit { collection, capacity, refill_per_sec } => {
            prog_cli::run_with_format(
                &engine,
                prog_cli::Command::TelemetryRateLimit { collection, capacity, refill_per_sec },
                mode,
            )
        }
        Commands::TelemetryRateRemove { collection } => prog_cli::run_with_format(
            &engine,
            prog_cli::Command::TelemetryRateRemove { collection },
            mode,
        ),
        Commands::TelemetryRateDefault { capacity, refill_per_sec } => prog_cli::run_with_format(
            &engine,
            prog_cli::Command::TelemetryRateDefault { capacity, refill_per_sec },
            mode,
        ),

        Commands::Doctor => {
            if !nexuslite::feature_flags::is_enabled("doctor") {
                eprintln!("doctor feature disabled");
                Ok(())
            } else {
                // Gather data
                let wasp_info = if let Some(db) = cli.db.clone().or_else(|| cfg.db_path.clone()) {
                    let wasp = db.with_extension("wasp");
                    let ok = std::fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(&wasp)
                        .is_ok();
                    Some((ok, wasp.display().to_string()))
                } else {
                    None
                };

                let mut config_files: Vec<(String, String)> = Vec::new();
                let paths = find_config_paths(cli.config.as_ref());
                for p in paths {
                    if p.exists() {
                        let status = match std::fs::read_to_string(&p) {
                            Ok(s) => match s.parse::<toml::Value>() {
                                Ok(val) => {
                                    let secrets = scan_toml_for_secret_keys(&val);
                                    if secrets.is_empty() {
                                        "ok".to_string()
                                    } else {
                                        format!("warning secret_keys:{}", secrets.join(","))
                                    }
                                }
                                Err(_) => "parse_error".to_string(),
                            },
                            Err(_) => "unreadable".to_string(),
                        };
                        config_files.push((p.display().to_string(), status));
                    }
                }

                let mut env_secret_keys: Vec<String> = Vec::new();
                for (k, _v) in std::env::vars() {
                    if is_secret_key(&k) {
                        env_secret_keys.push(k);
                    }
                }
                env_secret_keys.sort();
                let status = if env_secret_keys.is_empty() { "ok" } else { "warning" }.to_string();
                let advice = "prefer environment variables for secrets; avoid storing secrets in config files".to_string();

                prog_cli::run_with_format(
                    &engine,
                    prog_cli::Command::DoctorSummary {
                        wasp_access: wasp_info,
                        config_files,
                        env_secret_keys,
                        advice,
                        status,
                    },
                    mode,
                )
            }
        }
        Commands::Shell => {
            if !nexuslite::feature_flags::is_enabled("repl") {
                eprintln!("repl feature disabled");
                Ok(())
            } else {
                use std::io::{self, Write};
                let mut stdout = io::stdout();
                let stdin = io::stdin();
                let mut line = String::new();
                println!("Type 'help' for commands. 'exit' to quit.");
                loop {
                    line.clear();
                    let _ = write!(stdout, "> ");
                    let _ = stdout.flush();
                    if stdin.read_line(&mut line).is_err() {
                        break;
                    }
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    if trimmed.eq_ignore_ascii_case("exit") || trimmed.eq_ignore_ascii_case("quit")
                    {
                        break;
                    }
                    if trimmed.eq_ignore_ascii_case("help") {
                        println!(
                            "Commands:\n  info\n  config\n  list-collections\n  list-ephemeral\n  purge-ephemeral [all]\n  find <collection> <filter-json>\n  count <collection> <filter-json>\n  update <collection> <filter-json> <update-json>\n  delete <collection> <filter-json>\n  create-document <collection> <json> [ephemeral] [ttl=SECS]\n  exit"
                        );
                        continue;
                    }
                    if trimmed.eq_ignore_ascii_case("info") {
                        let report = nexuslite::api::info(&engine);
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&report).unwrap_or_else(|_| "{}".into())
                        );
                        continue;
                    }
                    if trimmed.eq_ignore_ascii_case("config") {
                        let as_json =
                            serde_json::to_string_pretty(&cfg).unwrap_or_else(|_| "{}".into());
                        let redacted = as_json
                            .replace("password", "***REDACTED***")
                            .replace("secret", "***REDACTED***")
                            .replace("token", "***REDACTED***")
                            .replace("api_key", "***REDACTED***")
                            .replace("apikey", "***REDACTED***");
                        println!("{redacted}");
                        continue;
                    }
                    if trimmed.eq_ignore_ascii_case("list-collections") {
                        for n in engine.list_collection_names() {
                            println!("{n}");
                        }
                        continue;
                    }
                    if trimmed.eq_ignore_ascii_case("list-ephemeral") {
                        let _ = prog_cli::run(&engine, prog_cli::Command::ListEphemeral);
                        continue;
                    }
                    if trimmed.eq_ignore_ascii_case("purge-ephemeral")
                        || trimmed.eq_ignore_ascii_case("purge-ephemeral all")
                    {
                        let all = trimmed.ends_with(" all");
                        let _ = prog_cli::run(&engine, prog_cli::Command::PurgeEphemeral { all });
                        continue;
                    }
                    if let Some(rest) = trimmed.strip_prefix("find ") {
                        let mut parts = rest.splitn(2, ' ');
                        if let (Some(col), Some(fjson)) = (parts.next(), parts.next()) {
                            let _ = prog_cli::run(
                                &engine,
                                prog_cli::Command::QueryFind {
                                    collection: col.to_string(),
                                    filter_json: fjson.to_string(),
                                    project: None,
                                    sort: None,
                                    limit: None,
                                    skip: None,
                                },
                            );
                            continue;
                        }
                    }
                    if let Some(rest) = trimmed.strip_prefix("count ")
                        && let Some((col, fjson)) = rest.split_once(' ')
                    {
                        let _ = prog_cli::run(
                            &engine,
                            prog_cli::Command::QueryCount {
                                collection: col.to_string(),
                                filter_json: fjson.to_string(),
                            },
                        );
                        continue;
                    }
                    if let Some(rest) = trimmed.strip_prefix("delete ")
                        && let Some((col, fjson)) = rest.split_once(' ')
                    {
                        let _ = prog_cli::run(
                            &engine,
                            prog_cli::Command::QueryDelete {
                                collection: col.to_string(),
                                filter_json: fjson.to_string(),
                            },
                        );
                        continue;
                    }
                    if let Some(rest) = trimmed.strip_prefix("update ") {
                        let mut parts = rest.splitn(3, ' ');
                        if let (Some(col), Some(fjson), Some(uj)) =
                            (parts.next(), parts.next(), parts.next())
                        {
                            let _ = prog_cli::run(
                                &engine,
                                prog_cli::Command::QueryUpdate {
                                    collection: col.to_string(),
                                    filter_json: fjson.to_string(),
                                    update_json: uj.to_string(),
                                },
                            );
                            continue;
                        }
                    }
                    if let Some(rest) = trimmed.strip_prefix("create-document ") {
                        let mut parts = rest.split_whitespace();
                        if let (Some(col), Some(json_start)) = (parts.next(), parts.next()) {
                            let mut json = json_start.to_string();
                            for t in parts.clone() {
                                if json.ends_with('}') {
                                    break;
                                }
                                json.push(' ');
                                json.push_str(t);
                            }
                            let mut ephemeral = false;
                            let mut ttl = None::<u64>;
                            for t in parts {
                                if t.eq_ignore_ascii_case("ephemeral") {
                                    ephemeral = true;
                                }
                                if let Some(v) = t.strip_prefix("ttl=")
                                    && let Ok(secs) = v.parse::<u64>()
                                {
                                    ttl = Some(secs);
                                }
                            }
                            let _ = prog_cli::run(
                                &engine,
                                prog_cli::Command::CreateDocument {
                                    collection: Some(col.to_string()),
                                    json,
                                    ephemeral,
                                    ttl_secs: ttl,
                                },
                            );
                            continue;
                        }
                    }
                    println!("unrecognized: {trimmed}");
                }
                Ok(())
            }
        }
    };
    if let Err(e) = r {
        // Pretty rate-limit message
        if let Some(db) = e.downcast_ref::<nexuslite::errors::DbError>() {
            match db {
                nexuslite::errors::DbError::RateLimitedWithRetry { retry_after_ms } => {
                    eprintln!("rate-limited; retry-after-ms={retry_after_ms}");
                    std::process::exit(2);
                }
                nexuslite::errors::DbError::RateLimited => {
                    eprintln!("rate-limited");
                    std::process::exit(2);
                }
                _ => {
                    eprintln!("error: {db}");
                    std::process::exit(1);
                }
            }
        } else {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}
