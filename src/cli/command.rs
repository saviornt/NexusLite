use std::path::PathBuf;

pub enum Command {
    // Database & Collections management
    DbCreate {
        db_path: Option<PathBuf>,
    },
    DbOpen {
        db_path: PathBuf,
    },
    DbClose {
        db_path: PathBuf,
    },
    ColCreate {
        name: String,
    },
    ColDelete {
        name: String,
    },
    ColList,
    ColRename {
        old: String,
        new: String,
    },
    Import {
        collection: String,
        file: PathBuf,
        format: Option<String>,
    },
    Export {
        collection: String,
        file: PathBuf,
        format: Option<String>,
        // optional knobs
        redact_fields: Option<Vec<String>>,
        filter_json: Option<String>,
        limit: Option<usize>,
    },
    // Query subcommands (programmatic)
    QueryFind {
        collection: String,
        filter_json: String,
        project: Option<String>,
        sort: Option<String>,
        limit: Option<usize>,
        skip: Option<usize>,
    },
    QueryFindR {
        collection: String,
        filter_json: String,
        project: Option<String>,
        sort: Option<String>,
        limit: Option<usize>,
        skip: Option<usize>,
        redact_fields: Option<Vec<String>>,
    },
    QueryCount {
        collection: String,
        filter_json: String,
    },
    QueryUpdate {
        collection: String,
        filter_json: String,
        update_json: String,
    },
    QueryDelete {
        collection: String,
        filter_json: String,
    },
    QueryUpdateOne {
        collection: String,
        filter_json: String,
        update_json: String,
    },
    QueryDeleteOne {
        collection: String,
        filter_json: String,
    },
    // Document creation
    CreateDocument {
        collection: Option<String>,
        json: String,
        ephemeral: bool,
        ttl_secs: Option<u64>,
    },
    // Ephemeral admin
    ListEphemeral,
    PurgeEphemeral {
        all: bool,
    },
    // Crypto ops
    CryptoKeygenP256 {
        out_priv: Option<PathBuf>,
        out_pub: Option<PathBuf>,
    },
    CryptoSignFile {
        key_priv: PathBuf,
        input: PathBuf,
        out_sig: Option<PathBuf>,
    },
    CryptoVerifyFile {
        key_pub: PathBuf,
        input: PathBuf,
        sig: PathBuf,
    },
    CryptoEncryptFile {
        key_pub: PathBuf,
        input: PathBuf,
        output: PathBuf,
    },
    CryptoDecryptFile {
        key_priv: PathBuf,
        input: PathBuf,
        output: PathBuf,
    },
    // Telemetry/observability configuration
    TelemetrySetSlow {
        ms: u64,
    },
    TelemetrySetAudit {
        enabled: bool,
    },
    TelemetrySetQueryLog {
        path: PathBuf,
        #[allow(dead_code)]
        slow_ms: Option<u64>,
        #[allow(dead_code)]
        structured: Option<bool>,
    },
    TelemetrySetMaxGlobal {
        limit: usize,
    },
    TelemetrySetMaxFor {
        collection: String,
        limit: usize,
    },
    TelemetryRateLimit {
        collection: String,
        capacity: u64,
        refill_per_sec: u64,
    },
    TelemetryRateRemove {
        collection: String,
    },
    TelemetryRateDefault {
        capacity: u64,
        refill_per_sec: u64,
    },
    // Encrypted checkpoint/restore
    CheckpointEncrypted {
        db_path: PathBuf,
        key_pub: PathBuf,
        output: PathBuf,
    },
    RestoreEncrypted {
        db_path: PathBuf,
        key_priv: PathBuf,
        input: PathBuf,
    },
    // PBE encryption toggles
    EncryptDbPbe {
        db_path: PathBuf,
        username: String,
    },
    DecryptDbPbe {
        db_path: PathBuf,
        username: String,
    },
    /// Verify .db.sig and .wasp.sig using a public key PEM; prints results.
    VerifyDbSigs {
        db_path: PathBuf,
        key_pub_pem: String,
    },
    // Feature Flags
    FeatureList,
    FeatureEnable {
        name: String,
    },
    FeatureDisable {
        name: String,
    },
    // Feature checks/print
    FeaturesPrint,
    FeaturesCheck,
}
