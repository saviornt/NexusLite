use std::path::PathBuf;

#[derive(Debug, Clone, Copy)]
pub enum ImportFormat {
    Auto,
    Ndjson,
    Csv,
    Bson,
}

#[derive(Debug, Clone)]
pub struct CsvOptions {
    pub delimiter: u8,
    pub has_headers: bool,
    pub type_infer: bool,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self { delimiter: b',', has_headers: true, type_infer: false }
    }
}

#[derive(Debug, Clone, Default)]
pub struct JsonOptions {
    pub array_mode: bool,
}

#[derive(Debug, Clone)]
pub struct ImportOptions {
    pub format: ImportFormat,
    pub collection: String,
    pub batch_size: usize,
    pub persistent: bool,
    pub ttl_field: Option<String>,
    pub skip_errors: bool,
    pub csv: CsvOptions,
    pub json: JsonOptions,
    pub error_sidecar: Option<PathBuf>,
    pub progress_every: Option<usize>,
}

impl Default for ImportOptions {
    fn default() -> Self {
        Self {
            format: ImportFormat::Auto,
            collection: "default".to_string(),
            batch_size: 1000,
            persistent: true,
            ttl_field: None,
            skip_errors: true,
            csv: CsvOptions::default(),
            json: JsonOptions::default(),
            error_sidecar: None,
            progress_every: Some(1000),
        }
    }
}

#[derive(Debug, Default)]
pub struct ImportReport {
    pub inserted: u64,
    pub skipped: u64,
}
