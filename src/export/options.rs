use crate::query::Filter;

#[derive(Debug, Clone, Copy)]
pub enum ExportFormat {
    Ndjson,
    Csv,
    Bson,
}

#[derive(Debug, Clone)]
pub struct CsvOptions {
    pub delimiter: u8,
    pub write_headers: bool,
}
impl Default for CsvOptions {
    fn default() -> Self { Self { delimiter: b',', write_headers: true } }
}

#[derive(Debug, Clone)]
pub struct ExportOptions {
    pub format: ExportFormat,
    pub csv: CsvOptions,
    pub temp_suffix: String,
    pub redact_fields: Option<Vec<String>>, // optional list of top-level fields to mask
    pub filter: Option<Filter>,
    pub limit: Option<usize>,
}
impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            format: ExportFormat::Ndjson,
            csv: CsvOptions::default(),
            temp_suffix: ".tmp".to_string(),
            redact_fields: None,
            filter: None,
            limit: None,
        }
    }
}

#[derive(Debug, Default)]
pub struct ExportReport {
    pub written: u64,
}
