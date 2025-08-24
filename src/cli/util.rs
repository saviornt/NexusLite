use crate::export::ExportFormat;
use crate::import::ImportFormat;

pub fn parse_format_input(s: &Option<String>) -> Option<String> {
    s.as_ref().map(|x| x.to_lowercase())
}

pub fn parse_import_format(s: &Option<String>) -> ImportFormat {
    match parse_format_input(s).as_deref() {
        Some("csv") => ImportFormat::Csv,
        Some("bson") => ImportFormat::Bson,
        Some("ndjson" | "json" | "jsonl") => ImportFormat::Ndjson,
        _ => ImportFormat::Auto,
    }
}

pub fn parse_export_format(s: &Option<String>) -> ExportFormat {
    match parse_format_input(s).as_deref() {
        Some("csv") => ExportFormat::Csv,
        Some("bson") => ExportFormat::Bson,
        _ => ExportFormat::Ndjson,
    }
}
