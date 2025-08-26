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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn import_format_parsing() {
        assert!(matches!(parse_import_format(&Some("csv".into())), ImportFormat::Csv));
        assert!(matches!(parse_import_format(&Some("ndjson".into())), ImportFormat::Ndjson));
        assert!(matches!(parse_import_format(&Some("jsonl".into())), ImportFormat::Ndjson));
        assert!(matches!(parse_import_format(&None), ImportFormat::Auto));
    }

    #[test]
    fn export_format_parsing() {
        assert!(matches!(parse_export_format(&Some("csv".into())), ExportFormat::Csv));
        assert!(matches!(parse_export_format(&Some("bson".into())), ExportFormat::Bson));
        assert!(matches!(parse_export_format(&None), ExportFormat::Ndjson));
    }
}
