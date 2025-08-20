use crate::engine::Engine;
use crate::export::{export_file, ExportFormat, ExportOptions};
use crate::import::{import_file, ImportFormat, ImportOptions};
use std::path::PathBuf;

pub enum Command {
    Import { collection: String, file: PathBuf, format: Option<String> },
    Export { collection: String, file: PathBuf, format: Option<String> },
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
    }
}
