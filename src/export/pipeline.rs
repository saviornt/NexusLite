use crate::engine::Engine;
use crate::query;
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use tempfile::NamedTempFile;

use super::options::{ExportOptions, ExportReport, ExportFormat};
use super::sinks::{BsonSink, CsvSink, DocSink, NdjsonSink};

/// Export a collection to a file atomically via a temp file + persist.
///
/// # Errors
/// Returns an error if the destination cannot be created or the write/persist fails.
pub fn export_file(
    engine: &Engine,
    collection: &str,
    path: impl AsRef<Path>,
    opts: &ExportOptions,
) -> io::Result<ExportReport> {
    log::info!("export: collection={}, path={}", collection, path.as_ref().display());
    let dest = path.as_ref();
    let parent = dest.parent().unwrap_or_else(|| Path::new("."));
    // Ensure parent directory exists
    if !parent.exists() {
        std::fs::create_dir_all(parent)?;
    }
    // Create a NamedTempFile in the same directory to ensure atomic replace
    let mut tmp = NamedTempFile::new_in(parent)?;
    let report = export_into_writer(engine, collection, &mut tmp, opts)?;
    // Persist atomically with Windows-friendly retries
    let mut last_err: Option<io::Error> = None;
    for attempt in 0..5 {
        if dest.exists()
            && let Err(e) = std::fs::remove_file(dest)
        {
            last_err = Some(e);
            std::thread::sleep(std::time::Duration::from_millis(10 + attempt * 5));
            continue;
        }
        match tmp.persist(dest) {
            Ok(_p) => {
                return Ok(report);
            }
            Err(pe) => {
                last_err = Some(pe.error);
                tmp = pe.file; // recover temp file and retry
                std::thread::sleep(std::time::Duration::from_millis(10 + attempt * 5));
            }
        }
    }
    Err(last_err.unwrap_or_else(|| io::Error::other("failed to persist export file")))
}

/// Export a collection directly to a newly created file at `path`.
///
/// # Errors
/// Returns an error if the file cannot be created or writing fails.
pub fn export_to_writer(
    engine: &Engine,
    collection: &str,
    path: impl AsRef<Path>,
    opts: &ExportOptions,
) -> io::Result<ExportReport> {
    let file = File::create(path)?;
    export_into_writer(engine, collection, file, opts)
}

fn export_into_writer<W: Write>(
    engine: &Engine,
    collection: &str,
    writer: W,
    opts: &ExportOptions,
) -> io::Result<ExportReport> {
    let Some(col) = engine.get_collection(collection) else {
        return Err(io::Error::new(io::ErrorKind::NotFound, "collection not found"));
    };
    let mut report = ExportReport::default();
    let redact = opts.redact_fields.as_ref();
    // Build sink for formatting/IO
    let mut sink: Box<dyn DocSink> = match opts.format {
        ExportFormat::Ndjson => Box::new(NdjsonSink::new(writer)),
        ExportFormat::Csv => Box::new(CsvSink::new(writer, opts.csv.delimiter, opts.csv.write_headers)),
        ExportFormat::Bson => Box::new(BsonSink::new(writer)),
    };

    // Selection + transformation pipeline
    let mut remaining = opts.limit.unwrap_or(usize::MAX);
    let matches_filter = |doc: &bson::Document| -> bool {
        match &opts.filter { Some(f) => query::eval_filter(doc, f), None => true }
    };
    for id in col.list_ids() {
        if remaining == 0 { break; }
        if let Some(d) = col.find_document(&id) {
            let mut doc = d.data.0.clone();
            if !matches_filter(&doc) { continue; }
            if let Some(fields) = redact { apply_redaction(&mut doc, fields); }
            sink.write_doc(&doc)?;
            report.written += 1;
            remaining = remaining.saturating_sub(1);
        }
    }
    sink.finish()?;
    Ok(report)
}

fn apply_redaction(doc: &mut bson::Document, fields: &[String]) {
    for f in fields {
        if doc.contains_key(f) {
            doc.insert(f, bson::Bson::String("***REDACTED***".into()));
        }
    }
}
