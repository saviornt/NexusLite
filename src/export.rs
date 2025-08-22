use crate::engine::Engine;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;
use tempfile::NamedTempFile;

#[derive(Debug, Clone, Copy)]
pub enum ExportFormat { Ndjson, Csv, Bson }

#[derive(Debug, Clone)]
pub struct CsvOptions { pub delimiter: u8, pub write_headers: bool }
impl Default for CsvOptions { fn default() -> Self { Self { delimiter: b',', write_headers: true } } }

#[derive(Debug, Clone)]
pub struct ExportOptions {
    pub format: ExportFormat,
    pub csv: CsvOptions,
    pub temp_suffix: String,
    pub redact_fields: Option<Vec<String>>, // optional list of top-level fields to mask
}
impl Default for ExportOptions {
    fn default() -> Self { Self { format: ExportFormat::Ndjson, csv: CsvOptions::default(), temp_suffix: ".tmp".to_string(), redact_fields: None } }
}

#[derive(Debug, Default)]
pub struct ExportReport { pub written: u64 }

pub fn export_file(engine: &Engine, collection: &str, path: impl AsRef<Path>, opts: &ExportOptions) -> io::Result<ExportReport> {
    log::info!("export: collection={}, path={}", collection, path.as_ref().display());
    let dest = path.as_ref();
    let parent = dest.parent().unwrap_or_else(|| Path::new("."));
    // Create a NamedTempFile in the same directory to ensure atomic replace
    let mut tmp = NamedTempFile::new_in(parent)?;
    let report = export_into_writer(engine, collection, &mut tmp, opts)?;
    // Persist atomically with Windows-friendly retries
    let mut last_err: Option<io::Error> = None;
    for attempt in 0..5 {
        if dest.exists() && let Err(e) = std::fs::remove_file(dest) {
            last_err = Some(e);
            std::thread::sleep(std::time::Duration::from_millis(10 + attempt * 5));
            continue;
        }
        match tmp.persist(dest) {
            Ok(_p) => { return Ok(report); }
            Err(pe) => {
                last_err = Some(pe.error);
                tmp = pe.file; // recover temp file and retry
                std::thread::sleep(std::time::Duration::from_millis(10 + attempt * 5));
            }
        }
    }
    Err(last_err.unwrap_or_else(|| io::Error::other("failed to persist export file")))
}

//

pub fn export_to_writer(engine: &Engine, collection: &str, path: impl AsRef<Path>, opts: &ExportOptions) -> io::Result<ExportReport> {
    let file = File::create(path)?;
    export_into_writer(engine, collection, file, opts)
}

fn export_into_writer<W: Write>(engine: &Engine, collection: &str, writer: W, opts: &ExportOptions) -> io::Result<ExportReport> {
    let Some(col) = engine.get_collection(collection) else { return Err(io::Error::new(io::ErrorKind::NotFound, "collection not found")); };
    let mut writer = BufWriter::new(writer);
    let mut report = ExportReport::default();
    let redact = opts.redact_fields.as_ref();
    match opts.format {
        ExportFormat::Ndjson => {
            log::debug!("export ndjson start");
            for id in col.list_ids() {
                if let Some(d) = col.find_document(&id) {
                let mut doc = d.data.0.clone();
                if let Some(fields) = redact { apply_redaction(&mut doc, fields); }
                let v = bson::to_bson(&doc).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                let s = serde_json::to_string(&v).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                writeln!(writer, "{}", s)?;
                report.written += 1;
                }
            }
        }
        ExportFormat::Csv => {
            let mut wtr = csv::WriterBuilder::new().delimiter(opts.csv.delimiter).from_writer(writer);
            let mut headers: Vec<String> = vec![];
            let ids = col.list_ids();
            for (i, id) in ids.iter().enumerate() {
                let Some(d) = col.find_document(id) else { continue };
                if i == 0 {
                    headers = d.data.0.keys().cloned().collect();
                    if opts.csv.write_headers { wtr.write_record(&headers)?; }
                }
                let mut row: Vec<String> = Vec::with_capacity(headers.len());
                for k in &headers {
                    if let Some(fields) = redact && fields.iter().any(|f| f == k) {
                        row.push("***REDACTED***".to_string());
                    } else {
                        row.push(d.data.0.get(k).map(bson_to_string).unwrap_or_default());
                    }
                }
                wtr.write_record(&row)?;
                report.written += 1;
            }
            writer = wtr.into_inner().map_err(|e| io::Error::other(e.to_string()))?;
        }
        ExportFormat::Bson => {
            for id in col.list_ids() {
                if let Some(d) = col.find_document(&id) {
                let mut buf = Vec::new();
                d.data.0.to_writer(&mut buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                writer.write_all(&buf)?;
                report.written += 1;
                }
            }
        }
    }
    writer.flush()?;
    Ok(report)
}

fn bson_to_string(v: &bson::Bson) -> String {
    match v {
        bson::Bson::String(s) => s.clone(),
        bson::Bson::Int32(i) => i.to_string(),
        bson::Bson::Int64(i) => i.to_string(),
        bson::Bson::Double(f) => f.to_string(),
        bson::Bson::Boolean(b) => b.to_string(),
        other => other.to_string(),
    }
}

fn apply_redaction(doc: &mut bson::Document, fields: &[String]) {
    for f in fields {
        if doc.contains_key(f) {
            doc.insert(f, bson::Bson::String("***REDACTED***".into()));
        }
    }
}
