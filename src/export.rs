use crate::engine::Engine;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

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
    let p = path.as_ref();
    let tmp = tmp_path(p, &opts.temp_suffix);
    let report = export_to_writer(engine, collection, &tmp, opts)?;
    // Try atomic replace; fallback to replace-by-rename
    if p.exists() { let _ = fs::remove_file(p); }
    fs::rename(&tmp, p)?;
    Ok(report)
}

fn tmp_path(path: &Path, suffix: &str) -> PathBuf {
    let mut p = PathBuf::from(path);
    let file = p
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "export".to_string());
    p.set_file_name(format!("{}{}", file, suffix));
    p
}

pub fn export_to_writer(engine: &Engine, collection: &str, path: impl AsRef<Path>, opts: &ExportOptions) -> io::Result<ExportReport> {
    let Some(col) = engine.get_collection(collection) else { return Err(io::Error::new(io::ErrorKind::NotFound, "collection not found")); };
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    let mut report = ExportReport::default();
    let redact = opts.redact_fields.as_ref().map(|v| v.iter().map(|s| s.to_string()).collect::<Vec<_>>());
    match opts.format {
        ExportFormat::Ndjson => {
            log::debug!("export ndjson start");
            for d in col.get_all_documents() {
                let mut doc = d.data.0.clone();
                if let Some(fields) = &redact { apply_redaction(&mut doc, fields); }
                let v = bson::to_bson(&doc).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                let s = serde_json::to_string(&v).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                writeln!(writer, "{}", s)?;
                report.written += 1;
            }
        }
        ExportFormat::Csv => {
            let mut wtr = csv::WriterBuilder::new().delimiter(opts.csv.delimiter).from_writer(writer);
            // Build headers from union of keys of first doc
            let docs = col.get_all_documents();
            let mut headers: Vec<String> = vec![];
            if let Some(first) = docs.first() {
                headers = first.data.0.keys().cloned().collect();
                if opts.csv.write_headers { wtr.write_record(&headers)?; }
            }
            for d in docs.into_iter() {
                if headers.is_empty() { // derive
                    headers = d.data.0.keys().cloned().collect();
                    if opts.csv.write_headers { wtr.write_record(&headers)?; }
                }
                let row: Vec<String> = headers.iter().map(|k| {
                    if let Some(fields) = &redact {
                        if fields.iter().any(|f| f == k) { return "***REDACTED***".to_string(); }
                    }
                    d.data.0.get(k).map(bson_to_string).unwrap_or_default()
                }).collect();
                wtr.write_record(&row)?;
                report.written += 1;
            }
            writer = wtr.into_inner().map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        }
        ExportFormat::Bson => {
            for d in col.get_all_documents() {
                let mut buf = Vec::new();
                d.data.0.to_writer(&mut buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                writer.write_all(&buf)?;
                report.written += 1;
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
