use crate::document::{Document, DocumentType};
use crate::engine::Engine;
use bson::Document as BsonDocument;
use std::convert::TryFrom;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};

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

/// Import data from a file path into the target collection.
///
/// # Errors
/// Returns I/O errors on read failures, parse errors (wrapped as `InvalidData`),
/// and writer errors for sidecar files when enabled.
pub fn import_file<P: AsRef<Path>>(engine: &Engine, path: P, opts: &ImportOptions) -> io::Result<ImportReport> {
    log::info!("import: path={}, collection={}", path.as_ref().display(), opts.collection);
    let file = File::open(&path)?;
    let mut reader = BufReader::new(file);
    let format = match opts.format {
        ImportFormat::Auto => detect_format(&mut reader, path.as_ref())?,
        other => other,
    };
    import_from_reader(engine, reader, format, opts)
}

fn detect_format<R: BufRead>(reader: &mut R, path: &Path) -> io::Result<ImportFormat> {
    if let Some(ext) = path.extension().and_then(|s| s.to_str()) {
        match ext.to_lowercase().as_str() {
            "jsonl" | "ndjson" | "json" => return Ok(ImportFormat::Ndjson),
            "csv" => return Ok(ImportFormat::Csv),
            "bson" => return Ok(ImportFormat::Bson),
            _ => {}
        }
    }
    let buf = reader.fill_buf()?; // peek without consuming
    let n = std::cmp::min(buf.len(), 8);
    let head = &buf[..n];
    // Heuristic: BSON starts with plausible little-endian size
    if head.len() >= 4 {
        let size = i32::from_le_bytes([head[0], head[1], head[2], head[3]]);
        if size > 0 && size < 10_000_000 { // arbitrary sane bound
            return Ok(ImportFormat::Bson);
        }
    }
    // JSON or CSV: look for braces or commas in the first chunk
    let s = String::from_utf8_lossy(&buf[..std::cmp::min(256, buf.len())]);
    if s.trim_start().starts_with('{') || s.trim_start().starts_with('[') {
        return Ok(ImportFormat::Ndjson);
    }
    Ok(ImportFormat::Csv)
}

/// Import data from an arbitrary reader.
///
/// # Errors
/// Returns I/O errors on read failures and parse errors (`InvalidData`).
pub fn import_from_reader<R: Read>(engine: &Engine, reader: R, format: ImportFormat, opts: &ImportOptions) -> io::Result<ImportReport> {
    let collection = engine
        .get_collection(&opts.collection)
        .unwrap_or_else(|| engine.create_collection(opts.collection.clone()));
    let mut report = ImportReport::default();
    let doc_type = if opts.persistent { DocumentType::Persistent } else { DocumentType::Ephemeral };
    match format {
        ImportFormat::Ndjson => import_ndjson(&collection, reader, doc_type, opts, &mut report)?,
        ImportFormat::Csv => import_csv(&collection, reader, doc_type, opts, &mut report)?,
        ImportFormat::Bson => import_bson(&collection, reader, doc_type, opts, &mut report)?,
        ImportFormat::Auto => unreachable!(),
    }
    Ok(report)
}

fn import_ndjson<R: Read>(collection: &std::sync::Arc<crate::collection::Collection>, reader: R, doc_type: DocumentType, opts: &ImportOptions, report: &mut ImportReport) -> io::Result<()> {
    if opts.json.array_mode {
        // Read entire content and parse as JSON array (sufficient for moderate inputs and tests)
        let mut s = String::new();
        let mut br = BufReader::new(reader);
        br.read_to_string(&mut s)?;
        let val: serde_json::Value = serde_json::from_str(&s).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let arr = val.as_array().ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "expected JSON array"))?;
        for v in arr {
            let bdoc: BsonDocument = bson::to_document(v).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let mut d = Document::new(bdoc.clone(), doc_type);
            apply_ttl(&mut d, &bdoc, opts.ttl_field.as_deref());
            collection.insert_document(d);
            report.inserted += 1;
        }
        return Ok(());
    }
    let mut reader = BufReader::new(reader);
    let mut line_no: usize = 0;
    let mut sidecar = match &opts.error_sidecar {
        Some(p) if opts.skip_errors => Some(File::create(p)?),
        _ => None,
    };
    let mut buf = String::with_capacity(8 * 1024);
    loop {
        buf.clear();
        let n = reader.read_line(&mut buf)?;
        if n == 0 { break; }
        line_no += 1;
        let line = buf.trim();
        if line.is_empty() { continue; }
        match serde_json::from_str::<serde_json::Value>(line) {
            Ok(v) => {
                let bdoc: BsonDocument = bson::to_document(&v).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                let mut d = Document::new(bdoc.clone(), doc_type);
                apply_ttl(&mut d, &bdoc, opts.ttl_field.as_deref());
                collection.insert_document(d);
                report.inserted += 1;
                if let Some(n) = opts.progress_every && line_no % n == 0 { log::info!("imported {} records (ndjson)", report.inserted); }
            }
            Err(e) => {
                if let Some(f) = sidecar.as_mut() {
                    let _ = writeln!(f, "{{\"line\":{},\"error\":\"{}\",\"record\":{}}}", line_no, escape_json(&e.to_string()), serde_json::Value::String(line.to_string()));
                }
                if opts.skip_errors { report.skipped += 1; } else { return Err(io::Error::new(io::ErrorKind::InvalidData, e)); }
            }
        }
    }
    Ok(())
}

fn import_csv<R: Read>(collection: &std::sync::Arc<crate::collection::Collection>, reader: R, doc_type: DocumentType, opts: &ImportOptions, report: &mut ImportReport) -> io::Result<()> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(opts.csv.has_headers)
        .delimiter(opts.csv.delimiter)
        .from_reader(reader);
    let headers: Vec<String> = if opts.csv.has_headers {
        rdr.headers().map(|h| h.iter().map(std::string::ToString::to_string).collect()).unwrap_or_default()
    } else { vec![] };
    let mut row_no: usize = 0;
    let mut sidecar = match &opts.error_sidecar {
        Some(p) if opts.skip_errors => Some(File::create(p)?),
        _ => None,
    };
    for rec in rdr.records() {
        row_no += 1;
        let rec = match rec { Ok(r) => r, Err(e) => {
            if let Some(f) = sidecar.as_mut() { let _ = writeln!(f, "{{\"row\":{},\"error\":\"{}\"}}", row_no, escape_json(&e.to_string())); }
            if opts.skip_errors { report.skipped += 1; continue; }
            return Err(io::Error::new(io::ErrorKind::InvalidData, e));
        } };
    let mut map = BsonDocument::new();
        if opts.csv.has_headers && !headers.is_empty() {
            for (i, field) in rec.iter().enumerate() {
                let key = headers.get(i).cloned().unwrap_or_else(|| format!("field_{i}"));
                map.insert(key, field_to_bson(field, opts.csv.type_infer));
            }
        } else {
            for (i, field) in rec.iter().enumerate() {
                map.insert(format!("field_{i}"), field_to_bson(field, opts.csv.type_infer));
            }
        }
    let mut d = Document::new(map.clone(), doc_type);
    apply_ttl(&mut d, &map, opts.ttl_field.as_deref());
    collection.insert_document(d);
    report.inserted += 1;
    if let Some(n) = opts.progress_every && row_no % n == 0 {
        log::info!("imported {} records (csv)", report.inserted);
    }
    }
    Ok(())
}

fn field_to_bson(field: &str, infer: bool) -> bson::Bson {
    if !infer { return bson::Bson::String(field.to_string()); }
    if let Ok(i) = field.parse::<i64>() { return bson::Bson::Int64(i); }
    if let Ok(f) = field.parse::<f64>() { return bson::Bson::Double(f); }
    match field.to_lowercase().as_str() {
        "true" => bson::Bson::Boolean(true),
        "false" => bson::Bson::Boolean(false),
        _ => bson::Bson::String(field.to_string()),
    }
}

fn import_bson<R: Read>(collection: &std::sync::Arc<crate::collection::Collection>, mut reader: R, doc_type: DocumentType, _opts: &ImportOptions, report: &mut ImportReport) -> io::Result<()> {
    let mut full = Vec::with_capacity(4096);
    loop {
        let mut len_buf = [0u8; 4];
        if reader.read_exact(&mut len_buf).is_err() { break; }
    let len = i32::from_le_bytes(len_buf);
    if len <= 0 || len > 16_000_000 { return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid bson size")); }
    let len = usize::try_from(len).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid bson size"))?;
        if full.capacity() < len { full.reserve(len - full.capacity()); }
        full.clear();
        // Extend to full length and fill
        full.extend_from_slice(&len_buf);
        let start = full.len();
        full.resize(len, 0);
        reader.read_exact(&mut full[start..])?;
        match bson::Document::from_reader(&mut &full[..]) {
            Ok(doc) => {
                let mut d = Document::new(doc.clone(), doc_type);
                apply_ttl(&mut d, &doc, None);
                collection.insert_document(d);
                report.inserted += 1;
            }
            Err(_) => report.skipped += 1,
        }
    }
    Ok(())
}

fn apply_ttl(doc: &mut Document, map: &BsonDocument, ttl_field: Option<&str>) {
    if !matches!(doc.metadata.document_type, DocumentType::Ephemeral) { return; }
    let Some(key) = ttl_field else { return; };
    if let Some(val) = map.get(key)
        && let Some(secs) = match val {
            bson::Bson::Int32(i) => Some(i64::from(*i)),
            bson::Bson::Int64(i) => Some(*i),
            #[allow(clippy::cast_possible_truncation)]
            bson::Bson::Double(f) => Some(*f as i64),
            bson::Bson::String(s) => s.parse::<i64>().ok(),
            _ => None,
        } {
        #[allow(clippy::cast_sign_loss)]
        doc.set_ttl(std::time::Duration::from_secs(secs as u64));
    }
}

fn escape_json(s: &str) -> String { s.replace('"', "\\\"") }
