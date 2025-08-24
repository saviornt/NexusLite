use crate::collection::Collection;
use crate::document::{Document, DocumentType};
use bson::Document as BsonDocument;
use std::io::{self, BufRead, BufReader, Read, Write};

use super::options::ImportOptions;
use super::options::ImportReport;
use crate::import::util::{apply_ttl, escape_json};

pub fn import_ndjson<R: Read>(
    collection: &std::sync::Arc<Collection>,
    reader: R,
    doc_type: DocumentType,
    opts: &ImportOptions,
    report: &mut ImportReport,
) -> io::Result<()> {
    if opts.json.array_mode {
        // Read entire content and parse as JSON array (sufficient for moderate inputs and tests)
        let mut s = String::new();
        let mut br = BufReader::new(reader);
        br.read_to_string(&mut s)?;
        let val: serde_json::Value =
            serde_json::from_str(&s).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let arr = val
            .as_array()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "expected JSON array"))?;
        for v in arr {
            let bdoc: BsonDocument =
                bson::to_document(v).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
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
        Some(p) if opts.skip_errors => Some(std::fs::File::create(p)?),
        _ => None,
    };
    let mut buf = String::with_capacity(8 * 1024);
    loop {
        buf.clear();
        let n = reader.read_line(&mut buf)?;
        if n == 0 {
            break;
        }
        line_no += 1;
        let line = buf.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<serde_json::Value>(line) {
            Ok(v) => {
                let bdoc: BsonDocument = bson::to_document(&v)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                let mut d = Document::new(bdoc.clone(), doc_type);
                apply_ttl(&mut d, &bdoc, opts.ttl_field.as_deref());
                collection.insert_document(d);
                report.inserted += 1;
                if let Some(n) = opts.progress_every
                    && line_no % n == 0
                {
                    log::info!("imported {} records (ndjson)", report.inserted);
                }
            }
            Err(e) => {
                if let Some(f) = sidecar.as_mut() {
                    let _ = writeln!(
                        f,
                        "{{\"line\":{},\"error\":\"{}\",\"record\":{}}}",
                        line_no,
                        escape_json(&e.to_string()),
                        serde_json::Value::String(line.to_string())
                    );
                }
                if opts.skip_errors {
                    report.skipped += 1;
                } else {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, e));
                }
            }
        }
    }
    Ok(())
}
