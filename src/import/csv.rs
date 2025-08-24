use crate::collection::Collection;
use crate::document::{Document, DocumentType};
use bson::Document as BsonDocument;
use std::io::{self, Read, Write};

use super::options::ImportOptions;
use super::options::ImportReport;
use crate::import::util::{apply_ttl, escape_json, field_to_bson};

pub fn import_csv<R: Read>(
    collection: &std::sync::Arc<Collection>,
    reader: R,
    doc_type: DocumentType,
    opts: &ImportOptions,
    report: &mut ImportReport,
) -> io::Result<()> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(opts.csv.has_headers)
        .delimiter(opts.csv.delimiter)
        .from_reader(reader);
    let headers: Vec<String> = if opts.csv.has_headers {
        rdr.headers()
            .map(|h| h.iter().map(std::string::ToString::to_string).collect())
            .unwrap_or_default()
    } else {
        vec![]
    };
    let mut row_no: usize = 0;
    let mut sidecar = match &opts.error_sidecar {
        Some(p) if opts.skip_errors => Some(std::fs::File::create(p)?),
        _ => None,
    };
    for rec in rdr.records() {
        row_no += 1;
        let rec = match rec {
            Ok(r) => r,
            Err(e) => {
                if let Some(f) = sidecar.as_mut() {
                    let _ = writeln!(
                        f,
                        "{{\"row\":{},\"error\":\"{}\"}}",
                        row_no,
                        escape_json(&e.to_string())
                    );
                }
                if opts.skip_errors {
                    report.skipped += 1;
                    continue;
                }
                return Err(io::Error::new(io::ErrorKind::InvalidData, e));
            }
        };
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
        if let Some(n) = opts.progress_every
            && row_no % n == 0
        {
            log::info!("imported {} records (csv)", report.inserted);
        }
    }
    Ok(())
}
