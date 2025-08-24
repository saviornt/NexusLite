use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::Path;

use crate::document::DocumentType;
use crate::engine::Engine;

use super::bson::import_bson;
use super::csv::import_csv;
use super::detect::detect_format;
use super::ndjson::import_ndjson;
use super::options::{ImportFormat, ImportOptions, ImportReport};

/// Import data from a file path into the target collection.
///
/// # Errors
/// Returns I/O errors on read failures, parse errors (wrapped as `InvalidData`),
/// and writer errors for sidecar files when enabled.
pub fn import_file<P: AsRef<Path>>(
    engine: &Engine,
    path: P,
    opts: &ImportOptions,
) -> io::Result<ImportReport> {
    log::info!("import: path={}, collection={}", path.as_ref().display(), opts.collection);
    let file = File::open(&path)?;
    let mut reader = BufReader::new(file);
    let format = match opts.format {
        ImportFormat::Auto => detect_format(&mut reader, path.as_ref())?,
        other => other,
    };
    import_from_reader(engine, reader, format, opts)
}

/// Import data from an arbitrary reader.
///
/// # Errors
/// Returns I/O errors on read failures and parse errors (`InvalidData`).
pub fn import_from_reader<R: Read>(
    engine: &Engine,
    reader: R,
    format: ImportFormat,
    opts: &ImportOptions,
) -> io::Result<ImportReport> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_format_by_extension() {
        let data = b"{\"k\":1}\n";
        let mut r = std::io::BufReader::new(&data[..]);
        assert!(matches!(
            detect_format(&mut r, Path::new("x.jsonl")).unwrap(),
            ImportFormat::Ndjson
        ));

        let mut r = std::io::BufReader::new(&data[..]);
        assert!(matches!(detect_format(&mut r, Path::new("x.csv")).unwrap(), ImportFormat::Csv));

        let mut r = std::io::BufReader::new(&data[..]);
        assert!(matches!(detect_format(&mut r, Path::new("x.bson")).unwrap(), ImportFormat::Bson));
    }

    #[test]
    fn detect_format_by_content_bson_size_header() {
        // BSON doc: size(4) + minimal doc {\0}\0
        let mut buf = Vec::new();
        // create a simple bson document via bson crate to be safe
        let doc = bson::doc! { "x": 1 };
        doc.to_writer(&mut buf).unwrap();
        let mut r = std::io::BufReader::new(&buf[..]);
        let fmt = detect_format(&mut r, Path::new("unknown.dat")).unwrap();
        assert!(matches!(fmt, ImportFormat::Bson));
    }

    #[test]
    fn detect_format_by_content_json_vs_csv() {
        let jsonish = b"{\"x\":1}\n";
        let mut r = std::io::BufReader::new(&jsonish[..]);
        assert!(matches!(
            detect_format(&mut r, Path::new("data.txt")).unwrap(),
            ImportFormat::Ndjson
        ));

        let csvish = b"a,b\n1,2\n";
        let mut r = std::io::BufReader::new(&csvish[..]);
        assert!(matches!(detect_format(&mut r, Path::new("data.txt")).unwrap(), ImportFormat::Csv));
    }
}
