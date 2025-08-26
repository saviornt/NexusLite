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
    use crate::engine::Engine;

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

    #[test]
    fn import_ndjson_skips_errors_when_enabled() {
        let eng = Engine::new(crate::test_support::temp_wasp("nl_import_ndjson_skip")).unwrap();
        let data = b"{\"a\":1}\n{bad}\n{\"a\":2}\n";
        let mut opts = ImportOptions::default();
        opts.collection = "u_imp".to_string();
        opts.skip_errors = true;
        let report = import_from_reader(&eng, &data[..], ImportFormat::Ndjson, &opts).unwrap();
        assert_eq!(report.inserted, 2);
        assert_eq!(report.skipped, 1);
    }

    #[test]
    fn import_ndjson_errors_when_skip_disabled() {
        let eng = Engine::new(crate::test_support::temp_wasp("nl_import_ndjson_err")).unwrap();
        let data = b"{\"a\":1}\n{bad}\n{\"a\":2}\n";
        let mut opts = ImportOptions::default();
        opts.collection = "u_imp".to_string();
        opts.skip_errors = false;
        let err = import_from_reader(&eng, &data[..], ImportFormat::Ndjson, &opts).err();
        assert!(err.is_some());
    }

    #[test]
    fn import_csv_with_headers_and_type_infer() {
        let eng = Engine::new(crate::test_support::temp_wasp("nl_import_csv_hdr_infer")).unwrap();
        let data = b"a,b\n1,2\n3,4\n";
        let mut opts = ImportOptions::default();
        opts.collection = "u_csv".to_string();
        opts.csv.has_headers = true;
        opts.csv.type_infer = true;
        let report = import_from_reader(&eng, &data[..], ImportFormat::Csv, &opts).unwrap();
        assert_eq!(report.inserted, 2);
        let col = eng.get_collection(&opts.collection).unwrap();
        let docs: Vec<_> =
            col.list_ids().into_iter().filter_map(|id| col.find_document(&id)).collect();
        assert_eq!(docs.len(), 2);
        for d in docs {
            assert!(d.data.0.get_i64("a").is_ok());
            assert!(d.data.0.get_i64("b").is_ok());
        }
    }

    #[test]
    fn import_csv_without_headers_and_no_infer() {
        let eng =
            Engine::new(crate::test_support::temp_wasp("nl_import_csv_nohdr_ninfer")).unwrap();
        let data = b"1,2\n3,4\n";
        let mut opts = ImportOptions::default();
        opts.collection = "u_csv2".to_string();
        opts.csv.has_headers = false;
        opts.csv.type_infer = false;
        let report = import_from_reader(&eng, &data[..], ImportFormat::Csv, &opts).unwrap();
        assert_eq!(report.inserted, 2);
        let col = eng.get_collection(&opts.collection).unwrap();
        let docs: Vec<_> =
            col.list_ids().into_iter().filter_map(|id| col.find_document(&id)).collect();
        assert_eq!(docs.len(), 2);
        let mut vals = vec![];
        for d in docs {
            vals.push(d.data.0.get_str("field_0").unwrap().to_string());
        }
        vals.sort();
        assert_eq!(vals, vec!["1".to_string(), "3".to_string()]);
    }

    #[test]
    fn import_csv_sidecar_on_error_when_skipping() {
        let eng = Engine::new(crate::test_support::temp_wasp("nl_import_csv_sidecar")).unwrap();
        // Malformed CSV line (unclosed quote)
        let data = b"a,b\n1,2\n\"bad,\n3,4\n";
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let sidecar_path = tmp.path().to_path_buf();
        let mut opts = ImportOptions::default();
        opts.collection = "u_csv3".to_string();
        opts.csv.has_headers = true;
        opts.skip_errors = true;
        opts.error_sidecar = Some(sidecar_path.clone());
        let report = import_from_reader(&eng, &data[..], ImportFormat::Csv, &opts).unwrap();
        assert!(report.skipped > 0);
        let side = std::fs::read_to_string(sidecar_path).unwrap();
        assert!(!side.is_empty());
    }

    #[test]
    fn import_bson_valid_and_invalid_size() {
        let eng = Engine::new(crate::test_support::temp_wasp("nl_import_bson")).unwrap();
        // Build a stream: two valid docs then an invalid size header
        let mut buf: Vec<u8> = Vec::new();
        let d1 = bson::doc! {"x": 1};
        let d2 = bson::doc! {"y": "z"};
        d1.to_writer(&mut buf).unwrap();
        d2.to_writer(&mut buf).unwrap();
        // Append an invalid size (too large)
        let mut invalid = vec![];
        invalid.extend_from_slice(&20_000_001i32.to_le_bytes());
        invalid.extend_from_slice(&[0u8; 16]);
        let data = [buf.as_slice(), invalid.as_slice()].concat();

        let mut opts = ImportOptions::default();
        opts.collection = "u_bson".to_string();
        // Valid docs should be inserted until invalid encountered (function returns Err on invalid size)
        let err = import_from_reader(&eng, &data[..], ImportFormat::Bson, &opts).err();
        assert!(err.is_some());

        // Import only valid bytes to assert insertion works
        let report_ok = import_from_reader(&eng, &buf[..], ImportFormat::Bson, &opts).unwrap();
        assert_eq!(report_ok.inserted, 2);
    }

    #[test]
    fn import_csv_delimiter_variations() {
        let eng = Engine::new(crate::test_support::temp_wasp("nl_import_csv_delims")).unwrap();

        // Comma
        let mut opts = ImportOptions::default();
        opts.collection = "u_csv_d1".to_string();
        opts.csv.has_headers = true;
        opts.csv.delimiter = b',';
        let data = b"a,b\n1,2\n";
        let r = import_from_reader(&eng, &data[..], ImportFormat::Csv, &opts).unwrap();
        assert_eq!(r.inserted, 1);

        // Semicolon
        let mut opts2 = ImportOptions::default();
        opts2.collection = "u_csv_d2".to_string();
        opts2.csv.has_headers = true;
        opts2.csv.delimiter = b';';
        let data2 = b"a;b\n3;4\n";
        let r2 = import_from_reader(&eng, &data2[..], ImportFormat::Csv, &opts2).unwrap();
        assert_eq!(r2.inserted, 1);

        // Tab
        let mut opts3 = ImportOptions::default();
        opts3.collection = "u_csv_d3".to_string();
        opts3.csv.has_headers = true;
        opts3.csv.delimiter = b'\t';
        let data3 = b"a\tb\n5\t6\n";
        let r3 = import_from_reader(&eng, &data3[..], ImportFormat::Csv, &opts3).unwrap();
        assert_eq!(r3.inserted, 1);
    }

    #[test]
    fn import_bson_boundary_sizes() {
        let eng = Engine::new(crate::test_support::temp_wasp("nl_import_bson_bounds")).unwrap();
        let mut opts = ImportOptions::default();
        opts.collection = "u_bson_bound".to_string();

        // Very small valid doc
        let d_small = bson::doc! {"a": 1};
        let mut buf_small = Vec::new();
        d_small.to_writer(&mut buf_small).unwrap();
        let r_small = import_from_reader(&eng, &buf_small[..], ImportFormat::Bson, &opts).unwrap();
        assert_eq!(r_small.inserted, 1);

        // Create a moderately sized document (not too big to blow memory)
        let mut big_map = bson::Document::new();
        for i in 0..1000 {
            big_map.insert(format!("k{i}"), bson::Bson::Int32(i));
        }
        let mut buf_mid = Vec::new();
        big_map.to_writer(&mut buf_mid).unwrap();
        let r_mid = import_from_reader(&eng, &buf_mid[..], ImportFormat::Bson, &opts).unwrap();
        assert_eq!(r_mid.inserted, 1);

        // Invalid too-large size header directly (simulate size > 16MB)
        let mut invalid = vec![];
        invalid.extend_from_slice(&20_000_001i32.to_le_bytes());
        invalid.extend_from_slice(&[0u8; 16]);
        let err = import_from_reader(&eng, &invalid[..], ImportFormat::Bson, &opts).err();
        assert!(err.is_some());
    }
}
