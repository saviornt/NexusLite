#![no_main]
use libfuzzer_sys::fuzz_target;
use std::io::Cursor;

fuzz_target!(|data: &[u8]| {
    if data.len() > 16384 { return; }
    let engine = match nexus_lite::engine::Engine::new(std::env::temp_dir().join("fuzz_csv_wal.log")) { Ok(e) => e, Err(_) => return };
    let mut opts = nexus_lite::import::ImportOptions::default();
    opts.collection = "fuzz".into();
    opts.csv.has_headers = true;
    opts.csv.type_infer = true;
    let cur = Cursor::new(data);
    let _ = nexus_lite::import::import_from_reader(&engine, cur, nexus_lite::import::ImportFormat::Csv, &opts);
});
