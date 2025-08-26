#![no_main]
use libfuzzer_sys::fuzz_target;
use std::io::Cursor;

fuzz_target!(|data: &[u8]| {
    if data.len() > 16384 { return; }
    let engine = match nexuslite::engine::Engine::new(std::env::temp_dir().join("fuzz_csv.wasp")) { Ok(e) => e, Err(_) => return };
    let mut opts = nexuslite::import::ImportOptions::default();
    opts.collection = "fuzz".into();
    opts.csv.has_headers = true;
    opts.csv.type_infer = true;
    let cur = Cursor::new(data);
    let _ = nexuslite::import::import_from_reader(&engine, cur, nexuslite::import::ImportFormat::Csv, &opts);
});
