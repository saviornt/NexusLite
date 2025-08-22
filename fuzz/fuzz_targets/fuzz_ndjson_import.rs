#![no_main]
use libfuzzer_sys::fuzz_target;
use std::io::Cursor;

fuzz_target!(|data: &[u8]| {
    if data.len() > 16384 { return; }
    // Interpret as UTF-8 lines; ignore invalid UTF-8
    let s = match std::str::from_utf8(data) { Ok(x) => x, Err(_) => return };
    let engine = match nexus_lite::engine::Engine::new(std::env::temp_dir().join("fuzz_ndjson_wal.log")) { Ok(e) => e, Err(_) => return };
    let mut opts = nexus_lite::import::ImportOptions::default();
    opts.collection = "fuzz".into();
    let cur = Cursor::new(s.as_bytes());
    let _ = nexus_lite::import::import_from_reader(&engine, cur, nexus_lite::import::ImportFormat::Ndjson, &opts);
});
