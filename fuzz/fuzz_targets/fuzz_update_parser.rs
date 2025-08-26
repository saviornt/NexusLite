#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() > 8192 { return; }
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = nexuslite::query::parse_update_json(s);
    }
});
