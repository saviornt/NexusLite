#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() > 8192 { return; }
    if let Ok(s) = std::str::from_utf8(data) {
        // Fuzz parse_filter_json; should not panic
        let _ = nexuslite::query::parse_filter_json(s);
    }
});
