#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() > 8192 { return; }
    if let Ok(s) = std::str::from_utf8(data) {
        if let Ok(filter) = nexus_lite::query::parse_filter_json(s) {
            // Build a tiny doc with a few fields to exercise eval paths
            let docs = [
                bson::doc!{"a": 1, "b": 2, "name": "x"},
                bson::doc!{"a": 10, "b": -5, "name": "y", "nested": {"z": 3}},
                bson::doc!{"active": true}
            ];
            for d in &docs {
                let _ = nexus_lite::query::eval_filter(d, &filter);
            }
        }
    }
});
