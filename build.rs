use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    let out = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));
    let mut features: Vec<String> = Vec::new();
    for (k, _v) in env::vars() {
        if let Some(name) = k.strip_prefix("CARGO_FEATURE_") {
            let pretty = name.to_ascii_lowercase().replace('_', "-");
            features.push(pretty);
        }
    }
    features.sort();
    let list = features
        .iter()
        .map(|s| format!("\"{s}\""))
        .collect::<Vec<_>>()
        .join(", ");
    // Define a static slice of &str representing compiled Cargo features
    let content = format!("pub static COMPILED_FEATURES: &[&str] = &[{list}];\n");
    fs::write(out.join("compiled_features.rs"), content).expect("write compiled_features.rs");
}
