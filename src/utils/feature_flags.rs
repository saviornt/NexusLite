//! Runtime feature flags registry.
//!
//! Provides a simple global registry of feature switches that can be toggled at runtime
//! via API/CLI. These are independent of Cargo compile-time features.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::LazyLock;

#[derive(Clone, Debug)]
pub struct FeatureFlag {
    pub name: String,
    pub enabled: bool,
    pub description: String,
}

static FLAGS: LazyLock<RwLock<HashMap<String, FeatureFlag>>> = LazyLock::new(|| {
    let mut map = HashMap::new();
    // Register default flags here.
    map.insert(
		"crypto-pqc".to_string(),
		FeatureFlag {
			name: "crypto-pqc".to_string(),
			enabled: false,
			description: "Post-quantum cryptography (ML-KEM, SPHINCS+). Not currently available; stub for future work.".to_string(),
		},
	);
    map.insert(
        "crypto-ecc".to_string(),
        FeatureFlag {
            name: "crypto-ecc".to_string(),
            enabled: true,
            description: "Elliptic-curve cryptography (P-256 ECDH/ECDSA) is available.".to_string(),
        },
    );
    map.insert(
        "open-metrics".to_string(),
        FeatureFlag {
            name: "open-metrics".to_string(),
            enabled: true,
            description: "Expose minimal OpenMetrics-style counters via telemetry::metrics_text()."
                .to_string(),
        },
    );
    map.insert(
        "regex".to_string(),
        FeatureFlag {
            name: "regex".to_string(),
            enabled: cfg!(feature = "regex"),
            description:
                "Regular expression operators in queries (requires Cargo feature 'regex')."
                    .to_string(),
        },
    );
    map.insert(
        "cli-bin".to_string(),
        FeatureFlag {
            name: "cli-bin".to_string(),
            enabled: true,
            description: "Clap-based CLI binary available (nexuslite).".to_string(),
        },
    );
    RwLock::new(map)
});

/// Enable or disable a feature flag. Returns true if the flag existed.
pub fn set(name: &str, enabled: bool) -> bool {
    let mut g = FLAGS.write();
    if let Some(f) = g.get_mut(name) {
        f.enabled = enabled;
        true
    } else {
        false
    }
}

/// Ensure a feature exists (register if missing) with provided default and description.
pub fn ensure(name: &str, default_enabled: bool, description: &str) {
    let mut g = FLAGS.write();
    g.entry(name.to_string()).or_insert_with(|| FeatureFlag {
        name: name.to_string(),
        enabled: default_enabled,
        description: description.to_string(),
    });
}

/// Returns whether a feature is enabled (false if unknown).
pub fn is_enabled(name: &str) -> bool {
    FLAGS.read().get(name).is_some_and(|f| f.enabled)
}

/// Get a feature by name.
pub fn get(name: &str) -> Option<FeatureFlag> {
    FLAGS.read().get(name).cloned()
}

/// List all known feature flags.
pub fn list() -> Vec<FeatureFlag> {
    FLAGS.read().values().cloned().collect()
}

/// Initialize runtime feature flags from environment variables.
/// Currently supported:
/// - NEXUSLITE_USE_PQC: "1", "true", "yes" enable crypto-pqc; "0", "false", "no" disable.
pub fn init_from_env() {
    if let Ok(v) = std::env::var("NEXUSLITE_USE_PQC") {
        let val = matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes");
        let _ = set("crypto-pqc", val);
    }
}
