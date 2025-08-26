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
        "crypto".to_string(),
        FeatureFlag {
            name: "crypto".to_string(),
            enabled: true,
            description: "Cryptography enabled (ECC by default).".to_string(),
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
    map.insert(
        "db-logging".to_string(),
        FeatureFlag {
            name: "db-logging".to_string(),
            enabled: true,
            description: "Enable database and process logs (app/audit/metrics).".to_string(),
        },
    );
    map.insert(
        "telemetry-adv".to_string(),
        FeatureFlag {
            name: "telemetry-adv".to_string(),
            enabled: true,
            description: "Advanced telemetry: query logs, structured outputs, extra counters."
                .to_string(),
        },
    );
    map.insert(
        "recovery".to_string(),
        FeatureFlag {
            name: "recovery".to_string(),
            enabled: true,
            description: "Snapshot/backup helpers and recovery controls (WASP).".to_string(),
        },
    );
    map.insert(
        "doctor".to_string(),
        FeatureFlag {
            name: "doctor".to_string(),
            enabled: true,
            description: "Enable database health checks and diagnostics commands.".to_string(),
        },
    );
    map.insert(
        "repl".to_string(),
        FeatureFlag {
            name: "repl".to_string(),
            enabled: true,
            description: "Interactive REPL shell available (nexuslite shell).".to_string(),
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
pub fn init_from_env() {
    // No-op for now; reserved for future env-driven flags
}

// Hidden crypto mode selector; defaults to ECC. Not exposed as a runtime flag.
// Future option reserved (commented): PQC.
#[derive(Clone, Copy, Debug)]
pub enum CryptoMode {
    Ecc,
    // Pqc, // reserved
}

static CRYPTO_MODE: LazyLock<RwLock<CryptoMode>> = LazyLock::new(|| RwLock::new(CryptoMode::Ecc));

/// Get the current crypto mode as a stable string ("ecc").
/// Not toggleable via CLI/API; reserved for future.
pub fn crypto_mode() -> &'static str {
    match *CRYPTO_MODE.read() {
        CryptoMode::Ecc => "ecc",
        // CryptoMode::Pqc => "pqc",
    }
}

// ---- Recovery feature options ----
#[derive(Clone, Copy, Debug)]
pub struct RecoveryConfig {
    pub auto_recover: bool,
}

static RECOVERY_CFG: LazyLock<RwLock<RecoveryConfig>> =
    LazyLock::new(|| RwLock::new(RecoveryConfig { auto_recover: false }));

pub fn recovery_set_auto_recover(enabled: bool) {
    RECOVERY_CFG.write().auto_recover = enabled;
}
pub fn recovery_auto_recover() -> bool {
    RECOVERY_CFG.read().auto_recover
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_contains_known_flags() {
        let names: Vec<String> = list().into_iter().map(|f| f.name).collect();
        assert!(names.contains(&"crypto".to_string()));
        assert!(names.contains(&"recovery".to_string()));
    }

    #[test]
    fn set_and_get_flag() {
        ensure("unit-ff", false, "unit test flag");
        assert!(!is_enabled("unit-ff"));
        assert!(set("unit-ff", true));
        assert!(is_enabled("unit-ff"));
    }

    #[test]
    fn recovery_auto_recover_toggles() {
        recovery_set_auto_recover(false);
        assert!(!recovery_auto_recover());
        recovery_set_auto_recover(true);
        assert!(recovery_auto_recover());
    }
}
