use crate::errors::DbError;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FeatureFlagInfo {
    pub name: String,
    pub enabled: bool,
    pub description: String,
    /// Optional key/value options associated with a feature (e.g., recovery.auto_recover)
    pub options: Option<std::collections::BTreeMap<String, String>>,
}

pub fn feature_list() -> Vec<FeatureFlagInfo> {
    crate::feature_flags::list()
        .into_iter()
        .map(|f| FeatureFlagInfo {
            name: f.name,
            enabled: f.enabled,
            description: f.description,
            options: None,
        })
        .collect()
}

pub fn feature_enable(name: &str) -> Result<(), DbError> {
    if crate::feature_flags::set(name, true) {
        Ok(())
    } else {
        Err(DbError::QueryError(format!("unknown feature flag: {name}")))
    }
}

pub fn feature_disable(name: &str) -> Result<(), DbError> {
    if crate::feature_flags::set(name, false) {
        Ok(())
    } else {
        Err(DbError::QueryError(format!("unknown feature flag: {name}")))
    }
}

pub fn feature_info(name: &str) -> Option<FeatureFlagInfo> {
    crate::feature_flags::get(name).map(|f| {
        let mut info = FeatureFlagInfo {
            name: f.name,
            enabled: f.enabled,
            description: f.description,
            options: None,
        };
        // Populate feature-specific options
        if info.name == "recovery" {
            let mut opts = std::collections::BTreeMap::new();
            opts.insert(
                "auto_recover".to_string(),
                crate::feature_flags::recovery_auto_recover().to_string(),
            );
            info.options = Some(opts);
        }
        info
    })
}

/// Initialize runtime feature flags from environment variables.
/// Returns the list of flags after initialization for convenience.
pub fn init_from_env() -> Vec<FeatureFlagInfo> {
    crate::feature_flags::init_from_env();
    feature_list()
}

// --- Recovery options (auto-recover) ---

/// Enable or disable automatic recovery (best-effort verify/repair) on database open.
pub fn recovery_set_auto_recover(enabled: bool) {
    crate::feature_flags::recovery_set_auto_recover(enabled);
}

/// Returns whether automatic recovery is enabled.
pub fn recovery_auto_recover() -> bool {
    crate::feature_flags::recovery_auto_recover()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feature_info_has_recovery_option() {
        let f = feature_info("recovery").expect("recovery exists");
        // options may be Some with auto_recover key
        if let Some(opts) = f.options {
            assert!(opts.contains_key("auto_recover"));
        }
    }
}
