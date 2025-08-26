// Submodules for clear separation of concerns
pub mod admin;
pub mod collections;
pub mod crypto;
pub mod db;
pub mod feature;

// Re-export the public API surface from submodules for a stable facade
pub use admin::{
    CollectionInfo, InfoReport, info, log_configure, log_configure_from_env, log_init_from_file,
    log_init_from_file_path, telemetry_configure_rate_limit, telemetry_remove_rate_limit,
    telemetry_set_audit_enabled, telemetry_set_db_name, telemetry_set_default_rate_limit,
    telemetry_set_max_results_for, telemetry_set_max_results_global, telemetry_set_query_log,
};
pub use collections::{
    count, create_document, delete_many, delete_one, export, find, import, parse_filter_json,
    parse_update_json, update_many, update_one,
};
pub use crypto::{
    checkpoint_encrypted, crypto_decrypt_file, crypto_encrypt_file, crypto_generate_p256,
    crypto_sign_file, crypto_verify_file, decrypt_db_with_password, encrypt_db_with_password,
    restore_encrypted,
};
pub use db::{
    db_close, db_create_collection, db_delete_collection, db_list_collections, db_new, db_open,
    db_rename_collection,
};
pub use feature::{
    FeatureFlagInfo, feature_disable, feature_enable, feature_info, feature_list, init_from_env,
    recovery_auto_recover, recovery_set_auto_recover,
};
