// Mirror src/utils: group feature_flags, errors, types, fsutil, logger tests here
#[path = "mod_errors.rs"]
mod errors_tests;
#[path = "mod_feature_flags.rs"]
mod feature_flags_tests;
#[path = "mod_minimal_async.rs"]
mod minimal_async_tests;
#[path = "mod_types.rs"]
mod types_tests;
// Placeholders for fsutil/logger tests if added later
// #[path = "mod_fsutil.rs"]
// mod fsutil_tests;
#[path = "mod_logger.rs"]
mod logger_tests;
