/// Initializes the logging system from the default file `log4rs.yaml` in the working directory.
/// Prefer `configure_logging` and `init_for_db_in` for programmatic control.
pub fn init() -> Result<(), Box<dyn std::error::Error>> {
    let _ = log4rs::init_file("log4rs.yaml", log4rs::config::Deserializers::default());
    Ok(())
}

/// Initializes the logging system from a specific config file path.
pub fn init_path(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let _ = log4rs::init_file(path, log4rs::config::Deserializers::default());
    Ok(())
}

/// Initializes logging to a database-scoped folder: `{db_name}_logs`.
/// Creates folder if missing and writes rolling log files.
///
/// # Errors
/// Returns an error if the directory cannot be created or the logger fails to initialize.
pub fn init_for_db(db_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    use log::LevelFilter;
    use log4rs::append::file::FileAppender;
    use log4rs::config::{Appender, Config, Root};
    use log4rs::encode::pattern::PatternEncoder;
    use std::fs;
    let log_dir = format!("{db_name}_logs");
    fs::create_dir_all(&log_dir)?;
    let logfile = format!("{log_dir}/{db_name}.log");
    let encoder = Box::new(PatternEncoder::new("{d(%Y-%m-%d %H:%M:%S%.3f)} [{l}] {t} - {m}{n}"));
    let file_appender = FileAppender::builder().encoder(encoder).build(logfile)?;
    let config = Config::builder()
        .appender(Appender::builder().build("file", Box::new(file_appender)))
        .build(Root::builder().appender("file").build(LevelFilter::Info))?;
    log4rs::init_config(config)?;
    Ok(())
}

/// Initializes logging to a specific base directory, creating `{base}/{db_name}_logs/{db_name}.log`.
///
/// # Errors
/// Returns an error if the directory cannot be created or the logger fails to initialize.
pub fn init_for_db_in(
    base_dir: &std::path::Path,
    db_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use log::LevelFilter;
    use log4rs::append::rolling_file::RollingFileAppender;
    use log4rs::append::rolling_file::policy::compound::{
        CompoundPolicy, roll::fixed_window::FixedWindowRoller, trigger::size::SizeTrigger,
    };
    use log4rs::config::{Appender, Config, Logger, Root};
    use log4rs::encode::pattern::PatternEncoder;
    use std::fs;
    use std::path::PathBuf;
    let mut dir = PathBuf::from(base_dir);
    dir.push(format!("{db_name}_logs"));
    fs::create_dir_all(&dir)?;
    let encoder_pattern = "{d(%Y-%m-%d %H:%M:%S%.3f)} [{l}] {t} - {m}{n}";
    // App logs (legacy-compatible name tested by integration tests): {db_name}.log
    let app_log = dir.join(format!("{db_name}.log"));
    let app_roller = FixedWindowRoller::builder()
        .build(&format!("{}", dir.join(format!("{db_name}.{{}}.log")).display()), 7)?;
    let app_trigger = SizeTrigger::new(10 * 1024 * 1024);
    let app_policy = CompoundPolicy::new(Box::new(app_trigger), Box::new(app_roller));
    let appender = RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d(%Y-%m-%d %H:%M:%S%.3f)} [{l}] {m}{n}")))
        .build(app_log, Box::new(app_policy))?;
    // Audit logs (Creates an auditable log of database usage for security monitoring.)
    let use_log = dir.join(format!("{db_name}_audit.log"));
    let use_roller = FixedWindowRoller::builder()
        .build(&format!("{}", dir.join(format!("{db_name}.audit.{{}}.log")).display()), 7)?;
    let use_policy =
        CompoundPolicy::new(Box::new(SizeTrigger::new(10 * 1024 * 1024)), Box::new(use_roller));
    let usage_appender = RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(encoder_pattern)))
        .build(use_log, Box::new(use_policy))?;
    // Metrics logs
    let met_log = dir.join(format!("{db_name}_metrics.log"));
    let met_roller = FixedWindowRoller::builder()
        .build(&format!("{}", dir.join(format!("{db_name}.metrics.{{}}.log")).display()), 7)?;
    let met_policy =
        CompoundPolicy::new(Box::new(SizeTrigger::new(10 * 1024 * 1024)), Box::new(met_roller));
    let metrics_appender = RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(encoder_pattern)))
        .build(met_log, Box::new(met_policy))?;

    let config = Config::builder()
        .appender(Appender::builder().build("app", Box::new(appender)))
        .appender(Appender::builder().build("audit", Box::new(usage_appender)))
        .appender(Appender::builder().build("metrics", Box::new(metrics_appender)))
        .logger(
            Logger::builder()
                .appender("audit")
                .additive(false)
                .build("nexuslite::audit", LevelFilter::Info),
        )
        .logger(
            Logger::builder()
                .appender("metrics")
                .additive(false)
                .build("nexuslite::metrics", LevelFilter::Info),
        )
        .build(Root::builder().appender("app").build(LevelFilter::Info))?;
    log4rs::init_config(config)?;
    Ok(())
}

/// Configure logging globally for the process. If log4rs is already initialized, this will replace the config.
/// - dir: base directory for logs; if None, current directory.
/// - level: error|warn|info|debug|trace
/// - retention: number of rolled files to keep (default 7)
pub fn configure_logging(
    dir: Option<&std::path::Path>,
    level: Option<&str>,
    retention: Option<usize>,
) {
    configure_logging_with_dev(dir, level, retention, false);
}

/// Configure logging globally with optional dev6 (developer-level) routing to files.
/// If `enable_dev6` is true, messages logged via the `dev6!` macro (target `nexuslite::dev6`) will also be persisted
/// to a `dev6.log` rolling file in the base directory.
pub fn configure_logging_with_dev(
    dir: Option<&std::path::Path>,
    level: Option<&str>,
    retention: Option<usize>,
    enable_dev6: bool,
) {
    use log::LevelFilter;
    use log4rs::append::rolling_file::RollingFileAppender;
    use log4rs::append::rolling_file::policy::compound::{
        CompoundPolicy, roll::fixed_window::FixedWindowRoller, trigger::size::SizeTrigger,
    };
    use log4rs::config::{Appender, Config, Logger, Root};
    use log4rs::encode::pattern::PatternEncoder;
    use std::path::PathBuf;
    let base = dir
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    let keep = retention.unwrap_or(7) as u32;
    let lvl = match level.unwrap_or("info").to_ascii_lowercase().as_str() {
        "error" => LevelFilter::Error,
        "warn" => LevelFilter::Warn,
        "debug" => LevelFilter::Debug,
        "trace" => LevelFilter::Trace,
        _ => LevelFilter::Info,
    };
    let enc_pattern = "{d(%Y-%m-%d %H:%M:%S%.3f)} [{l}] {t} - {m}{n}";
    let roller = FixedWindowRoller::builder()
        .build(&format!("{}", base.join("app.{}.log").display()), keep)
        .unwrap();
    let policy =
        CompoundPolicy::new(Box::new(SizeTrigger::new(10 * 1024 * 1024)), Box::new(roller));
    let appender = RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(enc_pattern)))
        .build(base.join("app.log"), Box::new(policy))
        .unwrap();
    let usage_roller = FixedWindowRoller::builder()
        .build(&format!("{}", base.join("audit.{}.log").display()), keep)
        .unwrap();
    let usage_policy =
        CompoundPolicy::new(Box::new(SizeTrigger::new(10 * 1024 * 1024)), Box::new(usage_roller));
    let usage_appender = RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(enc_pattern)))
        .build(base.join("audit.log"), Box::new(usage_policy))
        .unwrap();
    let metrics_roller = FixedWindowRoller::builder()
        .build(&format!("{}", base.join("metrics.{}.log").display()), keep)
        .unwrap();
    let metrics_policy =
        CompoundPolicy::new(Box::new(SizeTrigger::new(10 * 1024 * 1024)), Box::new(metrics_roller));
    let metrics_appender = RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(enc_pattern)))
        .build(base.join("metrics.log"), Box::new(metrics_policy))
        .unwrap();
    let mut builder = Config::builder()
        .appender(Appender::builder().build("app", Box::new(appender)))
        .appender(Appender::builder().build("audit", Box::new(usage_appender)))
        .appender(Appender::builder().build("metrics", Box::new(metrics_appender)))
        .logger(Logger::builder().appender("audit").additive(false).build("nexuslite::audit", lvl))
        .logger(Logger::builder().appender("metrics").additive(false).build("nexuslite::metrics", lvl));

    if enable_dev6 {
        let dev6_roller = FixedWindowRoller::builder()
            .build(&format!("{}", base.join("dev6.{}.log").display()), keep)
            .unwrap();
        let dev6_policy = CompoundPolicy::new(
            Box::new(SizeTrigger::new(10 * 1024 * 1024)),
            Box::new(dev6_roller),
        );
        let dev6_appender = RollingFileAppender::builder()
            .encoder(Box::new(PatternEncoder::new(enc_pattern)))
            .build(base.join("dev6.log"), Box::new(dev6_policy))
            .unwrap();
        builder = builder
            .appender(Appender::builder().build("dev6", Box::new(dev6_appender)))
            .logger(
                Logger::builder()
                    .appender("dev6")
                    .additive(false)
                    .build("nexuslite::dev6", LevelFilter::Trace),
            );
    } else {
        // Allow runtime visibility even without file routing (for debugging)
        builder = builder.logger(
            Logger::builder().additive(false).build("nexuslite::dev6", LevelFilter::Trace),
        );
    }

    let config = builder.build(Root::builder().appender("app").build(lvl)).unwrap();
    let _ = log4rs::init_config(config);
}

/// Configure logging from environment variables if present:
/// - NEXUSLITE_LOG_DIR
/// - NEXUSLITE_LOG_LEVEL
/// - NEXUSLITE_LOG_RETENTION
pub fn configure_from_env() {
    let dir = std::env::var("NEXUSLITE_LOG_DIR").ok().map(std::path::PathBuf::from);
    let level = std::env::var("NEXUSLITE_LOG_LEVEL").ok();
    let retention =
        std::env::var("NEXUSLITE_LOG_RETENTION").ok().and_then(|s| s.parse::<usize>().ok());
    let dev6_enabled = std::env::var("NEXUSLITE_DEV6")
        .map(|s| matches!(s.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    let level_ref = level.as_deref();
    configure_logging_with_dev(dir.as_deref(), level_ref, retention, dev6_enabled);
}
