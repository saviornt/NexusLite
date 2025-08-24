/// Initializes the logging system using log4rs.yaml (legacy fallback).
///
/// # Errors
/// Returns an error if the configuration file cannot be read or parsed.
pub fn init() -> Result<(), Box<dyn std::error::Error>> {
    log4rs::init_file("log4rs.yaml", log4rs::config::Deserializers::default())?;
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
    use log4rs::append::file::FileAppender;
    use log4rs::config::{Appender, Config, Root};
    use log4rs::encode::pattern::PatternEncoder;
    use std::fs;
    use std::path::PathBuf;
    let mut dir = PathBuf::from(base_dir);
    dir.push(format!("{db_name}_logs"));
    fs::create_dir_all(&dir)?;
    let logfile = dir.join(format!("{db_name}.log"));
    let encoder = Box::new(PatternEncoder::new("{d(%Y-%m-%d %H:%M:%S%.3f)} [{l}] {t} - {m}{n}"));
    let file_appender = FileAppender::builder().encoder(encoder).build(logfile)?;
    let config = Config::builder()
        .appender(Appender::builder().build("file", Box::new(file_appender)))
        .build(Root::builder().appender("file").build(LevelFilter::Info))?;
    log4rs::init_config(config)?;
    Ok(())
}
