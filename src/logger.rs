/// Initializes the logging system using log4rs.yaml (legacy fallback).
pub fn init() -> Result<(), Box<dyn std::error::Error>> {
    log4rs::init_file("log4rs.yaml", Default::default())?;
    Ok(())
}

/// Initializes logging to a database-scoped folder: `{db_name}_logs`.
/// Creates folder if missing and writes rolling log files.
pub fn init_for_db(db_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    use log::LevelFilter;
    use log4rs::append::file::FileAppender;
    use log4rs::config::{Appender, Config, Root};
    use log4rs::encode::pattern::PatternEncoder;
    use std::fs;
    let log_dir = format!("{}_logs", db_name);
    fs::create_dir_all(&log_dir)?;
    let logfile = format!("{}/nexuslite.log", log_dir);
    let encoder = Box::new(PatternEncoder::new("{d(%Y-%m-%d %H:%M:%S%.3f)} [{l}] {t} - {m}{n}"));
    let file_appender = FileAppender::builder().encoder(encoder).build(logfile)?;
    let config = Config::builder()
        .appender(Appender::builder().build("file", Box::new(file_appender)))
        .build(Root::builder().appender("file").build(LevelFilter::Info))?;
    log4rs::init_config(config)?;
    Ok(())
}
