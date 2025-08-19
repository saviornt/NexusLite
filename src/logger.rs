/// Initializes the logging system.
///
/// This function sets up the logger based on a configuration file.
/// It should be called once at the beginning of the application's execution.
pub fn init() -> Result<(), Box<dyn std::error::Error>> {
    log4rs::init_file("log4rs.yaml", Default::default())?;
    Ok(())
}
