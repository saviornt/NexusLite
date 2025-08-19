use std::fs;
use std::io::Read;
use tempfile::tempdir;
use nexus_lite;
use log::info;

#[test]
fn test_lib_init_logger() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let log_file_path = dir.path().join("test_lib_log.log");
    let config_file_path = dir.path().join("log4rs.yaml");

    let config_content = format!(
        "refresh_rate: 30 seconds\nappenders:\n  requests:\n    kind: file\n    path: \"{}\"\n    encoder:\n      pattern: \"{{d}} - {{m}}{{n}}\"\nroot:\n  level: info\n  appenders:\n    - requests",
        log_file_path.to_str().unwrap().replace("\\", "/") // Use forward slashes for path in YAML
    );
    fs::write(&config_file_path, config_content)?;

    // Temporarily change the current directory to the temp directory
    let original_dir = std::env::current_dir()?;
    std::env::set_current_dir(dir.path())?;

    // Initialize the library (which initializes the logger)
    nexus_lite::init()?;

    // Log a message
    info!("This is a test log message from lib init.");

    // Give some time for the logger to write (it's async)
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Read the log file content
    let mut file = fs::File::open(&log_file_path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    // Assert that the log message is in the file
    assert!(contents.contains("This is a test log message from lib init."));

    // Restore the original directory
    std::env::set_current_dir(original_dir)?;

    Ok(())
}