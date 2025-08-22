use chrono::Local;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::{Mutex, LazyLock};

static LOGGER: LazyLock<Mutex<TestLogger>> = LazyLock::new(|| Mutex::new(TestLogger::new().unwrap()));

pub struct TestLogger {
    file: File,
}

impl TestLogger {
    pub fn new() -> std::io::Result<Self> {
        let datetime = Local::now().format("%Y%m%d_%H%M%S").to_string();
        let filename = format!("test_log_{}.log", datetime);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(filename)?;
        Ok(Self { file })
    }

    pub fn log(&mut self, msg: &str) {
        let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let line = format!("[{}] {}\n", timestamp, msg);
        let _ = self.file.write_all(line.as_bytes());
    }
}

pub fn log_test(msg: &str) {
    if let Ok(mut logger) = LOGGER.lock() {
        logger.log(msg);
    }
}
