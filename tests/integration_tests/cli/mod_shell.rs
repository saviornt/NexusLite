use std::io::Write;
use std::process::{Command, Stdio};

#[test]
fn shell_starts_and_accepts_commands() {
    // Spawn the binary with `shell` and feed a couple of commands
    let mut child = Command::new(env!("CARGO_BIN_EXE_nexuslite"))
        .arg("shell")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("failed to spawn shell");
    {
        let mut stdin = child.stdin.take().unwrap();
        writeln!(stdin, "help").unwrap();
        writeln!(stdin, "config").unwrap();
        writeln!(stdin, "info").unwrap();
        writeln!(stdin, "list-collections").unwrap();
        writeln!(stdin, "exit").unwrap();
    }
    let out = child.wait_with_output().expect("shell run");
    assert!(out.status.success());
}

#[test]
fn doctor_redacts_secrets_and_scans_configs() {
    use std::fs;
    use std::io::Write as _;
    // no extra imports

    // Create a temp config file with secret-like keys
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg_path = dir.path().join("nexuslite.toml");
    let mut f = fs::File::create(&cfg_path).expect("create cfg");
    writeln!(f, "password = 'p'\napi_key='k'\n[section]\nsecret='s'").unwrap();

    // Run doctor with specific env vars set for the child process only
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_nexuslite"))
        .arg("doctor")
        .env("NEXUSLITE_CONFIG", cfg_path.to_string_lossy().to_string())
        .env("SUPER_SECRET_TOKEN", "value")
        .output()
        .expect("doctor run");
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("config_scanned:"));
    assert!(stdout.contains("status:warning"));
    assert!(stdout.contains("env_secrets:"));
    assert!(stdout.contains("env:SUPER_SECRET_TOKEN=REDACTED"));
}
