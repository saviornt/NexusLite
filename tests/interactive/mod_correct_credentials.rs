use std::io::IsTerminal;
use std::process::{Command, Stdio};
use tempfile::tempdir;

// Prompts for admin/password; run manually in a real terminal.
#[test]
#[ignore]
fn open_db_prompts_and_succeeds_with_correct_credentials() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("prompt_ok.db");
    {
        let engine = nexuslite::engine::Engine::new(dir.path().join("ok.wasp")).unwrap();
        let _db = nexuslite::Database::new(Some(db_path.to_str().unwrap())).unwrap();
        let _ = nexuslite::api::create_document(&engine, Some("c"), "{\"a\":1}", false, None);
        // Encrypt with known creds
        nexuslite::api::encrypt_db_with_password(db_path.as_path(), "admin", "password").unwrap();
        drop(engine);
    }
    // Ensure no env creds
    unsafe {
        std::env::remove_var("NEXUSLITE_USERNAME");
        std::env::remove_var("NEXUSLITE_PASSWORD");
    }

    if !std::io::stdin().is_terminal() {
        eprintln!("Interactive test (correct): stdin not a TTY; skipping.");
        return;
    }

    eprintln!("Interactive test (correct): enter -> Username: admin | Password: password");
    let status = Command::new(env!("CARGO_BIN_EXE_nexuslite"))
        .arg("db")
        .arg("open")
        .arg(db_path.to_string_lossy().to_string())
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .expect("spawn nexuslite");
    assert!(status.success(), "expected success when entering correct creds");
}
