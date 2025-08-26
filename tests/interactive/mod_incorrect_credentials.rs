use rand::RngCore;
use std::io::IsTerminal;
use std::process::{Command, Stdio};
use tempfile::tempdir;

// Prompts for random wrong creds; expect failure; run manually in a real terminal.
#[test]
#[ignore]
fn open_db_prompts_and_fails_with_incorrect_credentials() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("prompt_fail.db");
    {
        let engine = nexuslite::engine::Engine::new(dir.path().join("fail.wasp")).unwrap();
        let _db = nexuslite::Database::new(Some(db_path.to_str().unwrap())).unwrap();
        let _ = nexuslite::api::create_document(&engine, Some("c"), "{\"a\":1}", false, None);
        nexuslite::api::encrypt_db_with_password(db_path.as_path(), "admin", "password").unwrap();
        drop(engine);
    }
    // Ensure no env creds
    unsafe {
        std::env::remove_var("NEXUSLITE_USERNAME");
        std::env::remove_var("NEXUSLITE_PASSWORD");
    }

    if !std::io::stdin().is_terminal() {
        eprintln!("Interactive test (incorrect): stdin not a TTY; skipping.");
        return;
    }

    let mut rng = rand::rng();
    let bad_user = format!("u{:02x}", (rng.next_u32() & 0xff));
    let bad_pass = format!("p{:02x}", (rng.next_u32() & 0xff));
    eprintln!(
        "Interactive test (incorrect): enter WRONG -> Username: {} | Password: {}",
        bad_user, bad_pass
    );

    let status = Command::new(env!("CARGO_BIN_EXE_nexuslite"))
        .arg("db")
        .arg("open")
        .arg(db_path.to_string_lossy().to_string())
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .expect("spawn nexuslite");
    assert!(!status.success(), "expected failure when entering incorrect creds");
}
