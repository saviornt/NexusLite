use nexus_lite::cli::{Command, run};
use nexus_lite::engine::Engine;
use nexus_lite::errors::DbError;
use std::fs;
use std::io::Write;
use tempfile::tempdir;

#[tokio::test]
async fn test_cli_import_export_ndjson() {
    let dir = tempdir().unwrap();
    let wasp_path = dir.path().join("test.wasp");
    let engine = Engine::new(wasp_path).unwrap();

    // Write NDJSON input
    let in_path = dir.path().join("in.jsonl");
    {
        let mut f = fs::File::create(&in_path).unwrap();
        writeln!(f, "{{\"name\":\"alice\"}}\n{{\"name\":\"bob\"}}\n").unwrap();
    }

    // Import via CLI
    run(
        &engine,
        Command::Import {
            collection: "users".into(),
            file: in_path.clone(),
            format: Some("ndjson".into()),
        },
    )
    .unwrap();

    // Export via CLI
    let out_path = dir.path().join("out.jsonl");
    run(
        &engine,
        Command::Export {
            collection: "users".into(),
            file: out_path.clone(),
            format: Some("ndjson".into()),
            redact_fields: None,
            filter_json: None,
            limit: None,
        },
    )
    .unwrap();

    // Verify exported file content
    let s = fs::read_to_string(out_path).unwrap();
    assert!(s.contains("alice"));
    assert!(s.contains("bob"));
}

#[tokio::test]
async fn test_cli_telemetry_rate_limit() {
    let dir = tempdir().unwrap();
    let wasp_path = dir.path().join("test.wasp");
    let engine = Engine::new(wasp_path).unwrap();

    // Create collection and set a very tight rate limit
    let col = engine.create_collection("users".to_string());
    nexus_lite::telemetry::configure_rate_limit(&col.name_str(), 1, 0);

    // First count ok
    let filter = "true".to_string();
    let ok = run(
        &engine,
        Command::QueryCount { collection: col.name_str(), filter_json: filter.clone() },
    );
    assert!(ok.is_ok());
    // Second count should be rate-limited
    let err = run(&engine, Command::QueryCount { collection: col.name_str(), filter_json: filter });
    match err {
        Ok(()) => panic!("expected rate limited error"),
        Err(e) => {
            if let Some(db) = e.downcast_ref::<DbError>() {
                match db {
                    DbError::RateLimited | DbError::RateLimitedWithRetry { .. } => {}
                    _ => panic!("unexpected db error: {db}"),
                }
            } else {
                panic!("unexpected error type: {e}");
            }
        }
    }
}
