// Run with: cargo run --bin benchmark_wal_vs_wasp
// Results saved to benchmarks/results/benchmark_crashStorage_{datetime}.csv

use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

use bson::doc;
use nexus_lite::cache::CacheConfig;
use nexus_lite::document::{Document, DocumentType};
use nexus_lite::engine::Engine;

#[tokio::main]
async fn main() {
	// Create unique temp directories without tempfile crate
	let base = std::env::temp_dir();
	let wal_dir = base.join(format!("nexuslite_wal_{}", uuid::Uuid::new_v4()));
	let wasp_dir = base.join(format!("nexuslite_wasp_{}", uuid::Uuid::new_v4()));
	create_dir_all(&wal_dir).unwrap();
	create_dir_all(&wasp_dir).unwrap();

	let wal_path = wal_dir.join("bench_wal.log");
	let wasp_path = wasp_dir.join("bench_wasp.log");

	let n = 2000usize;

	// WAL-based engine
	let engine_wal = Engine::new(wal_path).unwrap();
	let col_wal = engine_wal.create_collection_with_config("bench".into(), CacheConfig { capacity: n + 16, ..Default::default() });
	let start_insert_wal = Instant::now();
	for i in 0..n {
		let d = Document::new(doc!{"i": i as i64, "payload": format!("x{}", i)}, DocumentType::Persistent);
		let _ = col_wal.insert_document(d);
	}
	let insert_wal_ns = start_insert_wal.elapsed().as_nanos();

	let start_read_wal = Instant::now();
	let docs_wal = col_wal.get_all_documents();
	let read_wal_ns = start_read_wal.elapsed().as_nanos();
	assert_eq!(docs_wal.len(), n);

	// WASP-based engine
	let engine_wasp = Engine::with_wasp(wasp_path).unwrap();
	let col_wasp = engine_wasp.create_collection_with_config("bench".into(), CacheConfig { capacity: n + 16, ..Default::default() });
	let start_insert_wasp = Instant::now();
	for i in 0..n {
		let d = Document::new(doc!{"i": i as i64, "payload": format!("x{}", i)}, DocumentType::Persistent);
		let _ = col_wasp.insert_document(d);
	}
	let insert_wasp_ns = start_insert_wasp.elapsed().as_nanos();

	let start_read_wasp = Instant::now();
	let docs_wasp = col_wasp.get_all_documents();
	let read_wasp_ns = start_read_wasp.elapsed().as_nanos();
	assert_eq!(docs_wasp.len(), n);

	// Persist results
	let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
	root.push("benchmarks/results");
	create_dir_all(&root).unwrap();

	let now = chrono::Utc::now();
	let datetime = now.format("%Y%m%d_%H%M%S");
	let unique = format!("benchmark_crashStorage_{}.csv", datetime);
	let mut f = File::create(root.join(&unique)).unwrap();
	writeln!(f, "backend,insert_ns,read_ns").unwrap();
	writeln!(f, "WAL,{insert_wal_ns},{read_wal_ns}").unwrap();
	writeln!(f, "WASP,{insert_wasp_ns},{read_wasp_ns}").unwrap();

	// Cleanup: drop engines to release file handles before deleting files on Windows
	// Scope ensures all references are dropped prior to cleanup
	// (engines and collections go out of scope here).

	// Give the OS a brief moment to release file locks (Windows)
	#[cfg(target_os = "windows")]
	std::thread::sleep(std::time::Duration::from_millis(100));

	// Remove created storage files and directories (keep results CSV)
	let _ = std::fs::remove_file(wal_dir.join("bench_wal.log"));
	let _ = std::fs::remove_file(wasp_dir.join("bench_wasp.log"));
	let _ = std::fs::remove_dir_all(&wal_dir);
	let _ = std::fs::remove_dir_all(&wasp_dir);
}
