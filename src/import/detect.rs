//! Format detection heuristics for import.

use std::io::{self, BufRead};
use std::path::Path;

use super::ImportFormat;

pub fn detect_format<R: BufRead>(reader: &mut R, path: &Path) -> io::Result<ImportFormat> {
	if let Some(ext) = path.extension().and_then(|s| s.to_str()) {
		match ext.to_lowercase().as_str() {
			"jsonl" | "ndjson" | "json" => return Ok(ImportFormat::Ndjson),
			"csv" => return Ok(ImportFormat::Csv),
			"bson" => return Ok(ImportFormat::Bson),
			_ => {}
		}
	}
	let buf = reader.fill_buf()?; // peek without consuming
	let n = std::cmp::min(buf.len(), 8);
	let head = &buf[..n];
	// Heuristic: BSON starts with plausible little-endian size
	if head.len() >= 4 {
		let size = i32::from_le_bytes([head[0], head[1], head[2], head[3]]);
		if size > 0 && size < 10_000_000 {
			// arbitrary sane bound
			return Ok(ImportFormat::Bson);
		}
	}
	// JSON or CSV: look for braces or commas in the first chunk
	let s = String::from_utf8_lossy(&buf[..std::cmp::min(256, buf.len())]);
	if s.trim_start().starts_with('{') || s.trim_start().starts_with('[') {
		return Ok(ImportFormat::Ndjson);
	}
	Ok(ImportFormat::Csv)
}
