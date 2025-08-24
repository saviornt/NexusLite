use crate::collection::Collection;
use crate::document::{Document, DocumentType};
use std::io::{self, Read};

use super::options::ImportOptions;
use super::options::ImportReport;
use crate::import::util::apply_ttl;

pub fn import_bson<R: Read>(
    collection: &std::sync::Arc<Collection>,
    mut reader: R,
    doc_type: DocumentType,
    _opts: &ImportOptions,
    report: &mut ImportReport,
) -> io::Result<()> {
    let mut full = Vec::with_capacity(4096);
    loop {
        let mut len_buf = [0u8; 4];
        if reader.read_exact(&mut len_buf).is_err() {
            break;
        }
        let len = i32::from_le_bytes(len_buf);
        if len <= 0 || len > 16_000_000 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid bson size"));
        }
        let len = usize::try_from(len)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid bson size"))?;
        if full.capacity() < len {
            full.reserve(len - full.capacity());
        }
        full.clear();
        // Extend to full length and fill
        full.extend_from_slice(&len_buf);
        let start = full.len();
        full.resize(len, 0);
        reader.read_exact(&mut full[start..])?;
        match bson::Document::from_reader(&mut &full[..]) {
            Ok(doc) => {
                let mut d = Document::new(doc.clone(), doc_type);
                apply_ttl(&mut d, &doc, None);
                collection.insert_document(d);
                report.inserted += 1;
            }
            Err(_) => report.skipped += 1,
        }
    }
    Ok(())
}
