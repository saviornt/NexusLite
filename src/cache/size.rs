use crate::document::Document;

#[inline]
pub fn approximate_doc_size(doc: &Document) -> usize {
    let mut sz = 0usize;
    if let Ok(bytes) = doc.data.0.to_vec() {
        sz += bytes.len();
    }
    // Rough overhead estimate for metadata
    sz + 16 + 32 + 8 + 1
}
