use crate::document::Document;
use bson::to_vec as bson_to_vec;

#[inline]
pub fn approximate_doc_size(doc: &Document) -> usize {
    let mut sz = 0usize;
    if let Ok(bytes) = bson_to_vec(&doc.data.0) {
        sz += bytes.len();
    }
    // Rough overhead estimate for metadata
    sz + 16 + 32 + 8 + 1
}
