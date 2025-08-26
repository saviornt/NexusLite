use crate::collection::Collection;
use crate::document::Document;
use crate::types::DocumentId;
use std::sync::Arc;

#[derive(Clone)]
pub struct Cursor {
    pub collection: Arc<Collection>,
    pub ids: Vec<DocumentId>,
    pub pos: usize,
    pub docs: Option<Vec<Document>>, // when present, iterate these
}

impl Cursor {
    pub fn advance(&mut self) -> Option<Document> {
        if let Some(ref docs) = self.docs {
            if self.pos >= docs.len() {
                return None;
            }
            let d = docs[self.pos].clone();
            self.pos += 1;
            return Some(d);
        }
        if self.pos >= self.ids.len() {
            return None;
        }
        let id = self.ids[self.pos].clone();
        self.pos += 1;
        self.collection.find_document(&id)
    }
    #[must_use]
    pub fn to_vec(mut self) -> Vec<Document> {
        if let Some(docs) = self.docs.take() {
            return docs;
        }
        let mut out = Vec::with_capacity(self.ids.len());
        while let Some(d) = self.advance() {
            out.push(d);
        }
        out
    }
}

impl Iterator for Cursor {
    type Item = Document;
    fn next(&mut self) -> Option<Self::Item> {
        self.advance()
    }
}
