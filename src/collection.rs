use std::collections::HashMap;
use crate::document::{Document, DocumentId};

#[derive(Debug)]
pub struct Collection {
    pub name: String,
    pub documents: HashMap<DocumentId, Document>,
}

impl Collection {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            documents: HashMap::new(),
        }
    }

    /// Insert a document into this collection
    pub fn insert_document(&mut self, doc: Document) -> DocumentId {
        let id = doc.id;
        self.documents.insert(id, doc);
        id
    }

    /// Find a document by ID
    pub fn find_document(&self, id: &DocumentId) -> Option<&Document> {
        self.documents.get(id)
    }

    /// Update a document (replace existing data)
    pub fn update_document(&mut self, id: &DocumentId, new_doc: Document) -> Option<()> {
        if self.documents.contains_key(id) {
            self.documents.insert(*id, new_doc);
            Some(())
        } else {
            None
        }
    }

    /// Delete a document by ID
    pub fn delete_document(&mut self, id: &DocumentId) -> Option<Document> {
        self.documents.remove(id)
    }

    /// List all document IDs
    pub fn list_document_ids(&self) -> Vec<DocumentId> {
        self.documents.keys().cloned().collect()
    }
}
