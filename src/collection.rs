use std::collections::HashMap;
use std::sync::RwLock;
use uuid::Uuid;
use crate::document::Document;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct Collection {
    pub name: String,
    pub documents: RwLock<HashMap<Uuid, Document>>,
}

impl Clone for Collection {
    fn clone(&self) -> Self {
        Collection {
            name: self.name.clone(),
            documents: RwLock::new(self.documents.read().unwrap().clone()),
        }
    }
}

impl Collection {
    pub fn new(name: String) -> Self {
        Collection {
            name,
            documents: RwLock::new(HashMap::new()),
        }
    }

    pub fn insert_document(&self, document: Document) -> Uuid {
        let doc_id = document.id;
        self.documents.write().unwrap().insert(doc_id, document);
        doc_id
    }

    pub fn find_document(&self, id: &Uuid) -> Option<Document> {
        self.documents.read().unwrap().get(id).cloned()
    }

    pub fn update_document(&self, id: &Uuid, new_document: Document) -> bool {
        let mut documents = self.documents.write().unwrap();
        if documents.contains_key(id) {
            documents.insert(*id, new_document);
            true
        } else {
            false
        }
    }

    pub fn delete_document(&self, id: &Uuid) -> bool {
        self.documents.write().unwrap().remove(id).is_some()
    }

    pub fn list_document_ids(&self) -> Vec<Uuid> {
        self.documents.read().unwrap().keys().cloned().collect()
    }
}
