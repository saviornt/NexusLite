use lru::LruCache;
use std::time::SystemTime;

use crate::types::{CollectionName, Document, DocumentId};

#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub value: Document,
    pub expires_at: Option<SystemTime>,
}

pub type HotCache = LruCache<(CollectionName, DocumentId), CacheEntry>;
