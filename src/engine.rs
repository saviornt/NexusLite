use crate::cache::{CacheEntry, HotCache};
use crate::collection::Collection;
use crate::errors::DbError;
use crate::types::{CollectionName, Document, DocumentId};
use crate::wal::{read_record, write_record, OpKind, WalRecord};
use parking_lot::{Mutex, RwLock};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime};
use uuid::Uuid;

/// Persistent engine configuration.
#[derive(Debug, Clone)]
pub struct EngineOptions {
    pub path: PathBuf,
    /// Max number of hot entries kept in the LRU cache.
    pub cache_capacity: usize,
    /// TTL sweep interval for background maintenance (seconds).
    pub ttl_sweep_secs: u64,
    /// Whether to spawn a background maintenance thread to clean expired cache entries.
    pub enable_background_maintenance: bool,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            path: PathBuf::from("./data"),
            cache_capacity: 10_000,
            ttl_sweep_secs: 2,
            enable_background_maintenance: true,
        }
    }
}

/// The main embedded engine.
pub struct Engine {
    pub(crate) options: EngineOptions,
    // In-memory collections
    pub(crate) collections: RwLock<HashMap<CollectionName, Collection>>,
    // Redis-like cache with TTL + LRU
    pub(crate) cache: Mutex<HotCache>,
    // WAL file + write buffer
    pub(crate) wal_writer: Mutex<BufWriter<File>>,
    pub(crate) wal_path: PathBuf,
    // handle to stop maintenance thread on drop
    maint_stop: Mutex<Option<std::sync::mpsc::Sender<()>>>,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("options", &self.options)
            .field("wal_path", &self.wal_path)
            .finish()
    }
}

impl Engine {
    /// Open (or create) a database at `options.path`.
    /// Replays the WAL to rebuild in-memory state.
    pub fn open(options: EngineOptions) -> Result<Self, DbError> {
        fs::create_dir_all(&options.path)?;
        let wal_path = options.path.join("wal.bin");

        // Ensure WAL exists
        if !wal_path.exists() {
            File::create(&wal_path)?;
        }

        // Open WAL for append + read (for replay).
        let wal_file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .open(&wal_path)?;

        // Quick pass over WAL to ensure readability (ignore contents).
        {
            let mut reader = BufReader::new(File::open(&wal_path)?);
            Self::replay_wal(&mut reader)?;
        }

        // Create writer after replay.
        let wal_writer = BufWriter::new(wal_file);

        let engine = Self {
            options: options.clone(),
            collections: RwLock::new(HashMap::new()),
            cache: Mutex::new(HotCache::new(options.cache_capacity.try_into().unwrap_or(1024))),
            wal_writer: Mutex::new(wal_writer),
            wal_path,
            maint_stop: Mutex::new(None),
        };

        // Load state from WAL into memory
        let state = Self::load_state_from_wal(&engine.options)?;
        *engine.collections.write() = state;

        if engine.options.enable_background_maintenance {
            engine.spawn_maintenance();
        }

        Ok(engine)
    }

    fn spawn_maintenance(&self) {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        *self.maint_stop.lock() = Some(tx);

        let cache = self.cache.clone();
        let sweep = self.options.ttl_sweep_secs;

        thread::Builder::new()
            .name("nosql-embed-maintenance".into())
            .spawn(move || loop {
                if let Ok(_) = rx.recv_timeout(Duration::from_secs(sweep)) {
                    break;
                }
                let now = SystemTime::now();
                let mut cache = cache.lock();
                // Collect keys to remove (can't remove while iterating)
                let mut to_remove = Vec::new();
                for (k, v) in cache.iter() {
                    if let Some(exp) = v.expires_at {
                        if exp <= now {
                            to_remove.push(k.clone());
                        }
                    }
                }
                for k in to_remove {
                    cache.pop(&k);
                }
            })
            .ok();
    }

    fn stop_maintenance(&self) {
        if let Some(tx) = self.maint_stop.lock().take() {
            let _ = tx.send(());
        }
    }

    /// Create a collection if it doesn't exist.
    pub fn create_collection(&self, name: impl Into<String>) -> Result<(), DbError> {
        let name = name.into();
        {
            let mut cols = self.collections.write();
            if !cols.contains_key(&name) {
                cols.insert(name.clone(), Collection::default());
            }
        }
        self.append_wal(WalRecord {
            op: OpKind::CreateCol,
            collection: Some(name),
            id: None,
            value_json: None,
            expires_at: None,
            ts: SystemTime::now(),
        })?;
        Ok(())
    }

    /// Insert or update a document (write-through to WAL).
    /// If `id` is None, a UUID is generated.
    /// `ttl_secs`: if Some, the document expires after the given seconds.
    pub fn upsert(
        &self,
        collection: &str,
        id: Option<&str>,
        mut doc: Document,
        ttl_secs: Option<u64>,
    ) -> Result<DocumentId, DbError> {
        // Ensure collection exists
        {
            let cols = self.collections.read();
            if !cols.contains_key(collection) {
                drop(cols);
                self.create_collection(collection)?;
            }
        }

        // Ensure doc has an _id
        let id_str = id
            .map(|s| s.to_string())
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        if let Value::Object(obj) = &mut doc {
            obj.insert("_id".to_string(), Value::String(id_str.clone()));
        } else {
            // Force object to keep things consistent
            doc = json!({"_id": id_str.clone(), "value": doc});
        }

        let expires_at = ttl_secs.map(|s| SystemTime::now() + Duration::from_secs(s));

        // WAL first
        let value_json = serde_json::to_vec(&doc)?;
        self.append_wal(WalRecord {
            op: OpKind::Upsert,
            collection: Some(collection.to_string()),
            id: Some(id_str.clone()),
            value_json: Some(value_json),
            expires_at,
            ts: SystemTime::now(),
        })?;

        // Apply to in-memory store
        {
            let mut cols = self.collections.write();
            let col = cols
                .get_mut(collection)
                .ok_or_else(|| DbError::NoSuchCollection(collection.into()))?;
            col.insert(id_str.clone(), doc.clone(), expires_at);
        }

        // Update cache
        {
            let mut cache = self.cache.lock();
            cache.put(
                (collection.to_string(), id_str.clone()),
                CacheEntry {
                    value: doc.clone(),
                    expires_at,
                },
            );
        }

        Ok(id_str)
    }

    /// Get a document by id. Respects TTL (expired docs act as missing).
    pub fn get(&self, collection: &str, id: &str) -> Result<Option<Document>, DbError> {
        // Cache first
        let now = SystemTime::now();
        {
            let mut cache = self.cache.lock();
            if let Some(entry) = cache.get(&(collection.to_string(), id.to_string())) {
                if entry.expires_at.map_or(true, |t| t > now) {
                    return Ok(Some(entry.value.clone()));
                } else {
                    cache.pop(&(collection.to_string(), id.to_string()));
                }
            }
        }

        // Main store
        let found = {
            let cols = self.collections.read();
            let col = match cols.get(collection) {
                Some(c) => c,
                None => return Ok(None),
            };
            col.get(id)
        };

        if let Some((doc, exp)) = found {
            if exp.map_or(true, |t| t > now) {
                // repopulate cache
                let mut cache = self.cache.lock();
                cache.put(
                    (collection.to_string(), id.to_string()),
                    CacheEntry {
                        value: doc.clone(),
                        expires_at: exp,
                    },
                );
                Ok(Some(doc))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Delete a document by id.
    pub fn delete(&self, collection: &str, id: &str) -> Result<bool, DbError> {
        // WAL
        self.append_wal(WalRecord {
            op: OpKind::Delete,
            collection: Some(collection.to_string()),
            id: Some(id.to_string()),
            value_json: None,
            expires_at: None,
            ts: SystemTime::now(),
        })?;

        // Remove from cache + store
        {
            let mut cache = self.cache.lock();
            cache.pop(&(collection.to_string(), id.to_string()));
        }
        let mut removed = false;
        {
            let mut cols = self.collections.write();
            if let Some(col) = cols.get_mut(collection) {
                removed = col.remove(id);
            }
        }
        Ok(removed)
    }

    /// Flush WAL to disk (fsync).
    pub fn flush(&self) -> Result<(), DbError> {
        let mut w = self.wal_writer.lock();
        w.flush()?;
        w.get_mut().sync_all()?;
        Ok(())
    }

    /// WAL compaction: rewrite active state to a new WAL file.
    pub fn compact(&self) -> Result<(), DbError> {
        // Snapshot
        let snapshot = self.collections.read().clone();

        // Write to a new temp file
        let tmp_path = self.options.path.join("wal.compacting.bin");
        {
            let tmp = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;
            let mut writer = BufWriter::new(tmp);

            // Recreate collections then upsert all live docs
            for (cname, _col) in snapshot.iter() {
                let rec = WalRecord {
                    op: OpKind::CreateCol,
                    collection: Some(cname.clone()),
                    id: None,
                    value_json: None,
                    expires_at: None,
                    ts: SystemTime::now(),
                };
                write_record(&mut writer, &rec)?;
            }
            let now = SystemTime::now();
            for (cname, col) in snapshot.iter() {
                for (id, (doc, exp)) in col.docs.iter() {
                    if let Some(t) = *exp {
                        if t <= now {
                            continue;
                        }
                    }
                    let rec = WalRecord {
                        op: OpKind::Upsert,
                        collection: Some(cname.clone()),
                        id: Some(id.clone()),
                        value_json: Some(serde_json::to_vec(doc)?),
                        expires_at: *exp,
                        ts: SystemTime::now(),
                    };
                    write_record(&mut writer, &rec)?;
                }
            }
            writer.flush()?;
            writer.get_mut().sync_all()?;
        }

        // Swap files
        {
            {
                let mut w = self.wal_writer.lock();
                w.flush()?;
            }
            let wal_backup = self.options.path.join("wal.backup.bin");
            if wal_backup.exists() {
                let _ = fs::remove_file(&wal_backup);
            }
            fs::rename(&self.wal_path, &wal_backup)?;
            fs::rename(&tmp_path, &self.wal_path)?;

            // Reopen WAL writer append
            let wal_file = OpenOptions::new()
                .read(true)
                .write(true)
                .append(true)
                .open(&self.wal_path)?;
            let writer = BufWriter::new(wal_file);
            *self.wal_writer.lock() = writer;

            let _ = fs::remove_file(&wal_backup);
        }
        Ok(())
    }

    fn append_wal(&self, rec: WalRecord) -> Result<(), DbError> {
        let mut w = self.wal_writer.lock();
        write_record(&mut *w, &rec)?;
        Ok(())
    }

    /// Replays WAL into a throwaway map and returns it (pure function).
    fn load_state_from_wal(options: &EngineOptions) -> Result<HashMap<CollectionName, Collection>, DbError> {
        let mut reader = BufReader::new(File::open(options.path.join("wal.bin"))?);
        let mut map: HashMap<CollectionName, Collection> = HashMap::new();
        loop {
            match read_record(&mut reader) {
                Ok(Some(rec)) => {
                    match rec.op {
                        OpKind::CreateCol => {
                            if let Some(c) = rec.collection {
                                map.entry(c).or_default();
                            }
                        }
                        OpKind::Upsert => {
                            if let (Some(c), Some(id), Some(bytes)) = (rec.collection.clone(), rec.id.clone(), rec.value_json.clone()) {
                                let doc: Document = serde_json::from_slice(&bytes)?;
                                let col = map.entry(c).or_default();
                                col.insert(id, doc, rec.expires_at);
                            }
                        }
                        OpKind::Delete => {
                            if let (Some(c), Some(id)) = (rec.collection.clone(), rec.id.clone()) {
                                if let Some(col) = map.get_mut(&c) {
                                    col.remove(&id);
                                }
                            }
                        }
                    }
                }
                Ok(None) => break,
                Err(_e) => break, // best-effort if truncated
            }
        }
        // Drop expired entries during replay
        let now = SystemTime::now();
        for (_c, col) in map.iter_mut() {
            col.retain_live(now);
        }
        Ok(map)
    }

    fn replay_wal(reader: &mut BufReader<File>) -> Result<(), DbError> {
        loop {
            match read_record(reader) {
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(_e) => break,
            }
        }
        Ok(())
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.stop_maintenance();
        let _ = self.flush();
    }
}
