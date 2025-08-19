use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use crate::collection::Collection;
use serde::{Serialize, Deserialize, Serializer, Deserializer};
use serde::ser::SerializeStruct;
use std::fmt;
use bincode::config;

pub struct Engine {
    pub collections: RwLock<HashMap<String, Arc<Collection>>>,
}

impl Engine {
    pub fn new() -> Self {
        Engine {
            collections: RwLock::new(HashMap::new()),
        }
    }

    pub fn create_collection(&self, name: String) -> Arc<Collection> {
        let mut collections = self.collections.write().unwrap();
        let collection = Arc::new(Collection::new(name.clone()));
        collections.insert(name, collection.clone());
        collection
    }

    pub fn get_collection(&self, name: &str) -> Option<Arc<Collection>> {
        let collections = self.collections.read().unwrap();
        collections.get(name).cloned()
    }

    pub fn delete_collection(&self, name: &str) -> bool {
        self.collections.write().unwrap().remove(name).is_some()
    }

    pub fn list_collection_names(&self) -> Vec<String> {
        self.collections.read().unwrap().keys().cloned().collect()
    }

    pub fn save<P: AsRef<std::path::Path>>(&self, path: P) -> Result<(), Box<dyn std::error::Error>> {
        let collections_map = self.collections.read().unwrap();
        let serializable_map: HashMap<String, Collection> = collections_map.iter()
            .map(|(name, collection_arc)| (name.clone(), (**collection_arc).clone()))
            .collect();
        let encoded = bincode::serde::encode_to_vec(&serializable_map, config::standard())?;
        std::fs::write(path, encoded)?;
        Ok(())
    }

    pub fn load<P: AsRef<std::path::Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let serialized = std::fs::read(path)?;
        let (deserialized_map, _): (HashMap<String, Collection>, usize) = bincode::serde::decode_from_slice(&serialized, config::standard())?;
        let collections = deserialized_map.into_iter()
            .map(|(name, collection)| (name, Arc::new(collection)))
            .collect();
        Ok(Engine {
            collections: RwLock::new(collections),
        })
    }
}

// Custom Serialize for Engine
impl Serialize for Engine {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let collections_map = self.collections.read().unwrap();
        let serializable_map: HashMap<String, Collection> = collections_map.iter()
            .map(|(name, collection_arc)| (name.clone(), (**collection_arc).clone()))
            .collect();
        let mut state = serializer.serialize_struct("Engine", 1)?;
        state.serialize_field("collections", &serializable_map)?;
        state.end()
    }
}

// Custom Deserialize for Engine
impl<'de> Deserialize<'de> for Engine {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct EngineVisitor;

        impl<'de> serde::de::Visitor<'de> for EngineVisitor {
            type Value = Engine;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Engine")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut collections: Option<HashMap<String, Collection>> = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        "collections" => {
                            collections = Some(map.next_value()?);
                        }
                        _ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                let collections = collections.unwrap_or_default();
                let collections_arc = collections.into_iter()
                    .map(|(name, collection)| (name, Arc::new(collection)))
                    .collect();
                Ok(Engine {
                    collections: RwLock::new(collections_arc),
                })
            }
        }

        deserializer.deserialize_struct("Engine", &["collections"], EngineVisitor)
    }
}
