use crate::errors::{KvStoreError, Result};
use crate::kv::KvsEngine;
use sled::Db;
use std::path::Path;

/// A wrapper for the sled db which implements the KvsEngine trait
#[derive(Clone, Debug)]
pub struct SledKvsEngine {
    db: Db,
}

impl KvsEngine for SledKvsEngine {
    /// Get a key
    fn get(&self, key: String) -> Result<Option<String>> {
        self.db
            .get(key.as_bytes())
            .map(|o| o.map(|v| String::from_utf8_lossy(&v).into_owned()))
            .map_err(|_| KvStoreError::NonExistentKeyError(key))
    }

    /// Set a key's value
    fn set(&self, key: String, value: String) -> Result<()> {
        let result = self
            .db
            .insert(key.as_bytes(), value.as_bytes())
            .map(|_| ())
            .map_err(|_| KvStoreError::NonExistentKeyError(key));
        // TODO: can we get away w/out flushing here? it's *terrible* for performance
        // self.db.flush()?;
        result
    }

    /// Remove a key from the database
    fn remove(&self, key: String) -> Result<()> {
        let result = self.db.remove(key.as_bytes());

        // TODO: can we get away w/out flushing here? it's *terrible* for performance
        self.db.flush()?;
        match result {
            Ok(o) => match o {
                None => Err(KvStoreError::NonExistentKeyError(key)),
                _v => Ok(()),
            },
            Err(e) => Err(KvStoreError::SledError(e)),
        }
    }
}

impl SledKvsEngine {
    /// Open the sled db for reading and writing
    pub fn open(dirpath: &Path) -> Result<Self> {
        let db = Db::open(dirpath)?;
        Ok(Self { db })
    }
}
