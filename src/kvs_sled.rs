use sled::Db;
use std::path::Path;
use crate::errors::{KvStoreError, Result};
use crate::kv::KvsEngine;

/// TODO: documentation
pub struct SledKvsEngine {
    db: Db
}

impl KvsEngine for SledKvsEngine {
    /// TODO: documentation
    fn get(&mut self, key: String) -> Result<Option<String>> {
        self.db
            .get(key.as_bytes())
            .map(|o| 
                o.map(|v| String::from_utf8_lossy(&v).into_owned())
            )
            .map_err(|_| KvStoreError::NonExistentKeyError(key))
    }

    /// TODO: documentation
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let result = self.db
            .insert(key.as_bytes(), value.as_bytes())
            .map(|_| ())
            .map_err(|_| KvStoreError::NonExistentKeyError(key));
        self.db.flush()?;
        result
    }

    /// TODO: documentation
    fn remove(&mut self, key: String) -> Result<()> {
        let result = self.db
            .remove(key.as_bytes());

        self.db.flush()?;
        match result {
            Ok(o) => match o {
                None => Err(KvStoreError::NonExistentKeyError(key)),
                _v => Ok(()),
            },
            Err(e) => Err(KvStoreError::SledError(e))
        }
    }
}

impl SledKvsEngine {
    /// TODO: documentation
    pub fn open(dirpath: &Path) -> Result<Self> {
        let db = Db::open(dirpath).map_err(|_| KvStoreError::DatabaseInitializationError("Error opening sled db".to_owned()))?;
        Ok(Self {
            db
        })
    }
}
