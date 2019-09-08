use crate::errors::{KvStoreError, Result};

/// TODO: document
pub trait KvsEngine {
    /// Set a key to a value
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// Get a key's value
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// Remove a key's value from the store
    fn remove(&mut self, key: String) -> Result<()>;
}
