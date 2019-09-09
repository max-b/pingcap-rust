use crate::errors::{Result};

/// A trait which defines the required methods to implement a pluggable
/// storage backend for our key value server
pub trait KvsEngine {
    /// Set a key to a value
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// Get a key's value
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// Remove a key's value from the store
    fn remove(&mut self, key: String) -> Result<()>;
}
