use crate::errors::Result;

/// A trait which defines the required methods to implement a pluggable
/// storage backend for our key value server
pub trait KvsEngine: Clone + Send + 'static {
    /// Set a key to a value
    fn set(&self, key: String, value: String) -> Result<()>;

    /// Get a key's value
    fn get(&self, key: String) -> Result<Option<String>>;

    /// Remove a key's value from the store
    fn remove(&self, key: String) -> Result<()>;
}
