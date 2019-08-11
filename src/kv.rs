use std::collections::HashMap;

/// A Key Store mapping string keys to
/// string values
pub struct KvStore {
    store: HashMap<String, String>,
}

impl KvStore {
    /// Create a new KvStore
    /// ```rust
    /// extern crate kvs;
    /// use kvs::KvStore;
    /// let store = KvStore::new();
    /// ```
    pub fn new() -> Self {
        KvStore {
            store: HashMap::new(),
        }
    }

    /// Get a String value from a String key
    /// ```rust
    /// extern crate kvs;
    /// use kvs::KvStore;
    /// let mut store = KvStore::new();
    /// store.set(String::from("key"), String::from("value"));
    /// let value = store.get(String::from("key"));
    /// ```
    pub fn get(&self, key: String) -> Option<String> {
        self.store.get(&key).cloned()
    }

    /// Set a String key to a String key
    /// ```rust
    /// extern crate kvs;
    /// use kvs::KvStore;
    /// let mut store = KvStore::new();
    /// store.set(String::from("key"), String::from("value"));
    /// ```
    pub fn set(&mut self, key: String, value: String) {
        self.store.insert(key, value);
    }

    /// Remove a String key
    /// ```rust
    /// extern crate kvs;
    /// use kvs::KvStore;
    /// let mut store = KvStore::new();
    /// store.set(String::from("key"), String::from("value"));
    /// store.remove(String::from("key"))
    /// ```
    pub fn remove(&mut self, key: String) {
        self.store.remove(&key);
    }
}
