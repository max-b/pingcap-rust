use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::path::Path;
use std::result;

#[derive(Debug)]
pub enum KvStoreError {
    Io(io::Error),
    EncoderError(bson::EncoderError),
    DecoderError(bson::DecoderError),
    NonExistentKeyError(String),
    SerializationError(String),
}

impl From<io::Error> for KvStoreError {
    fn from(err: io::Error) -> KvStoreError {
        KvStoreError::Io(err)
    }
}

impl From<bson::EncoderError> for KvStoreError {
    fn from(err: bson::EncoderError) -> KvStoreError {
        KvStoreError::EncoderError(err)
    }
}

impl From<bson::DecoderError> for KvStoreError {
    fn from(err: bson::DecoderError) -> KvStoreError {
        KvStoreError::DecoderError(err)
    }
}

impl fmt::Display for KvStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "KvStoreError")
    }
}

impl Error for KvStoreError {
    fn description(&self) -> &str {
        match self {
            KvStoreError::Io(err) => err.description(),
            KvStoreError::EncoderError(err) => err.description(),
            KvStoreError::DecoderError(err) => err.description(),
            KvStoreError::NonExistentKeyError(string) => string,
            KvStoreError::SerializationError(string) => string,
        }
    }

    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            KvStoreError::Io(err) => Some(err),
            KvStoreError::EncoderError(err) => Some(err),
            KvStoreError::DecoderError(err) => Some(err),
            KvStoreError::NonExistentKeyError(_) => None,
            KvStoreError::SerializationError(_) => None,
        }
    }
}

/// A KvStore result that wraps KvStoreError
pub type Result<T> = result::Result<T, KvStoreError>;

/// An enum which defines records
#[derive(Serialize, Deserialize, Debug)]
enum Record {
    Set(String, String),
    Delete(String),
}

/// A Key Store mapping string keys to
/// string values
#[derive(Debug)]
pub struct KvStore {
    log_index: HashMap<String, u64>,
    file: File,
    reader: BufReader<File>,
    writer: BufWriter<File>,
}

impl KvStore {
    /// Open a directory for use as KvStore backing
    /// ```rust
    /// extern crate kvs;
    /// use kvs::KvStore;
    /// use std::path::Path;
    /// use tempfile::TempDir;
    /// # use std::error::Error;
    /// #
    /// # fn main() -> Result<(), Box<Error>> {
    /// let temp_dir = TempDir::new()?;
    /// let mut store = KvStore::open(temp_dir.path())?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    pub fn open(dirpath: &Path) -> Result<Self> {
        let path = dirpath.join("data.log");
        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(path)?;

        let mut log_index: HashMap<String, u64> = HashMap::new();

        let mut reader = BufReader::new(file.try_clone()?);

        let mut file_pointer_location = reader.seek(SeekFrom::Start(0))?;
        while let Ok(decoded) = bson::decode_document(&mut reader) {
            let bson_doc = bson::Bson::Document(decoded);

            let record: Record = bson::from_bson(bson_doc)?;
            match record {
                Record::Set(key, _value) => {
                    log_index.insert(key, file_pointer_location);
                }
                Record::Delete(key) => {
                    log_index.remove(&key);
                }
            };
            file_pointer_location = reader.seek(SeekFrom::Current(0))?;
        }

        let writer = BufWriter::new(file.try_clone()?);

        Ok(Self {
            file,
            log_index,
            reader,
            writer,
        })
    }

    /// Get a String value from a String key
    /// ```rust
    /// extern crate kvs;
    /// use kvs::KvStore;
    /// use std::path::Path;
    /// use tempfile::TempDir;
    /// # use std::error::Error;
    /// #
    /// # fn main() -> Result<(), Box<Error>> {
    /// let temp_dir = TempDir::new()?;
    /// let mut store = KvStore::open(temp_dir.path())?;
    /// store.set("key".to_owned(), "value".to_owned())?;
    /// let val = store.get("key".to_owned())?;
    /// assert_eq!(val, Some("value".to_owned()));
    /// #
    /// # Ok(())
    /// # }
    /// ```
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let record_log_location = self.log_index.get(&key);

        match record_log_location {
            None => Ok(None),
            Some(location) => {
                self.reader.seek(SeekFrom::Start(*location))?;
                let decoded = bson::decode_document(&mut self.reader)?;
                let bson_doc = bson::Bson::Document(decoded);

                let record: Record = bson::from_bson(bson_doc)?;
                match record {
                    Record::Set(_, value) => Ok(Some(value)),
                    Record::Delete(_) => Ok(None),
                }
            }
        }
    }

    /// Set a String key to a String key
    /// ```rust
    /// extern crate kvs;
    /// use kvs::KvStore;
    /// use std::path::Path;
    /// use tempfile::TempDir;
    /// # use std::error::Error;
    /// #
    /// # fn main() -> Result<(), Box<Error>> {
    /// let temp_dir = TempDir::new()?;
    /// let mut store = KvStore::open(temp_dir.path())?;
    /// store.set("key".to_owned(), "value".to_owned())?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let record = Record::Set(key, value);
        let new_record_location = self.writer.seek(SeekFrom::End(0))?;

        self.serialize_and_write(&record)?;
        if let Record::Set(key, _value) = record {
            self.log_index.insert(key, new_record_location);
            return Ok(());
        }

        Err(KvStoreError::SerializationError(
            "Error serializing record".to_owned(),
        ))
    }

    /// Remove a String key
    /// ```rust
    /// extern crate kvs;
    /// use kvs::KvStore;
    /// use std::path::Path;
    /// use tempfile::TempDir;
    /// # use std::error::Error;
    /// #
    /// # fn main() -> Result<(), Box<Error>> {
    /// let temp_dir = TempDir::new()?;
    /// let mut store = KvStore::open(temp_dir.path())?;
    /// store.set("key".to_owned(), "value".to_owned())?;
    /// store.remove("key".to_owned());
    /// let val = store.get("key".to_owned())?;
    /// assert_eq!(val, None);
    /// #
    /// # Ok(())
    /// # }
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        let mut record_option = None;

        match self.log_index.entry(key) {
            Entry::Vacant(_) => {
                return Err(KvStoreError::NonExistentKeyError(
                    "Key not in store".to_owned(),
                ));
            }
            Entry::Occupied(o) => {
                record_option = Some(Record::Delete(o.key().to_string()));
                o.remove_entry();
            }
        };

        if let Some(record) = record_option {
            self.serialize_and_write(&record)?;
        }

        Ok(())
    }

    /// Serialize and write to log file
    fn serialize_and_write(&mut self, record: &Record) -> Result<()> {
        let serialized_record = bson::to_bson(record)?;
        // TODO: probably should error here if it doesn't properly parse the document thing??
        // And/or I should just be manually creating a bson document so I don't need that
        // to_bson call??
        if let Some(document) = serialized_record.as_document() {
            bson::encode_document(&mut self.writer, document)?;
            self.writer.flush()?;
            return Ok(());
        }

        Err(KvStoreError::SerializationError(
            "Error serializing record".to_owned(),
        ))
    }
}
