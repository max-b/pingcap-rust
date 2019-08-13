use std::collections::HashMap;
use std::result;
use std::path::Path;
use std::io;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::io::prelude::*;
use std::fs::{File, OpenOptions};
use std::error::Error;
use std::fmt;
use serde::{Serialize, Deserialize};

#[derive(Debug)]
pub enum KvStoreError {
    Io(io::Error),
    EncoderError(bson::EncoderError),
    DecoderError(bson::DecoderError),
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
        }
    }

    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            KvStoreError::Io(err) => Some(err),
            KvStoreError::EncoderError(err) => Some(err),
            KvStoreError::DecoderError(err) => Some(err),
        }
    }
}

/// A KvStore result that wraps KvStoreError
pub type Result<T> = result::Result<T, KvStoreError>;

/// An enum which defines records
#[derive(Serialize, Deserialize, Debug)]
enum Record {
    Set(String, String),
    Delete(String)
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
    /// Create a new KvStore
    /// ```rust
    /// extern crate kvs;
    /// use kvs::KvStore;
    /// ```
    /// Open a file for use as KvStore backing
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).create(true).append(true).open(path)?;

        let mut log_index: HashMap<String, u64> = HashMap::new();

        let mut reader = BufReader::new(file.try_clone()?);

        let mut file_pointer_location = reader.seek(SeekFrom::Start(0))?;
        while let Ok(decoded) = bson::decode_document(&mut reader) {
            let bson_doc = bson::Bson::Document(decoded);

            let record: Record = bson::from_bson(bson_doc)?;
            match record {
                Record::Set(key, value) => {
                    log_index.insert(key, file_pointer_location);
                },
                Record::Delete(key) => {
                    log_index.remove(&key);
                },
            };
            file_pointer_location = reader.seek(SeekFrom::Current(0))?;
        }

        let mut writer = BufWriter::new(file.try_clone()?);
        // TODO: do we want to have a BufReader and a BufWriter for our kvstore instead of a file
        // handle?
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
    /// let mut store = KvStore::new();
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
                    Record::Set(key, value) => {
                        println!("found value: {} for key: {}", &value, &key);
                        Ok(Some(value))
                    },
                    Record::Delete(_) => {
                        Ok(None)
                    }
                }
            }
        }
    }

    /// Set a String key to a String key
    /// ```rust
    /// extern crate kvs;
    /// use kvs::KvStore;
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let record = Record::Set(key, value);
        let new_record_location = self.writer.seek(SeekFrom::End(0))?;

        self.serialize_and_write(&record)?;
        if let Record::Set(key, value) = record {
            self.log_index.insert(key, new_record_location);
            return Ok(())
        }

        // TODO: probably want to Error or something here because this should not actually
        // ever occur
        Ok(())
    }

    /// Remove a String key
    /// ```rust
    /// extern crate kvs;
    /// use kvs::KvStore;
    /// let mut store = KvStore::new();
    /// store.set(String::from("key"), String::from("value"));
    /// store.remove(String::from("key"))
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        let record = Record::Delete(key);

        self.serialize_and_write(&record)?;

        // The reason to use this weirdish if let is to pull the key value
        // back out from the record without cloning anywhere...
        if let Record::Delete(key) = record {
            self.log_index.remove(&key);
            return Ok(())
        }

        // TODO: probably want to Error or something here because this should not actually
        // ever occur
        Ok(())
    }

    /// Serialize and write to log file
    fn serialize_and_write(&mut self, record: &Record) -> Result<()> {
        let serialized_record = bson::to_bson(record)?;
        // TODO: probably should error here if it doesn't properly parse the document thing??
        if let Some(document) = serialized_record.as_document() {
            bson::encode_document(&mut self.writer, document)?;
        }

        Ok(())
    }
}
