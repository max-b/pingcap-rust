use crate::errors::{KvStoreError, Result};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::path::{Path, PathBuf};

/// An enum which defines records
#[derive(Serialize, Deserialize, Debug)]
enum Record {
    Set(String, String),
    Delete(String),
}

/// A type for reading, and tracking log files
#[derive(Debug)]
struct LogFileReader {
    path: PathBuf,
    reader: BufReader<File>,
}

/// A type for reading, writing to, and tracking log files
#[derive(Debug)]
struct LogFileWriter {
    path: PathBuf,
    file: File,
    writer: BufWriter<File>,
}

// TODO: improve type naming here
/// A mapping between a key and a (file log path, file location, record size) tuple
type LogFileIndexMap = HashMap<String, (PathBuf, u64, u64)>;

/// A Key Store mapping string keys to
/// string values
#[derive(Debug)]
pub struct KvStore {
    log_index: LogFileIndexMap,
    log_file_readers: HashMap<PathBuf, LogFileReader>,
    active_log: LogFileWriter,
    dirpath: PathBuf,
    log_file_paths: Vec<PathBuf>,
    log_file_counter: usize,
    bytes_for_compaction: u64,
}

static COMPACT_AFTER_BYTE_SIZE: u64 = 2048;
static MAX_FILE_SIZE: u64 = 2048;

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
        let mut log_index: LogFileIndexMap = HashMap::new();
        let mut log_file_readers: HashMap<PathBuf, LogFileReader> = HashMap::new();

        let mut paths: Vec<_> = fs::read_dir(dirpath)?.filter_map(|r| r.ok()).collect();
        paths.sort_by_key(|dir| dir.metadata().unwrap().modified().unwrap());

        let mut last_path = None;
        let mut bytes_for_compaction = 0;

        for path in &paths {
            let file = OpenOptions::new().read(true).open(&path.path())?;

            let reader = BufReader::new(file);

            let mut log_file = LogFileReader {
                reader,
                path: PathBuf::from(path.path()),
            };

            let mut file_pointer_location = log_file.reader.seek(SeekFrom::Start(0))?;

            while let Ok(decoded) = bson::decode_document(&mut log_file.reader) {
                let new_file_pointer_location = log_file.reader.seek(SeekFrom::Current(0))?;
                let record_size = new_file_pointer_location - file_pointer_location;
                let bson_doc = bson::Bson::Document(decoded);

                let record: Record = bson::from_bson(bson_doc)?;
                match record {
                    Record::Set(key, _value) => {
                        if let Some(prev) = log_index.insert(
                            key,
                            (
                                PathBuf::from(path.path()),
                                file_pointer_location,
                                record_size,
                            ),
                        ) {
                            let (_, _, prev_record_size) = prev;
                            bytes_for_compaction = bytes_for_compaction + prev_record_size;
                        }
                    }
                    Record::Delete(key) => {
                        log_index.remove(&key);
                    }
                };
                file_pointer_location = log_file.reader.seek(SeekFrom::Current(0))?;
            }

            last_path = Some(path.path());
            log_file_readers.insert(path.path(), log_file);
        }

        let mut log_file_paths: Vec<PathBuf> = paths.into_iter().map(|d| d.path()).collect();
        let log_file_counter = log_file_readers.len();

        let active_log_path = if let Some(path) = last_path {
            path
        } else {
            let path: PathBuf = [dirpath.clone(), &PathBuf::from(format!("{}.log", 0))]
                .iter()
                .collect();
            log_file_paths.push(path.clone());
            path
        };

        let active_log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&active_log_path)?;

        let writer = BufWriter::new(active_log_file.try_clone()?);
        let reader = BufReader::new(active_log_file.try_clone()?);

        let active_log = LogFileWriter {
            file: active_log_file,
            writer,
            path: active_log_path.clone(),
        };

        let active_log_reader = LogFileReader {
            reader,
            path: active_log_path.clone(),
        };

        log_file_readers.insert(active_log_path.clone(), active_log_reader);

        Ok(Self {
            log_index,
            log_file_readers,
            active_log,
            dirpath: dirpath.to_path_buf(),
            log_file_paths,
            log_file_counter,
            bytes_for_compaction,
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
            Some((log_file_path, location, _record_size)) => {
                // TODO: fix unwrap!
                let file_log = self.log_file_readers.get_mut(log_file_path).unwrap();
                file_log.reader.seek(SeekFrom::Start(*location))?;
                let decoded = bson::decode_document(&mut file_log.reader)?;
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
        let record = Record::Set(key.clone(), value.clone());

        // TODO replace unwrap w/ ?
        let new_record_location = self.serialize_and_write(&record)?;
        if let Some(prev) = self.log_index.insert(key, new_record_location.clone()) {
            let (_, _, record_size) = prev;
            self.bytes_for_compaction = self.bytes_for_compaction + record_size;
        }

        self.compact()?;

        Ok(())
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
                let previous_record = o.get();
                let (_, _, record_size) = previous_record;
                self.bytes_for_compaction = self.bytes_for_compaction + record_size;
                o.remove_entry();
            }
        };

        if let Some(record) = record_option {
            self.serialize_and_write(&record)?;
        }

        self.compact()?;

        Ok(())
    }

    /// Open a new log file for writing to
    fn open_new_log_file(&mut self) -> Result<()> {
        self.log_file_counter = self.log_file_counter + 1;
        let new_log_path: PathBuf = [
            self.dirpath.clone(),
            PathBuf::from(format!("{}.log", self.log_file_counter)),
        ]
        .iter()
        .collect();

        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&new_log_path)?;

        let reader = BufReader::new(file.try_clone()?);

        self.log_file_readers.insert(
            new_log_path.clone(),
            LogFileReader {
                reader,
                path: new_log_path.clone(),
            },
        );

        let writer = BufWriter::new(file.try_clone()?);
        self.active_log = LogFileWriter {
            writer,
            file,
            path: new_log_path.clone(),
        };

        self.log_file_paths.push(new_log_path);

        Ok(())
    }

    /// Compact oldest log entry
    fn compact(&mut self) -> Result<()> {
        if self.bytes_for_compaction <= COMPACT_AFTER_BYTE_SIZE {
            return Ok(());
        }

        if self.log_file_paths.len() <= 1 {
            return Ok(());
        }

        let mut key_to_remove = None;
        if let Some(path_to_remove) = &self.log_file_paths.first().cloned() {
            let file = OpenOptions::new().read(true).open(&path_to_remove)?;

            let mut reader = BufReader::new(file);
            let mut current_record_location = reader.seek(SeekFrom::Start(0))?;

            while let Ok(decoded) = bson::decode_document(&mut reader) {
                let bson_doc = bson::Bson::Document(decoded);

                let record: Record = bson::from_bson(bson_doc)?;

                if let Record::Set(key, record_value) = record {
                    let record_log_location = self.log_index.get(&key);

                    if let Some((path, location, record_size)) = record_log_location {
                        if path == path_to_remove && *location == current_record_location {
                            let record = Record::Set(key.clone(), record_value);
                            let new_record_location = self.serialize_and_write(&record)?;
                            self.log_index.insert(key.clone(), new_record_location);
                        } else {
                            self.bytes_for_compaction = match self.bytes_for_compaction.checked_sub(*record_size) {
                                Some(b) => b,
                                None => 0
                            };
                        }
                    }
                }
                current_record_location = reader.seek(SeekFrom::Current(0))?;
            }
            key_to_remove = Some(path_to_remove.clone());
        }

        if let Some(path) = key_to_remove {
            self.log_file_readers.remove(&path);
            fs::remove_file(&path)?;
            self.log_file_paths.retain(|x| x != &path);
        }

        Ok(())
    }

    /// Get the active log file, potentially opening a new one
    /// for writing to
    fn setup_active_log_file(&mut self) -> Result<()> {
        if self.active_log.file.metadata()?.len() > MAX_FILE_SIZE {
            self.open_new_log_file()?;
        }
        Ok(())
    }

    /// Serialize and write to log file
    /// Returns the location of the record that was written
    /// as a (log_file_path, location_in_file, record_size) tuple
    fn serialize_and_write(&mut self, record: &Record) -> Result<(PathBuf, u64, u64)> {
        self.setup_active_log_file()?;

        let record_location_start = self.active_log.writer.seek(SeekFrom::End(0))?;

        let serialized_record = bson::to_bson(record)?;
        // TODO: probably should error here if it doesn't properly parse the document thing??
        // And/or I should just be manually creating a bson document so I don't need that
        // to_bson call??
        if let Some(document) = serialized_record.as_document() {
            bson::encode_document(&mut self.active_log.writer, document)?;
            let record_location_end = self.active_log.writer.seek(SeekFrom::Current(0))?;
            let record_size = record_location_end - record_location_start;
            self.active_log.writer.flush()?;

            return Ok((
                self.active_log.path.clone(),
                record_location_start,
                record_size,
            ));
        }

        Err(KvStoreError::SerializationError(
            "Error serializing record".to_owned(),
        ))
    }
}
