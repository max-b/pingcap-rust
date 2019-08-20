use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::path::{Path, PathBuf};
use crate::errors::{KvStoreError, Result};

/// An enum which defines records
#[derive(Serialize, Deserialize, Debug)]
enum Record {
    Set(String, String),
    Delete(String),
}

/// A type for reading, writing to, and tracking log files
#[derive(Debug)]
struct LogFile {
    index: usize,
    path: PathBuf,
    file: File,
    reader: BufReader<File>,
    writer: BufWriter<File>,
}

/// A mapping between a key and a (file collection index, file location) tuple
type LogFileIndexMap = HashMap<String, (usize, u64)>;

/// A Key Store mapping string keys to
/// string values
#[derive(Debug)]
pub struct KvStore {
    log_index: LogFileIndexMap,
    log_files: Vec<LogFile>,
    dirpath: PathBuf,
    compaction_index: usize,
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
        let mut log_index: LogFileIndexMap = HashMap::new();
        let mut log_files: Vec<LogFile> = Vec::new();

        let mut log_file_index = 0;

        let mut paths: Vec<_> = fs::read_dir(dirpath)?
                                                      .filter_map(|r| r.ok())
                                                      .collect();
        paths.sort_by_key(|dir| dir.metadata().unwrap().modified().unwrap());

        for path in paths {
            println!("path = {:?}", &path);
            let file = OpenOptions::new()
                .read(true)
                .create(true)
                .append(true)
                .open(&path.path())?;

            let reader = BufReader::new(file.try_clone()?);
            let writer = BufWriter::new(file.try_clone()?);

            let mut log_file = LogFile {
                index: log_file_index,
                reader,
                writer,
                file,
                path: PathBuf::from(path.path()),
            };

            let mut file_pointer_location = log_file.reader.seek(SeekFrom::Start(0))?;

            while let Ok(decoded) = bson::decode_document(&mut log_file.reader) {
                let bson_doc = bson::Bson::Document(decoded);

                let record: Record = bson::from_bson(bson_doc)?;
                match record {
                    Record::Set(key, _value) => {
                        log_index.insert(key, (log_file_index, file_pointer_location));
                    }
                    Record::Delete(key) => {
                        log_index.remove(&key);
                    }
                };
                file_pointer_location = log_file.reader.seek(SeekFrom::Current(0))?;
            }

            log_files.push(log_file);
            log_file_index = log_file_index + 1;
        }

        Ok(Self {
            log_index,
            log_files,
            dirpath: dirpath.to_path_buf(),
            compaction_index: 0,
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
        println!("record_log_location = {:?}", &record_log_location);

        match record_log_location {
            None => Ok(None),
            Some((file_log_index, location)) => {
                let file_log = &mut self.log_files[*file_log_index];
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
        let record = Record::Set(key, value);
        println!("record = {:?}", &record);

        let new_record_location = self.serialize_and_write(&record)?;
        if let Record::Set(key, _value) = record {
            println!("setting key: {}, to location: {:?}", &key, &new_record_location);
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

    /// Open a new log file for writing to
    fn open_new_log_file(&mut self) -> Result<()> {
        let new_log_path: PathBuf = [self.dirpath.clone(), PathBuf::from(format!("{}.log", self.log_files.len()))].iter().collect();

        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&new_log_path)?;

        let reader = BufReader::new(file.try_clone()?);
        let writer = BufWriter::new(file.try_clone()?);
        let log_file_index = self.log_files.len();

        self.log_files.push(LogFile {
            index: log_file_index,
            reader,
            writer,
            file,
            path: new_log_path,
        });

        Ok(())
    }

    /// Compact oldest log entry
    fn compact(&mut self) -> Result<()> {
        println!("ever gets to compaction???");
        let log_file = self.log_files.get(self.compaction_index);

        println!("log_file = {:?}", &log_file);
        match log_file {
            None => {
                return Ok(());
            },
            Some(log_file) => {
                let log_file_index = log_file.index;
                let log_file_path = log_file.path.clone();
                let mut reader = BufReader::new(log_file.file.try_clone()?);
                reader.seek(SeekFrom::Start(0))?;

                while let Ok(decoded) = bson::decode_document(&mut reader) {
                    let bson_doc = bson::Bson::Document(decoded);

                    let record: Record = bson::from_bson(bson_doc)?;
                    match record {
                        Record::Set(key, record_value) => {
                            let record_log_location = self.log_index.get(&key);

                            match record_log_location {
                                None => {},
                                Some((idx, location)) => {
                                    println!("idx = {}, log_file_index = {}", &idx, &log_file_index);
                                    if idx == &log_file_index {
                                        println!("setting a new value from compaction");
                                        self.set(key, record_value)?;
                                    }
                                }
                            }
                        }
                        Record::Delete(_key) => {}
                    };
                }

                println!("removing file {:?}", &log_file_path);
                fs::remove_file(&log_file_path)?;
            }
        };

        self.compaction_index = self.compaction_index + 1;

        Ok(())
    }

    /// Get the active log file, potentially opening a new one
    /// for writing to
    fn setup_active_log_file(&mut self) -> Result<()> {
        let active_log = self.log_files.last_mut();
        match active_log {
            None => {
                self.open_new_log_file()?;
            },
            Some(log_file) => {
                // If log_file length is greater than max file size, make new file
                if log_file.file.metadata()?.len() > 204800 {
                    self.open_new_log_file()?;
                }
            }
        };

        Ok(())
    }

    /// Serialize and write to log file
    /// Returns the location of the record that was written
    /// as a (log_file_index, location_in_file) tuple
    fn serialize_and_write(&mut self, record: &Record) -> Result<(usize, u64)> {
        self.setup_active_log_file()?;
        // setup_active_log_file always ensures that there will be at least
        // one log_file in the log_files vector
        let log_file = self.log_files.last_mut().unwrap();

        let new_record_location = log_file.writer.seek(SeekFrom::End(0))?;

        let serialized_record = bson::to_bson(record)?;
        // TODO: probably should error here if it doesn't properly parse the document thing??
        // And/or I should just be manually creating a bson document so I don't need that
        // to_bson call??
        if let Some(document) = serialized_record.as_document() {
            bson::encode_document(&mut log_file.writer, document)?;
            log_file.writer.flush()?;

            if self.log_files.len() - self.compaction_index > 10 {
                self.compact()?;
            }

            return Ok((self.log_files.len() - 1, new_record_location));
        }

        Err(KvStoreError::SerializationError(
            "Error serializing record".to_owned(),
        ))
    }
}
