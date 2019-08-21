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
    path: PathBuf,
    file: File,
    reader: BufReader<File>,
    writer: BufWriter<File>,
}

/// A mapping between a key and a (file log path, file location) tuple
type LogFileIndexMap = HashMap<String, (PathBuf, u64)>;

/// A Key Store mapping string keys to
/// string values
#[derive(Debug)]
pub struct KvStore {
    log_index: LogFileIndexMap,
    log_files: HashMap<PathBuf, LogFile>,
    dirpath: PathBuf,
    active_log_path: Option<PathBuf>,
    log_file_paths: Vec<PathBuf>,
    log_file_counter: usize,
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
        let mut log_files: HashMap<PathBuf, LogFile> = HashMap::new();

        println!("opening path: {:?}", &dirpath);
        let mut paths: Vec<_> = fs::read_dir(dirpath)?
                                                      .filter_map(|r| r.ok())
                                                      .collect();
        paths.sort_by_key(|dir| dir.metadata().unwrap().modified().unwrap());

        let mut last_path = None;

        for path in &paths {
            let file = OpenOptions::new()
                .read(true)
                .create(true)
                .append(true)
                .open(&path.path())?;

            println!("opened file = {:?}", &file);
            let reader = BufReader::new(file.try_clone()?);
            let writer = BufWriter::new(file.try_clone()?);

            let mut log_file = LogFile {
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
                        log_index.insert(key, (PathBuf::from(path.path()), file_pointer_location));
                    }
                    Record::Delete(key) => {
                        log_index.remove(&key);
                    }
                };
                file_pointer_location = log_file.reader.seek(SeekFrom::Current(0))?;
            }

            last_path = Some(path.path());
            log_files.insert(path.path(), log_file);
        }

        let log_file_paths = paths.into_iter().map(|d| d.path()).collect();
        let log_file_counter = log_files.len();

        Ok(Self {
            log_index,
            log_files,
            dirpath: dirpath.to_path_buf(),
            active_log_path: last_path,
            log_file_paths,
            log_file_counter,
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
            Some((file_log_path, location)) => {
                // TODO: fix unwrap!
                let file_log = self.log_files.get_mut(file_log_path).unwrap();
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

        let new_record_location = self.serialize_and_write(&record)?;
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

    /// Open a new log file for writing to
    fn open_new_log_file(&mut self) -> Result<()> {
        self.log_file_counter = self.log_file_counter + 1;
        let new_log_path: PathBuf = [self.dirpath.clone(), PathBuf::from(format!("{}.log", self.log_file_counter))].iter().collect();

        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&new_log_path)?;

        println!("opened new log file = {:?}", &file);

        let reader = BufReader::new(file.try_clone()?);
        let writer = BufWriter::new(file.try_clone()?);

        self.log_files.insert(
            new_log_path.clone(),
            LogFile {
                reader,
                writer,
                file,
                path: new_log_path.clone(),
            }
        );

        self.log_file_paths.push(new_log_path.clone());
        self.active_log_path = Some(new_log_path);

        Ok(())
    }

    /// Compact oldest log entry
    fn compact(&mut self) -> Result<()> {
        let mut key_to_remove = None;

        if let Some(path_to_remove) = &self.log_file_paths.first() {
            let log_file = self.log_files.get_mut(path_to_remove.clone());

            if let Some(log_file) = log_file {
                let log_file_path = log_file.path.clone();
                let mut reader = BufReader::new(log_file.file.try_clone()?);
                reader.seek(SeekFrom::Start(0))?;

                while let Ok(decoded) = bson::decode_document(&mut reader) {
                    let bson_doc = bson::Bson::Document(decoded);

                    let record: Record = bson::from_bson(bson_doc)?;

                    if let Record::Set(key, record_value) = record {
                        let record_log_location = self.log_index.get(&key);

                        if let Some((path, _location)) = record_log_location {
                            if path == &log_file_path {
                                self.set(key, record_value)?;
                            }
                        }
                    }
                }

                key_to_remove = Some(log_file_path);
            }
        }

        if let Some(path) = key_to_remove {
            println!("removing file: {:?}", &path);
            self.log_files.remove(&path);
            fs::remove_file(&path)?;
            self.log_file_paths.retain(|x| x != &path);
        }

        Ok(())
    }

    /// Get the active log file, potentially opening a new one
    /// for writing to
    fn setup_active_log_file(&mut self) -> Result<()> {
        if let Some(active_log_path) = &self.active_log_path {
            let active_log = self.log_files.get(active_log_path);
            if let Some(log_file) = active_log {
                // If log_file length is greater than max file size, make new file
                if log_file.file.metadata()?.len() > 40480 {
                    println!("log file length greater than max file size");
                    self.open_new_log_file()?;
                } else {
                    println!("log file smaller than max file size");
                }
            } else {
                panic!("cant find active_log_path in self.log_files");
            }
        } else {
            println!("cant find active log path");
            self.open_new_log_file()?;
        }

        println!("after open_new_log_file, active_log_path = {:?}", &self.active_log_path);

        Ok(())
    }

    /// Serialize and write to log file
    /// Returns the location of the record that was written
    /// as a (log_file_path, location_in_file) tuple
    fn serialize_and_write(&mut self, record: &Record) -> Result<(PathBuf, u64)> {
        self.setup_active_log_file()?;
        // TODO: fix unwrap()
        let active_log_path = self.active_log_path.clone().unwrap();
        let log_file = self.log_files.get_mut(&active_log_path).unwrap();

        let new_record_location = log_file.writer.seek(SeekFrom::End(0))?;

        let serialized_record = bson::to_bson(record)?;
        // TODO: probably should error here if it doesn't properly parse the document thing??
        // And/or I should just be manually creating a bson document so I don't need that
        // to_bson call??
        if let Some(document) = serialized_record.as_document() {
            bson::encode_document(&mut log_file.writer, document)?;
            log_file.writer.flush()?;

            if self.log_files.len() > 3 {
                self.compact()?;
            }

            return Ok((active_log_path, new_record_location));
        }

        Err(KvStoreError::SerializationError(
            "Error serializing record".to_owned(),
        ))
    }
}
