use std::error::Error;
use std::fmt;
use std::io;
use std::result;
use sled;

#[derive(Debug)]
pub enum KvStoreError {
    Io(io::Error),
    EncoderError(bson::EncoderError),
    DecoderError(bson::DecoderError),
    SledError(sled::Error),
    NonExistentKeyError(String),
    SerializationError(String),
    DatabaseInitializationError(String),
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

impl From<sled::Error> for KvStoreError {
    fn from(err: sled::Error) -> KvStoreError {
        KvStoreError::SledError(err)
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
            KvStoreError::SledError(err) => err.description(),
            KvStoreError::NonExistentKeyError(string) => string,
            KvStoreError::SerializationError(string) => string,
            KvStoreError::DatabaseInitializationError(string) => string,
        }
    }

    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            KvStoreError::Io(err) => Some(err),
            KvStoreError::EncoderError(err) => Some(err),
            KvStoreError::DecoderError(err) => Some(err),
            KvStoreError::SledError(err) => Some(err),
            KvStoreError::NonExistentKeyError(_) => None,
            KvStoreError::SerializationError(_) => None,
            KvStoreError::DatabaseInitializationError(_) => None,
        }
    }
}

/// A KvStore result that wraps KvStoreError
pub type Result<T> = result::Result<T, KvStoreError>;
