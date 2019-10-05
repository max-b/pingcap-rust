use sled;
use std::error::Error;
use std::fmt;
use std::io;
use std::result;

#[derive(Debug)]
pub enum KvStoreError {
    Io(io::Error),
    EncoderError(bson::EncoderError),
    DecoderError(bson::DecoderError),
    SledError(sled::Error),
    NonExistentKeyError(String),
    SerializationError(String),
    LockError(String),
    ClientError(String),
}

impl From<KvStoreError> for io::Error {
    fn from(err: KvStoreError) -> Self {
        match err {
            KvStoreError::Io(err) => err,
            KvStoreError::EncoderError(err) => io::Error::new(
                io::ErrorKind::Other,
                err.description(),
            ),
            KvStoreError::DecoderError(err) => io::Error::new(
                io::ErrorKind::Other,
                err.description(),
            ),
            KvStoreError::SledError(err) => io::Error::new(
                io::ErrorKind::Other,
                err.description(),
            ),
            KvStoreError::NonExistentKeyError(err) => io::Error::new(
                io::ErrorKind::Other,
                err,
            ),
            KvStoreError::SerializationError(err) => io::Error::new(
                io::ErrorKind::Other,
                err,
            ),
            KvStoreError::LockError(err) => io::Error::new(
                io::ErrorKind::Other,
                err,
            ),
            KvStoreError::ClientError(err) => io::Error::new(
                io::ErrorKind::Other,
                err,
            ),
        }
    }
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
        write!(f, "{}", self.description())
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
            KvStoreError::LockError(string) => string,
            KvStoreError::ClientError(string) => string,
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
            KvStoreError::LockError(_) => None,
            KvStoreError::ClientError(_) => None,
        }
    }
}

/// A KvStore result that wraps KvStoreError
pub type Result<T> = result::Result<T, KvStoreError>;
