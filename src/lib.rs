#![deny(missing_docs)]

//! A Key Value Store!

pub use errors::Result;
pub use kv::KvStore;

mod errors;
mod kv;
