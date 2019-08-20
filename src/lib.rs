#![deny(missing_docs)]

//! A Key Value Store!

pub use kv::{KvStore};
pub use errors::{Result};

mod kv;
mod errors;
