#![deny(missing_docs)]
#![feature(result_map_or_else)]

//! A Key Value Store!

pub use errors::Result;
pub use kv::KvsEngine;
pub use kv_store::KvStore;
pub use kvs_server::KvsServer;
pub use kvs_sled::SledKvsEngine;

mod errors;
mod kv;
mod kv_store;
mod kvs_server;
mod kvs_sled;
