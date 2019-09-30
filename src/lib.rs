#![deny(missing_docs)]
#![feature(result_map_or_else)]

//! A Key Value Store!

pub use errors::Result;
pub use kv::KvsEngine;
pub use store::KvStore;
pub use server::KvsServer;
pub use crate::sled::SledKvsEngine;
pub use thread_pool::{NaiveThreadPool, RayonThreadPool, SharedQueueThreadPool, ThreadPool};

/// TODO: documentation
pub mod thread_pool;

mod errors;
mod kv;
mod store;
mod server;
mod sled;
