#![deny(missing_docs)]
#![feature(result_map_or_else)]

//! A Key Value Store!

pub use crate::sled::SledKvsEngine;
pub use client::{Command, KvsClient};
pub use errors::Result;
pub use kv::KvsEngine;
pub use server::KvsServer;
pub use store::KvStore;
pub use thread_pool::{NaiveThreadPool, RayonThreadPool, SharedQueueThreadPool, ThreadPool};

/// A Thread Pool module which contains both a pluggable ThreadPool trait
/// as well as implementations of it
pub mod thread_pool;

mod client;
mod errors;
mod kv;
mod server;
mod sled;
mod store;
