#![deny(missing_docs)]

//! A Key Value Store!

extern crate serde;

#[macro_use(bson, doc)]
extern crate bson;

pub use kv::{KvStore, Result};

mod kv;
