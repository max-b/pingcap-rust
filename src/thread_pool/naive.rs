use crate::errors::Result;
use crate::thread_pool::ThreadPool;
use std::thread;

/// A naive thread pool which simply spawns a 
/// new thread every time `spawn` is called
pub struct NaiveThreadPool {}

impl ThreadPool for NaiveThreadPool {
    fn new(_threads: u32) -> Result<Self> {
        Ok(Self {})
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let _handle = thread::spawn(job);
    }
}
