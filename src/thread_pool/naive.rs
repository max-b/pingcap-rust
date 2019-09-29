use crate::errors::Result;
use crate::thread_pool::ThreadPool;
use std::thread;

/// TODO: Documentation
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
