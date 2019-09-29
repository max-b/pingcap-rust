use crate::errors::Result;
use crate::thread_pool::ThreadPool;
use rayon;

/// TODO: Documentation
pub struct RayonThreadPool(rayon::ThreadPool);

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads as usize)
            .build()
            .expect("failed to create a thread pool");
        Ok(Self(pool))
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.0.install(job);
    }
}
