use crate::errors::Result;
use crate::thread_pool::ThreadPool;
use crossbeam::crossbeam_channel::{unbounded, Receiver, Sender};
use std::thread;

type BoxedFunc = Box<dyn FnOnce() + Send + 'static>;

enum Message {
    Job(BoxedFunc),
}

#[derive(Debug)]
struct Worker {
    receiver: Receiver<Message>,
}

impl Worker {
    pub fn new(receiver: Receiver<Message>) -> Self {
        Self { receiver }
    }

    pub fn start(self) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            while let Ok(message) = self.receiver.recv() {
                match message {
                    Message::Job(job) => {
                        job();
                    }
                }
            }
        })
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if thread::panicking() {
            let worker = Worker {
                receiver: self.receiver.clone(),
            };
            worker.start();
        }
    }
}

/// TODO: Documentation
pub struct SharedQueueThreadPool {
    sender: Sender<Message>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let (sender, receiver) = unbounded();

        for _i in 0..threads {
            let worker = Worker::new(receiver.clone());
            worker.start();
        }

        Ok(Self { sender })
    }

    fn spawn<T>(&self, job: T)
    where
        T: FnOnce() + Send + 'static,
    {
        self.sender
            .send(Message::Job(Box::new(job)))
            .expect("failed sending message");
    }
}
