use std::iter;
use std::thread;
use std::sync::Arc;
use crossbeam::deque::{
    Injector as InjectorQueue, 
    Stealer,
    Worker as WorkerQueue
};
use crossbeam::crossbeam_channel::{Sender, Receiver, unbounded};
use crate::errors::Result;
use crate::thread_pool::ThreadPool;

type BoxedFunc = Box<dyn FnOnce() + Send + 'static>;

enum Message {
    Terminate,
    AddStealer(Stealer<BoxedFunc>),
    AddSender(Sender<Message>)
}

fn find_task(
    local: &WorkerQueue<BoxedFunc>,
    shared_global: &Arc<InjectorQueue<BoxedFunc>>,
    stealers: &[Stealer<BoxedFunc>],
) -> Option<BoxedFunc> {
    // Pop a task from the local queue, if not empty.
    local.pop().or_else(|| {
        // Otherwise, we need to look for a task elsewhere.
        iter::repeat_with(|| {
            // Try stealing a batch of tasks from the global queue.
            shared_global.steal_batch_and_pop(local)
                // Or try stealing a task from one of the other threads.
                .or_else(|| stealers.iter().map(|s| s.steal()).collect())
        })
        // Loop while no task was stolen and any steal operation needs to be retried.
        .find(|s| !s.is_retry())
        // Extract the stolen task, if there is one.
        .and_then(|s| s.success())
    })
}

#[derive(Debug)]
struct Worker {
    receiver: Receiver<Message>,
    local: WorkerQueue<BoxedFunc>,
    global: Arc<InjectorQueue<BoxedFunc>>,
    stealers: Vec<Stealer<BoxedFunc>>,
    senders: Vec<Sender<Message>>
}

impl Worker {
    pub fn new(receiver: Receiver<Message>, global: Arc<InjectorQueue<BoxedFunc>>) -> Self {
        let local = WorkerQueue::<BoxedFunc>::new_fifo();
        let stealers: Vec<Stealer<BoxedFunc>> = Vec::new();
        let senders: Vec<Sender<Message>> = Vec::new();

        let worker = Self {
            receiver,
            global,
            local,
            stealers,
            senders
        };

        worker
    }

    pub fn start(mut self) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            loop {
                match self.receiver.try_recv() {
                    Ok(message) => {
                        match message {
                            Message::Terminate => {
                                break;
                            },
                            Message::AddStealer(stealer) => {
                                self.stealers.push(stealer);
                            },
                            Message::AddSender(sender) => {
                                self.senders.push(sender);
                            }
                        }
                    },
                    Err(_) => {} // No message available yet
                };

                match find_task(&self.local, &self.global, &self.stealers) {
                    Some(f) => { 
                        f();
                    },
                    None => {}
                }
            }
        })
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if thread::panicking() {
            let local = WorkerQueue::<BoxedFunc>::new_fifo();
            // Need to move all tasks from panicking queue to new worker queue
            loop {
                match self.local.pop() {
                    Some(t) => {
                        local.push(t);
                    },
                    None => { break; }
                }
            }

            let new_stealer = local.stealer();
            let mut stealers = self.stealers.clone();

            for sender in &self.senders {
                sender.send(Message::AddStealer(new_stealer.clone())).expect("send failed");
            }

            stealers.push(new_stealer);

            let worker = Worker {
                local,
                senders: self.senders.clone(),
                receiver: self.receiver.clone(),
                global: self.global.clone(),
                stealers
            };

            worker.start();
        }
    }
}

/// TODO: Documentation
pub struct SharedQueueThreadPool {
    shared_injector: Arc<InjectorQueue<BoxedFunc>>,
    senders: Vec<Sender<Message>>,
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        for sender in &self.senders {
            sender.send(Message::Terminate).expect("failed sending message");
        }
    }
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let mut stealers = Vec::new();
        let mut senders = Vec::new();
        let mut workers = Vec::new();
        let shared_injector = Arc::new(InjectorQueue::<BoxedFunc>::new());

        for _i in 0..threads {
            let (sender, receiver) = unbounded();
            let worker = Worker::new(receiver, shared_injector.clone());
            stealers.push(worker.local.stealer());
            senders.push(sender);
            workers.push(worker);
        }

        for sender in &senders {
            sender.send(Message::AddSender(sender.clone())).expect("send failed");
            for stealer in &stealers {
                sender.send(Message::AddStealer(stealer.clone())).expect("send failed");
            }
        }

        for worker in workers {
            worker.start();
        }


        Ok(Self{ shared_injector, senders })
    }

    fn spawn<T>(&self, job: T) where T: FnOnce() + Send + 'static {
        self.shared_injector.push(Box::new(job));
    }
}
