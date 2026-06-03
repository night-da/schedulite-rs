//! FIFO scheduler: shared `mpsc` channel and `Message::Terminate` shutdown.

use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};

use crate::config::PoolConfig;
use crate::error::PoolError;
use crate::pool::PoolMetrics;
use crate::task::{run_job_safely, Job};

enum Message {
    NewJob(Job),
    Terminate,
}

impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NewJob(_) => f.debug_tuple("NewJob").finish(),
            Self::Terminate => write!(f, "Terminate"),
        }
    }
}

#[derive(Debug)]
enum Sender {
    Unbounded(mpsc::Sender<Message>),
    Bounded(mpsc::SyncSender<Message>),
}

impl Sender {
    fn try_send(&self, msg: Message) -> Result<(), PoolError> {
        match self {
            Self::Unbounded(s) => s.send(msg).map_err(|_| PoolError::SubmitFailed),
            Self::Bounded(s) => match s.try_send(msg) {
                Ok(()) => Ok(()),
                Err(mpsc::TrySendError::Full(_)) => Err(PoolError::QueueFull),
                Err(mpsc::TrySendError::Disconnected(_)) => Err(PoolError::SubmitFailed),
            },
        }
    }

    fn send(&self, msg: Message) -> Result<(), PoolError> {
        match self {
            Self::Unbounded(s) => s.send(msg),
            Self::Bounded(s) => s.send(msg),
        }
        .map_err(|_| PoolError::ShutdownFailed)
    }
}

#[derive(Debug)]
pub(crate) struct FifoBackend {
    workers: Vec<JoinHandle<()>>,
    sender: Option<Sender>,
}

impl FifoBackend {
    pub fn new(config: &PoolConfig, metrics: Arc<PoolMetrics>) -> Self {
        let (sender, receiver) = if let Some(cap) = config.queue_capacity {
            let (tx, rx) = mpsc::sync_channel(cap);
            (Sender::Bounded(tx), rx)
        } else {
            let (tx, rx) = mpsc::channel();
            (Sender::Unbounded(tx), rx)
        };
        let receiver = Arc::new(Mutex::new(receiver));

        let workers = (0..config.workers)
            .map(|_| {
                let receiver = Arc::clone(&receiver);
                let metrics = Arc::clone(&metrics);
                thread::spawn(move || worker_loop(receiver, metrics))
            })
            .collect();

        Self {
            workers,
            sender: Some(sender),
        }
    }

    pub fn submit(&self, job: Job) -> Result<(), PoolError> {
        self.sender
            .as_ref()
            .ok_or(PoolError::SubmitFailed)?
            .try_send(Message::NewJob(job))
    }

    pub fn shutdown(&mut self) -> Result<(), PoolError> {
        let Some(sender) = self.sender.take() else {
            return Ok(());
        };
        for _ in 0..self.workers.len() {
            sender.send(Message::Terminate)?;
        }
        drop(sender);
        for handle in self.workers.drain(..) {
            handle.join().map_err(|_| PoolError::ShutdownFailed)?;
        }
        Ok(())
    }
}

fn worker_loop(receiver: Arc<Mutex<mpsc::Receiver<Message>>>, metrics: Arc<PoolMetrics>) {
    loop {
        let message = {
            let receiver = receiver.lock().unwrap_or_else(|e| e.into_inner());
            receiver.recv()
        };
        match message {
            Ok(Message::NewJob(job)) => {
                let _ = run_job_safely(job, &metrics);
            }
            Ok(Message::Terminate) | Err(_) => break,
        }
    }
}
