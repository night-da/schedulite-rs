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

pub(crate) struct FifoBackend {
    workers: Vec<JoinHandle<()>>,
    sender: Option<mpsc::Sender<Message>>,
}

impl FifoBackend {
    pub fn new(config: &PoolConfig, metrics: Arc<PoolMetrics>) -> Self {
        let (sender, receiver) = mpsc::channel();
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
            .send(Message::NewJob(job))
            .map_err(|_| PoolError::SubmitFailed)
    }

    pub fn shutdown(&mut self) -> Result<(), PoolError> {
        let Some(sender) = self.sender.take() else {
            return Ok(());
        };
        for _ in 0..self.workers.len() {
            sender
                .send(Message::Terminate)
                .map_err(|_| PoolError::ShutdownFailed)?;
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
