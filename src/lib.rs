//! Schedulite: lightweight FIFO / work-stealing thread pool.

use std::error::Error;
use std::fmt;
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};

// --- Error ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolError {
    SubmitFailed,
    ShutdownFailed,
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            PoolError::SubmitFailed => "failed to submit job",
            PoolError::ShutdownFailed => "failed to shutdown pool",
        })
    }
}

impl Error for PoolError {}

// --- Task ---

type Job = Box<dyn FnOnce() + Send + 'static>;

enum Message {
    NewJob(Job),
    Terminate,
}

// --- Pool ---

pub struct SchedulitePool {
    workers: Vec<JoinHandle<()>>,
    sender: Option<mpsc::Sender<Message>>,
}

impl SchedulitePool {
    pub fn new(size: usize) -> Self {
        assert!(size > 0, "pool size must be greater than zero");
        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));

        let workers = (0..size)
            .map(|_| {
                let receiver = Arc::clone(&receiver);
                thread::spawn(move || worker_loop(receiver))
            })
            .collect();

        Self {
            workers,
            sender: Some(sender),
        }
    }

    pub fn submit<F>(&self, f: F) -> Result<(), PoolError>
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender
            .as_ref()
            .ok_or(PoolError::SubmitFailed)?
            .send(Message::NewJob(Box::new(f)))
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

impl Drop for SchedulitePool {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn worker_loop(receiver: Arc<Mutex<mpsc::Receiver<Message>>>) {
    loop {
        let message = {
            let receiver = receiver.lock().unwrap_or_else(|e| e.into_inner());
            receiver.recv()
        };
        match message {
            Ok(Message::NewJob(job)) => {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
            }
            Ok(Message::Terminate) | Err(_) => break,
        }
    }
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn submit_and_shutdown() {
        let mut pool = SchedulitePool::new(2);
        let counter = Arc::new(Mutex::new(0));
        for _ in 0..10 {
            let counter = Arc::clone(&counter);
            pool.submit(move || *counter.lock().unwrap() += 1).unwrap();
        }
        pool.shutdown().unwrap();
        assert_eq!(*counter.lock().unwrap(), 10);
    }

    #[test]
    fn shutdown_waits_for_in_flight_jobs() {
        let mut pool = SchedulitePool::new(2);
        let counter = Arc::new(Mutex::new(0));
        for _ in 0..4 {
            let counter = Arc::clone(&counter);
            pool.submit(move || {
                thread::sleep(Duration::from_millis(20));
                *counter.lock().unwrap() += 1;
            })
            .unwrap();
        }
        pool.shutdown().unwrap();
        assert_eq!(*counter.lock().unwrap(), 4);
    }

    #[test]
    fn submit_fails_after_shutdown() {
        let mut pool = SchedulitePool::new(1);
        pool.shutdown().unwrap();
        assert_eq!(pool.submit(|| {}), Err(PoolError::SubmitFailed));
    }

    #[test]
    fn shutdown_is_idempotent() {
        let mut pool = SchedulitePool::new(1);
        pool.shutdown().unwrap();
        pool.shutdown().unwrap();
    }
}
