//! Schedulite: lightweight FIFO / work-stealing thread pool.

use std::error::Error;
use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskOutcome {
    Completed,
    Panicked,
}

// --- Metrics ---

#[derive(Debug, Default)]
pub struct PoolMetrics {
    submitted: AtomicU64,
    completed: AtomicU64,
    panicked: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolMetricsSnapshot {
    pub submitted: u64,
    pub completed: u64,
    pub panicked: u64,
}

impl PoolMetrics {
    fn record_submitted(&self) {
        self.submitted.fetch_add(1, Ordering::Relaxed);
    }
    fn record_completed(&self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
    }
    fn record_panicked(&self) {
        self.panicked.fetch_add(1, Ordering::Relaxed);
    }
    pub fn snapshot(&self) -> PoolMetricsSnapshot {
        PoolMetricsSnapshot {
            submitted: self.submitted.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            panicked: self.panicked.load(Ordering::Relaxed),
        }
    }
}

// --- Message ---

enum Message {
    NewJob(Job),
    Terminate,
}

// --- Pool ---

pub struct SchedulitePool {
    workers: Vec<JoinHandle<()>>,
    sender: Option<mpsc::Sender<Message>>,
    metrics: Arc<PoolMetrics>,
}

impl SchedulitePool {
    pub fn new(size: usize) -> Self {
        assert!(size > 0, "pool size must be greater than zero");
        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        let metrics = Arc::new(PoolMetrics::default());

        let workers = (0..size)
            .map(|_| {
                let receiver = Arc::clone(&receiver);
                let metrics = Arc::clone(&metrics);
                thread::spawn(move || worker_loop(receiver, metrics))
            })
            .collect();

        Self {
            workers,
            sender: Some(sender),
            metrics,
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
            .map_err(|_| PoolError::SubmitFailed)?;
        self.metrics.record_submitted();
        Ok(())
    }

    pub fn metrics_snapshot(&self) -> PoolMetricsSnapshot {
        self.metrics.snapshot()
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

fn run_job_safely(job: Job, metrics: &PoolMetrics) -> TaskOutcome {
    match catch_unwind(AssertUnwindSafe(job)) {
        Ok(()) => {
            metrics.record_completed();
            TaskOutcome::Completed
        }
        Err(_) => {
            metrics.record_panicked();
            TaskOutcome::Panicked
        }
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
        let m = pool.metrics_snapshot();
        assert_eq!(*counter.lock().unwrap(), 10);
        assert_eq!(m.submitted, 10);
        assert_eq!(m.completed, 10);
        assert_eq!(m.panicked, 0);
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

    #[test]
    fn panic_isolation() {
        let mut pool = SchedulitePool::new(2);
        for i in 0..100 {
            pool.submit(move || {
                if i % 10 == 0 {
                    panic!("intentional panic in task {i}");
                }
            })
            .unwrap();
        }
        pool.shutdown().unwrap();
        let m = pool.metrics_snapshot();
        assert_eq!(m.submitted, 100);
        assert_eq!(m.completed, 90);
        assert_eq!(m.panicked, 10);
        assert_eq!(m.completed + m.panicked, m.submitted);
    }
}
