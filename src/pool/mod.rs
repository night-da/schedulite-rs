//! Thread pool public API.

mod metrics;

use std::sync::Arc;

pub use metrics::{PoolMetrics, PoolMetricsSnapshot};

use crate::config::SchedulerMode;
use crate::error::PoolError;
use crate::scheduler::PoolBackend;
use crate::task::Job;

/// Configurable thread pool with FIFO or work-stealing scheduling.
pub struct SchedulitePool {
    mode: SchedulerMode,
    metrics: Arc<PoolMetrics>,
    backend: PoolBackend,
}

impl SchedulitePool {
    /// Creates a FIFO pool with `size` worker threads.
    pub fn new(size: usize) -> Self {
        Self::with_mode(size, SchedulerMode::Fifo)
    }

    /// Creates a pool with the given scheduler mode.
    ///
    /// # Panics
    ///
    /// Panics if `size` is zero.
    pub fn with_mode(size: usize, mode: SchedulerMode) -> Self {
        assert!(size > 0, "pool size must be greater than zero");
        let metrics = Arc::new(PoolMetrics::default());
        let backend = PoolBackend::new(mode, size, Arc::clone(&metrics));
        Self {
            mode,
            metrics,
            backend,
        }
    }

    pub fn mode(&self) -> SchedulerMode {
        self.mode
    }

    /// Enqueues a job on the global queue.
    pub fn submit<F>(&self, f: F) -> Result<(), PoolError>
    where
        F: FnOnce() + Send + 'static,
    {
        self.backend.submit(Box::new(f))?;
        self.metrics.record_submitted();
        Ok(())
    }

    /// Enqueues a job on a worker's local queue (steal mode only).
    /// In FIFO mode this falls back to the shared channel.
    pub fn submit_to_worker<F>(&self, worker_id: usize, f: F) -> Result<(), PoolError>
    where
        F: FnOnce() + Send + 'static,
    {
        let job: Job = Box::new(f);
        self.backend.submit_to_worker(worker_id, job)?;
        self.metrics.record_submitted();
        Ok(())
    }

    /// Returns execution counters.
    pub fn metrics_snapshot(&self) -> PoolMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Stops accepting new jobs and joins all worker threads. Idempotent.
    pub fn shutdown(&mut self) -> Result<(), PoolError> {
        self.backend.shutdown()
    }
}

impl Drop for SchedulitePool {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

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

    fn run_counter_pool(mode: SchedulerMode, workers: usize, tasks: usize) -> u64 {
        let mut pool = SchedulitePool::with_mode(workers, mode);
        let counter = Arc::new(Mutex::new(0));
        for _ in 0..tasks {
            let counter = Arc::clone(&counter);
            pool.submit(move || {
                thread::sleep(Duration::from_millis(1));
                *counter.lock().unwrap() += 1;
            })
            .unwrap();
        }
        pool.shutdown().unwrap();
        *counter.lock().unwrap()
    }

    #[test]
    fn fifo_vs_steal_produce_same_results() {
        let fifo = run_counter_pool(SchedulerMode::Fifo, 4, 40);
        let steal = run_counter_pool(SchedulerMode::Steal, 4, 40);
        assert_eq!(fifo, 40);
        assert_eq!(steal, 40);
    }

    #[test]
    fn steal_handles_skewed_local_submissions() {
        let mut pool = SchedulitePool::with_mode(4, SchedulerMode::Steal);
        let counter = Arc::new(Mutex::new(0));
        for i in 0..400 {
            let counter = Arc::clone(&counter);
            let job = move || {
                for _ in 0..200 {
                    *counter.lock().unwrap() += 1;
                }
            };
            if i < 360 {
                pool.submit_to_worker(0, job).unwrap();
            } else {
                pool.submit(job).unwrap();
            }
        }
        pool.shutdown().unwrap();
        let m = pool.metrics_snapshot();
        assert_eq!(*counter.lock().unwrap(), 400 * 200);
        assert_eq!(m.completed, 400);
        assert!(m.stolen > 0);
    }
}
