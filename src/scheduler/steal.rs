//! Work-stealing scheduler: local queues, global injector, steal from peers.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::config::PoolConfig;
use crate::error::PoolError;
use crate::pool::PoolMetrics;
use crate::task::{run_job_safely, Job};

pub(crate) struct StealBackend {
    workers: Vec<JoinHandle<()>>,
    injector: Arc<Mutex<VecDeque<Job>>>,
    local_queues: Vec<Arc<Mutex<VecDeque<Job>>>>,
    draining: Arc<AtomicBool>,
    queue_capacity: Option<usize>,
}

impl std::fmt::Debug for StealBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StealBackend")
            .field("workers", &self.workers)
            .field(
                "injector_len",
                &self.injector.lock().map(|q| q.len()).unwrap_or(0),
            )
            .field("local_queues", &self.local_queues.len())
            .field("draining", &self.draining)
            .field("queue_capacity", &self.queue_capacity)
            .finish()
    }
}

impl StealBackend {
    pub fn new(config: &PoolConfig, metrics: Arc<PoolMetrics>) -> Self {
        let injector = Arc::new(Mutex::new(VecDeque::new()));
        let local_queues: Vec<_> = (0..config.workers)
            .map(|_| Arc::new(Mutex::new(VecDeque::new())))
            .collect();
        let all_locals = Arc::new(local_queues.clone());
        let draining = Arc::new(AtomicBool::new(false));

        let workers = (0..config.workers)
            .map(|worker_id| {
                let local = Arc::clone(&local_queues[worker_id]);
                let injector = Arc::clone(&injector);
                let all_locals = Arc::clone(&all_locals);
                let draining = Arc::clone(&draining);
                let metrics = Arc::clone(&metrics);
                thread::spawn(move || {
                    worker_loop(worker_id, local, injector, all_locals, draining, metrics)
                })
            })
            .collect();

        Self {
            workers,
            injector,
            local_queues,
            draining,
            queue_capacity: config.queue_capacity,
        }
    }

    fn queue_full(&self, queue: &VecDeque<Job>) -> bool {
        if let Some(cap) = self.queue_capacity {
            queue.len() >= cap
        } else {
            false
        }
    }

    pub fn submit(&self, job: Job) -> Result<(), PoolError> {
        if self.draining.load(Ordering::Relaxed) {
            return Err(PoolError::SubmitFailed);
        }
        let mut guard = self.injector.lock().unwrap_or_else(|e| e.into_inner());
        if self.queue_full(&guard) {
            return Err(PoolError::QueueFull);
        }
        guard.push_back(job);
        Ok(())
    }

    pub fn submit_to_worker(&self, worker_id: usize, job: Job) -> Result<(), PoolError> {
        if self.draining.load(Ordering::Relaxed) {
            return Err(PoolError::SubmitFailed);
        }
        let queue = self
            .local_queues
            .get(worker_id)
            .ok_or(PoolError::SubmitFailed)?;
        let mut guard = queue.lock().unwrap_or_else(|e| e.into_inner());
        if self.queue_full(&guard) {
            return Err(PoolError::QueueFull);
        }
        guard.push_back(job);
        Ok(())
    }

    pub fn shutdown(&mut self) -> Result<(), PoolError> {
        self.signal_drain();
        self.join_workers()
    }

    pub fn shutdown_timeout(&mut self, timeout: Duration) -> Result<(), PoolError> {
        self.signal_drain();

        let deadline = Instant::now() + timeout;
        loop {
            if self.workers.iter().all(|h| h.is_finished()) {
                return self.join_workers();
            }
            if Instant::now() >= deadline {
                return Err(PoolError::ShutdownTimeout);
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    fn signal_drain(&mut self) {
        self.draining.store(true, Ordering::Relaxed);
    }

    fn join_workers(&mut self) -> Result<(), PoolError> {
        for handle in self.workers.drain(..) {
            handle.join().map_err(|_| PoolError::ShutdownFailed)?;
        }
        Ok(())
    }
}

fn worker_loop(
    worker_id: usize,
    local: Arc<Mutex<VecDeque<Job>>>,
    injector: Arc<Mutex<VecDeque<Job>>>,
    all_locals: Arc<Vec<Arc<Mutex<VecDeque<Job>>>>>,
    draining: Arc<AtomicBool>,
    metrics: Arc<PoolMetrics>,
) {
    loop {
        if let Some(job) = take_job(worker_id, &local, &injector, &all_locals, &metrics) {
            let _ = run_job_safely(job, &metrics);
            continue;
        }

        if draining.load(Ordering::Relaxed) {
            break;
        }

        thread::yield_now();
    }
}

fn take_job(
    worker_id: usize,
    local: &Mutex<VecDeque<Job>>,
    injector: &Mutex<VecDeque<Job>>,
    all_locals: &[Arc<Mutex<VecDeque<Job>>>],
    metrics: &PoolMetrics,
) -> Option<Job> {
    if let Some(job) = local.lock().unwrap_or_else(|e| e.into_inner()).pop_front() {
        return Some(job);
    }

    if let Some(job) = injector
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .pop_front()
    {
        return Some(job);
    }

    for (id, queue) in all_locals.iter().enumerate() {
        if id == worker_id {
            continue;
        }
        if let Ok(mut guard) = queue.try_lock()
            && let Some(job) = guard.pop_back()
        {
            metrics.record_stolen();
            return Some(job);
        }
    }

    None
}
