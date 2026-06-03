//! Work-stealing scheduler: local queues, global injector, steal from peers.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use crate::config::PoolConfig;
use crate::error::PoolError;
use crate::pool::PoolMetrics;
use crate::task::{run_job_safely, Job};

pub(crate) struct StealBackend {
    workers: Vec<JoinHandle<()>>,
    injector: Arc<Mutex<VecDeque<Job>>>,
    local_queues: Vec<Arc<Mutex<VecDeque<Job>>>>,
    draining: Arc<AtomicBool>,
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
        }
    }

    pub fn submit(&self, job: Job) -> Result<(), PoolError> {
        if self.draining.load(Ordering::Relaxed) {
            return Err(PoolError::SubmitFailed);
        }
        self.injector
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push_back(job);
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
        queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push_back(job);
        Ok(())
    }

    pub fn shutdown(&mut self) -> Result<(), PoolError> {
        if self.workers.is_empty() {
            return Ok(());
        }
        self.draining.store(true, Ordering::Relaxed);
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
