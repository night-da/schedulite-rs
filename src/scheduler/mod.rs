//! Scheduler backends selected by [`SchedulerMode`](crate::config::SchedulerMode).

mod fifo;
mod steal;

use std::sync::Arc;

use crate::config::SchedulerMode;
use crate::error::PoolError;
use crate::pool::PoolMetrics;
use crate::task::Job;

pub(crate) use fifo::FifoBackend;
pub(crate) use steal::StealBackend;

pub(crate) enum PoolBackend {
    Fifo(FifoBackend),
    Steal(StealBackend),
}

impl PoolBackend {
    pub fn new(mode: SchedulerMode, size: usize, metrics: Arc<PoolMetrics>) -> Self {
        match mode {
            SchedulerMode::Fifo => Self::Fifo(FifoBackend::new(size, metrics)),
            SchedulerMode::Steal => Self::Steal(StealBackend::new(size, metrics)),
        }
    }

    pub fn submit(&self, job: Job) -> Result<(), PoolError> {
        match self {
            Self::Fifo(b) => b.submit(job),
            Self::Steal(b) => b.submit(job),
        }
    }

    pub fn submit_to_worker(&self, worker_id: usize, job: Job) -> Result<(), PoolError> {
        match self {
            Self::Fifo(b) => b.submit(job),
            Self::Steal(b) => b.submit_to_worker(worker_id, job),
        }
    }

    pub fn shutdown(&mut self) -> Result<(), PoolError> {
        match self {
            Self::Fifo(b) => b.shutdown(),
            Self::Steal(b) => b.shutdown(),
        }
    }
}
