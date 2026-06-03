//! Scheduler backends.

mod fifo;

use std::sync::Arc;

use crate::config::SchedulerMode;
use crate::error::PoolError;
use crate::pool::PoolMetrics;
use crate::task::Job;

pub(crate) use fifo::FifoBackend;

pub(crate) enum PoolBackend {
    Fifo(FifoBackend),
}

impl PoolBackend {
    pub fn new(mode: SchedulerMode, size: usize, metrics: Arc<PoolMetrics>) -> Self {
        match mode {
            SchedulerMode::Fifo => Self::Fifo(FifoBackend::new(size, metrics)),
            SchedulerMode::Steal => unimplemented!("steal mode not yet implemented"),
        }
    }

    pub fn submit(&self, job: Job) -> Result<(), PoolError> {
        match self {
            Self::Fifo(b) => b.submit(job),
        }
    }

    pub fn shutdown(&mut self) -> Result<(), PoolError> {
        match self {
            Self::Fifo(b) => b.shutdown(),
        }
    }
}
