use std::fmt;

/// Selects the scheduling algorithm used by [`SchedulitePool`](crate::SchedulitePool).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulerMode {
    /// Shared channel queue (Rust Book model).
    Fifo,
    /// Per-worker local queues with work-stealing.
    Steal,
}

impl fmt::Display for SchedulerMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Fifo => "fifo",
            Self::Steal => "steal",
        })
    }
}

/// Configuration for building a [`SchedulitePool`](crate::SchedulitePool).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolConfig {
    /// Number of worker threads (must be >= 1).
    pub workers: usize,
    /// Scheduling strategy.
    pub mode: SchedulerMode,
    /// Maximum queue depth per channel (FIFO) or per injector/local queue (Steal).
    /// `None` means unbounded.
    pub queue_capacity: Option<usize>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            workers: 4,
            mode: SchedulerMode::Fifo,
            queue_capacity: None,
        }
    }
}
