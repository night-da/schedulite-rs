/// Selects the scheduling algorithm used by [`SchedulitePool`](crate::SchedulitePool).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulerMode {
    /// Shared channel queue (Rust Book model).
    Fifo,
    /// Per-worker local queues with work-stealing.
    Steal,
}
