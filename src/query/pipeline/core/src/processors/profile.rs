use std::sync::atomic::AtomicU64;

#[derive(Default)]
pub struct Profile {
    /// The time spent to process in nanoseconds
    pub cpu_time: AtomicU64,
    /// The time spent to wait in nanoseconds, usually used to
    /// measure the time spent on waiting for I/O
    pub wait_time: AtomicU64,
}
