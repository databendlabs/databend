use std::sync::atomic::AtomicU64;

#[derive(Default)]
pub struct Profile {
    /// The id of processor
    pub pid: usize,
    /// The name of processor
    pub p_name: String,

    /// The time spent to process in nanoseconds
    pub cpu_time: AtomicU64,
    /// The time spent to wait in nanoseconds, usually used to
    /// measure the time spent on waiting for I/O
    pub wait_time: AtomicU64,
}

impl Profile {
    pub fn create(pid: usize, p_name: String) -> Profile {
        Profile {
            pid,
            p_name,
            cpu_time: AtomicU64::new(0),
            wait_time: AtomicU64::new(0),
        }
    }
}
