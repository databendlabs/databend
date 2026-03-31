// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::atomic::Ordering;
use std::sync::{atomic, LazyLock};
use std::time::Duration;

use crate::Result;

use futures::{Future, FutureExt};
use tokio::runtime::{Builder, Runtime};
use tracing::Span;

/// We cache the call to num_cpus::get() because:
///
/// 1. It shouldn't change during the lifetime of the program
/// 2. It's a relatively expensive call (requires opening several files and examining them)
static NUM_COMPUTE_INTENSIVE_CPUS: LazyLock<usize> =
    LazyLock::new(calculate_num_compute_intensive_cpus);

pub fn get_num_compute_intensive_cpus() -> usize {
    *NUM_COMPUTE_INTENSIVE_CPUS
}

fn calculate_num_compute_intensive_cpus() -> usize {
    if let Ok(user_specified) = std::env::var("LANCE_CPU_THREADS") {
        return user_specified.parse().unwrap();
    }

    let cpus = num_cpus::get();

    if cpus <= *IO_CORE_RESERVATION {
        // If the user is not setting a custom value for LANCE_IO_CORE_RESERVATION then we don't emit
        // a warning because they're just on a small machine and there isn't much they can do about it.
        if cpus > 2 {
            log::warn!(
                "Number of CPUs is less than or equal to the number of IO core reservations. \
                This is not a supported configuration. using 1 CPU for compute intensive tasks."
            );
        }
        return 1;
    }

    num_cpus::get() - *IO_CORE_RESERVATION
}

pub static IO_CORE_RESERVATION: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("LANCE_IO_CORE_RESERVATION")
        .unwrap_or("2".to_string())
        .parse()
        .unwrap()
});

fn create_runtime() -> Runtime {
    Builder::new_multi_thread()
        .thread_name("lance-cpu")
        .max_blocking_threads(get_num_compute_intensive_cpus())
        .worker_threads(1)
        // keep the thread alive "forever"
        .thread_keep_alive(Duration::from_secs(u64::MAX))
        .build()
        .unwrap()
}

static CPU_RUNTIME: atomic::AtomicPtr<Runtime> = atomic::AtomicPtr::new(std::ptr::null_mut());

static RUNTIME_INSTALLED: atomic::AtomicBool = atomic::AtomicBool::new(false);

static ATFORK_INSTALLED: atomic::AtomicBool = atomic::AtomicBool::new(false);

fn global_cpu_runtime() -> &'static mut Runtime {
    loop {
        let ptr = CPU_RUNTIME.load(Ordering::SeqCst);
        if !ptr.is_null() {
            return unsafe { &mut *ptr };
        }
        if !RUNTIME_INSTALLED.fetch_or(true, Ordering::SeqCst) {
            break;
        }
        std::thread::yield_now();
    }
    if !ATFORK_INSTALLED.fetch_or(true, Ordering::SeqCst) {
        install_atfork();
    }
    let new_ptr = Box::into_raw(Box::new(create_runtime()));
    CPU_RUNTIME.store(new_ptr, Ordering::SeqCst);
    unsafe { &mut *new_ptr }
}

/// After a fork() operation, force re-creation of the BackgroundExecutor. Note: this function
/// runs in "async-signal context" which means that we can't (safely) do much here.
extern "C" fn atfork_tokio_child() {
    CPU_RUNTIME.store(std::ptr::null_mut(), Ordering::SeqCst);
    RUNTIME_INSTALLED.store(false, Ordering::SeqCst);
}

#[cfg(not(windows))]
fn install_atfork() {
    unsafe { libc::pthread_atfork(None, None, Some(atfork_tokio_child)) };
}

#[cfg(windows)]
fn install_atfork() {}

/// Spawn a CPU intensive task
///
/// This task will be put onto a thread pool dedicated for CPU-intensive work
/// This keeps the tokio thread pool free so that we can always be ready to service
/// cheap I/O & control requests.
///
/// This can also be used to convert a big chunk of synchronous work into a future
/// so that it can be run in parallel with something like StreamExt::buffered()
pub fn spawn_cpu<F: FnOnce() -> Result<R> + Send + 'static, R: Send + 'static>(
    func: F,
) -> impl Future<Output = Result<R>> {
    let (send, recv) = tokio::sync::oneshot::channel();
    // Propagate the current span into the task
    let span = Span::current();
    global_cpu_runtime().spawn_blocking(move || {
        let _span_guard = span.enter();
        let result = func();
        let _ = send.send(result);
    });
    recv.map(|res| res.unwrap())
}
