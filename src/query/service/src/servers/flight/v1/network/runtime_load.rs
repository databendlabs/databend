// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Process-wide load over the lifetime of an exchange, for its summary logs.
//!
//! The figures cover everything the process ran meanwhile, not only the
//! exchange, so they tell whether a shared resource was saturated: the CPU, the
//! runtime polling the HTTP/2 connections, or the IO runtime running the
//! exchange tasks.

use std::fmt;
use std::time::Duration;
use std::time::Instant;

use databend_common_base::runtime::GlobalIORuntime;
use tokio::runtime::Handle;

/// Load counters at one point in time.
pub struct LoadSnapshot {
    at: Instant,
    process_cpu: Duration,
    io_runtime: RuntimeBusy,
    /// The runtime polling the HTTP/2 connections, when it is not the IO runtime.
    h2_runtime: Option<(Handle, RuntimeBusy)>,
}

#[derive(Clone, Copy)]
struct RuntimeBusy {
    workers: usize,
    busy: Duration,
}

impl RuntimeBusy {
    fn capture(handle: &Handle) -> Self {
        let metrics = handle.metrics();
        let workers = metrics.num_workers();
        let busy = (0..workers)
            .map(|worker| metrics.worker_total_busy_duration(worker))
            .sum();
        RuntimeBusy { workers, busy }
    }
}

impl LoadSnapshot {
    /// `h2_runtime` is the runtime the connections were created on.
    pub fn capture(h2_runtime: Option<Handle>) -> Self {
        let io = GlobalIORuntime::instance().inner();
        let h2_runtime = h2_runtime
            .filter(|handle| handle.id() != io.id())
            .map(|handle| {
                let busy = RuntimeBusy::capture(&handle);
                (handle, busy)
            });
        LoadSnapshot {
            at: Instant::now(),
            process_cpu: process_cpu_time(),
            io_runtime: RuntimeBusy::capture(&io),
            h2_runtime,
        }
    }

    /// The load from this snapshot until now.
    pub fn elapsed(&self) -> LoadDelta {
        let wall = self.at.elapsed();
        let io = RuntimeBusy::capture(&GlobalIORuntime::instance().inner());
        LoadDelta {
            wall,
            process_cpu: process_cpu_time().saturating_sub(self.process_cpu),
            io_runtime: (io.workers, io.busy.saturating_sub(self.io_runtime.busy)),
            h2_runtime: self.h2_runtime.as_ref().map(|(handle, start)| {
                let now = RuntimeBusy::capture(handle);
                (now.workers, now.busy.saturating_sub(start.busy))
            }),
        }
    }
}

/// Load over an interval.
pub struct LoadDelta {
    wall: Duration,
    process_cpu: Duration,
    /// Workers and their summed busy time.
    io_runtime: (usize, Duration),
    h2_runtime: Option<(usize, Duration)>,
}

impl LoadDelta {
    /// Average number of busy threads over the interval.
    fn busy_threads(&self, busy: Duration) -> f64 {
        match self.wall.as_secs_f64() {
            secs if secs > 0.0 => busy.as_secs_f64() / secs,
            _ => 0.0,
        }
    }
}

impl fmt::Display for LoadDelta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "process_cpu_threads={:.1} io_runtime_busy={:.1}/{}",
            self.busy_threads(self.process_cpu),
            self.busy_threads(self.io_runtime.1),
            self.io_runtime.0,
        )?;
        if let Some((workers, busy)) = self.h2_runtime {
            write!(
                f,
                " h2_runtime_busy={:.1}/{}",
                self.busy_threads(busy),
                workers
            )?;
        }
        Ok(())
    }
}

/// User and system CPU time of the process.
fn process_cpu_time() -> Duration {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: `getrusage` fills the struct it is given.
    let ret = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if ret != 0 {
        return Duration::ZERO;
    }
    // SAFETY: initialized by the successful call above.
    let usage = unsafe { usage.assume_init() };
    let time = |t: libc::timeval| {
        Duration::from_secs(t.tv_sec.max(0) as u64) + Duration::from_micros(t.tv_usec.max(0) as u64)
    };
    time(usage.ru_utime) + time(usage.ru_stime)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_cpu_time_advances() {
        let start = process_cpu_time();
        let mut x = 0u64;
        let until = Instant::now() + Duration::from_millis(30);
        while Instant::now() < until {
            x = std::hint::black_box(x.wrapping_add(1));
        }
        assert!(process_cpu_time() > start);
    }

    #[test]
    fn test_busy_threads() {
        let delta = LoadDelta {
            wall: Duration::from_secs(2),
            process_cpu: Duration::from_secs(3),
            io_runtime: (4, Duration::from_secs(1)),
            h2_runtime: Some((8, Duration::from_secs(4))),
        };
        assert_eq!(
            delta.to_string(),
            "process_cpu_threads=1.5 io_runtime_busy=0.5/4 h2_runtime_busy=2.0/8"
        );
    }
}
