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

use std::fmt::Write;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::Sender;
use std::time::Duration;
use std::time::Instant;

use log::warn;
use tokio::runtime::Handle;

use super::backtrace::get_all_tasks;

/// How often the watchdog spawns a probe task to check the runtime is responsive.
const RUNTIME_WATCHDOG_PROBE_INTERVAL: Duration = Duration::from_secs(10);

/// If a probe task is not scheduled within this duration, the runtime is
/// considered unhealthy and its tasks are dumped to the log.
const RUNTIME_WATCHDOG_UNHEALTHY_THRESHOLD: Duration = Duration::from_secs(3);

pub(super) enum WatchdogEvent {
    Stop,
    ProbeScheduled(u64),
}

/// Periodically spawn a probe task on the runtime and measure how long it
/// takes to be scheduled. If the probe takes longer than the unhealthy
/// threshold, dump the tasks that belong to this runtime to the log.
///
/// Returns when the stop signal is received (or the sender is dropped).
pub(super) fn run_watchdog_loop(
    handle: &Handle,
    task_dump_marker: &str,
    event_tx: &Sender<WatchdogEvent>,
    event_rx: &Receiver<WatchdogEvent>,
) {
    let mut probe_id = 0u64;

    loop {
        if wait_probe_interval(event_rx) {
            return;
        }

        // Spawn a tiny probe task and measure how long until it actually runs.
        probe_id = probe_id.wrapping_add(1);
        let current_probe_id = probe_id;
        let probe_tx = event_tx.clone();
        let scheduled_at = Instant::now();
        #[expect(clippy::disallowed_methods)]
        handle.spawn(async move {
            let _ = probe_tx.send(WatchdogEvent::ProbeScheduled(current_probe_id));
        });

        match wait_probe_scheduled(event_rx, current_probe_id) {
            ProbeWaitResult::Stop => return,
            ProbeWaitResult::Scheduled => {}
            ProbeWaitResult::Timeout => {
                let elapsed = scheduled_at.elapsed();
                let dump = dump_runtime_tasks(task_dump_marker);
                warn!(
                    "Runtime {:?} watchdog probe was not scheduled within {:?} (waited {:?}); dumping owned tasks:\n{}",
                    task_dump_marker, RUNTIME_WATCHDOG_UNHEALTHY_THRESHOLD, elapsed, dump,
                );
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
enum ProbeWaitResult {
    Stop,
    Scheduled,
    Timeout,
}

fn wait_probe_interval(event_rx: &Receiver<WatchdogEvent>) -> bool {
    wait_probe_interval_for(event_rx, RUNTIME_WATCHDOG_PROBE_INTERVAL)
}

fn wait_probe_interval_for(event_rx: &Receiver<WatchdogEvent>, interval: Duration) -> bool {
    let wait_until = Instant::now() + interval;

    loop {
        let Some(timeout) = wait_until.checked_duration_since(Instant::now()) else {
            return false;
        };

        match event_rx.recv_timeout(timeout) {
            Ok(WatchdogEvent::Stop) => return true,
            Ok(WatchdogEvent::ProbeScheduled(_)) => {
                // Ignore stale probe completions from an earlier timed-out round.
            }
            Err(RecvTimeoutError::Timeout) => return false,
            Err(RecvTimeoutError::Disconnected) => return true,
        }
    }
}

fn wait_probe_scheduled(event_rx: &Receiver<WatchdogEvent>, probe_id: u64) -> ProbeWaitResult {
    wait_probe_scheduled_for(event_rx, probe_id, RUNTIME_WATCHDOG_UNHEALTHY_THRESHOLD)
}

fn wait_probe_scheduled_for(
    event_rx: &Receiver<WatchdogEvent>,
    probe_id: u64,
    threshold: Duration,
) -> ProbeWaitResult {
    let wait_until = Instant::now() + threshold;

    loop {
        let Some(timeout) = wait_until.checked_duration_since(Instant::now()) else {
            return ProbeWaitResult::Timeout;
        };

        match event_rx.recv_timeout(timeout) {
            Ok(WatchdogEvent::Stop) => return ProbeWaitResult::Stop,
            Ok(WatchdogEvent::ProbeScheduled(id)) if id == probe_id => {
                return ProbeWaitResult::Scheduled;
            }
            Ok(WatchdogEvent::ProbeScheduled(_)) => {
                // Ignore stale probe completions from an earlier timed-out round.
            }
            Err(RecvTimeoutError::Timeout) => return ProbeWaitResult::Timeout,
            Err(RecvTimeoutError::Disconnected) => return ProbeWaitResult::Stop,
        }
    }
}

/// Dump only the tasks whose stack frames contain `task_dump_marker`
/// (i.e. tasks owned by this runtime). Returns an empty-style placeholder
/// when no matching task is found.
fn dump_runtime_tasks(task_dump_marker: &str) -> String {
    let (tasks, polling_tasks) = get_all_tasks(false);

    let mut output = String::new();
    let mut matched = 0usize;
    for mut tasks in [tasks, polling_tasks] {
        tasks.sort_by(|l, r| Ord::cmp(&l.stack_frames.len(), &r.stack_frames.len()));

        for item in tasks.into_iter().rev() {
            if !item
                .stack_frames
                .iter()
                .any(|frame| frame.contains(task_dump_marker))
            {
                continue;
            }

            matched += 1;
            for frame in item.stack_frames {
                writeln!(output, "{}", frame).unwrap();
            }
            writeln!(output).unwrap();
        }
    }

    if matched == 0 {
        format!("<no tasks tagged with {task_dump_marker} were found>\n")
    } else {
        output
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::thread;

    use super::*;
    use crate::runtime::Thread;

    #[test]
    fn test_wait_probe_scheduled_returns_stop_immediately() {
        let (event_tx, event_rx) = mpsc::channel();
        let _join = Thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            event_tx.send(WatchdogEvent::Stop).unwrap();
        });

        let started_at = Instant::now();
        let result = wait_probe_scheduled_for(&event_rx, 1, Duration::from_secs(5));

        assert_eq!(result, ProbeWaitResult::Stop);
        assert!(started_at.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn test_wait_probe_scheduled_ignores_stale_probe_events() {
        let (event_tx, event_rx) = mpsc::channel();
        event_tx.send(WatchdogEvent::ProbeScheduled(1)).unwrap();
        event_tx.send(WatchdogEvent::ProbeScheduled(2)).unwrap();

        let result = wait_probe_scheduled_for(&event_rx, 2, Duration::from_millis(100));

        assert_eq!(result, ProbeWaitResult::Scheduled);
    }

    #[test]
    fn test_wait_probe_scheduled_times_out_on_stale_probe_events() {
        let (event_tx, event_rx) = mpsc::channel();
        event_tx.send(WatchdogEvent::ProbeScheduled(1)).unwrap();

        let result = wait_probe_scheduled_for(&event_rx, 2, Duration::from_millis(20));

        assert_eq!(result, ProbeWaitResult::Timeout);
    }

    #[test]
    fn test_wait_probe_interval_ignores_stale_probe_events() {
        let (event_tx, event_rx) = mpsc::channel();
        event_tx.send(WatchdogEvent::ProbeScheduled(1)).unwrap();

        let should_stop = wait_probe_interval_for(&event_rx, Duration::from_millis(20));

        assert!(!should_stop);
    }

    #[test]
    fn test_wait_probe_interval_returns_stop() {
        let (event_tx, event_rx) = mpsc::channel();
        event_tx.send(WatchdogEvent::Stop).unwrap();

        let should_stop = wait_probe_interval_for(&event_rx, Duration::from_secs(5));

        assert!(should_stop);
    }
}
