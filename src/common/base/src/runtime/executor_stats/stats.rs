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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::SystemTime;

use crate::runtime::ThreadTracker;

const RING_BUFFER_SIZE: usize = 10;
const TS_SHIFT: u32 = 32;
const VAL_MASK: u64 = 0xFFFFFFFF;

const MICROS_PER_SEC: u64 = 1_000_000;

/// Snapshot of executor statistics containing timestamp-value pairs for process time and rows.
#[derive(Debug, Clone)]
pub struct ExecutorStatsSnapshot {
    pub process_time: Vec<(u32, u32)>,
    pub process_rows: Vec<(u32, u32)>,
}

/// Packs a timestamp (u32) and a value (u32) into a u64.
#[inline]
fn pack(timestamp: u32, value: u32) -> u64 {
    (timestamp as u64) << TS_SHIFT | (value as u64)
}

/// Unpacks an u64 into a timestamp (u32) and a value (u32).
#[inline]
fn unpack(packed: u64) -> (u32, u32) {
    ((packed >> TS_SHIFT) as u32, (packed & VAL_MASK) as u32)
}

/// A slot for storing executor statistics for a specific time window (1 second).
///
/// It uses a single AtomicU64 to store both a Unix timestamp and a value.
/// - The upper 32 bits store the timestamp (seconds since Unix epoch).
/// - The lower 32 bits store the accumulated value (e.g., rows, duration in micros).
#[derive(Default)]
pub struct ExecutorStatsSlot(AtomicU64);

impl ExecutorStatsSlot {
    /// Creates a new empty ExecutorStatsSlot.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a metric value using the provided timestamp.
    pub fn record_metric(&self, timestamp: usize, value: usize) {
        // Convert to u32, clamping if necessary
        let timestamp_u32 = timestamp as u32;
        let value_u32 = if value > u32::MAX as usize {
            u32::MAX
        } else {
            value as u32
        };
        self.add(timestamp_u32, value_u32);
    }

    /// Adds a value to the slot for the given timestamp.
    ///
    /// This operation is thread-safe and uses a lock-free CAS loop.
    /// If the time window has expired, the value is reset before adding.
    pub fn add(&self, timestamp: u32, value_to_add: u32) {
        let mut current_packed = self.0.load(Ordering::SeqCst);
        loop {
            let (current_ts, current_val) = unpack(current_packed);
            let new_packed = if current_ts == timestamp {
                pack(current_ts, current_val.saturating_add(value_to_add))
            } else {
                pack(timestamp, value_to_add)
            };
            match self.0.compare_exchange_weak(
                current_packed,
                new_packed,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return,
                Err(expected) => {
                    current_packed = expected;
                }
            }
        }
    }

    /// Gets the timestamp and value
    pub fn get(&self) -> (u32, u32) {
        let packed = self.0.load(Ordering::Acquire);
        unpack(packed)
    }
}

// A ring-buffer thread-free implementation for storing scheduling profile
pub struct ExecutorStats {
    pub process_time: [ExecutorStatsSlot; RING_BUFFER_SIZE],
    pub process_rows: [ExecutorStatsSlot; RING_BUFFER_SIZE],
}

impl ExecutorStats {
    pub fn new() -> Self {
        let process_time = std::array::from_fn(|_| ExecutorStatsSlot::new());
        let process_rows = std::array::from_fn(|_| ExecutorStatsSlot::new());
        ExecutorStats {
            process_time,
            process_rows,
        }
    }

    pub fn record_process(&self, begin: SystemTime, elapsed_micros: usize, rows: usize) {
        let begin_micros = begin
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let end_micros = begin_micros + elapsed_micros as u64;

        let begin_timestamp_secs = begin_micros / MICROS_PER_SEC;
        let end_timestamp_secs = end_micros / MICROS_PER_SEC;

        if begin_timestamp_secs == end_timestamp_secs {
            // Single second case - record all in one slot
            let slot_idx = (begin_timestamp_secs % RING_BUFFER_SIZE as u64) as usize;
            self.process_time[slot_idx]
                .record_metric(begin_timestamp_secs as usize, elapsed_micros);
            self.process_rows[slot_idx].record_metric(begin_timestamp_secs as usize, rows);
        } else {
            // Cross-second case - distribute proportionally
            let total_duration_micros = elapsed_micros as u64;

            for current_sec in begin_timestamp_secs..=end_timestamp_secs {
                let slot_idx = (current_sec % RING_BUFFER_SIZE as u64) as usize;

                let sec_start_micros = if current_sec == begin_timestamp_secs {
                    begin_micros % MICROS_PER_SEC
                } else {
                    0
                };

                let sec_end_micros = if current_sec == end_timestamp_secs {
                    end_micros % MICROS_PER_SEC
                } else {
                    MICROS_PER_SEC
                };

                let sec_duration_micros = sec_end_micros - sec_start_micros;
                let proportion = sec_duration_micros as f64 / total_duration_micros as f64;

                let allocated_micros = (elapsed_micros as f64 * proportion) as usize;
                let allocated_rows = (rows as f64 * proportion) as usize;

                if allocated_micros > 0 {
                    self.process_time[slot_idx]
                        .record_metric(current_sec as usize, allocated_micros);
                }
                if allocated_rows > 0 {
                    self.process_rows[slot_idx].record_metric(current_sec as usize, allocated_rows);
                }
            }
        }
    }

    pub fn record_thread_tracker(rows: usize) {
        ThreadTracker::with(|x| {
            x.borrow()
                .payload
                .process_rows
                .store(rows, Ordering::SeqCst)
        });
    }

    pub fn dump_snapshot(&self) -> ExecutorStatsSnapshot {
        let process_time_snapshot = self.process_time.iter().map(|slot| slot.get()).collect();
        let process_rows_snapshot = self.process_rows.iter().map(|slot| slot.get()).collect();

        ExecutorStatsSnapshot {
            process_time: process_time_snapshot,
            process_rows: process_rows_snapshot,
        }
    }
}

impl Default for ExecutorStats {
    fn default() -> Self {
        Self::new()
    }
}
