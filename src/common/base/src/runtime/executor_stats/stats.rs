use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::SystemTime;

use crate::runtime::ThreadTracker;

const RING_BUFFER_SIZE: usize = 10;
const TS_SHIFT: u32 = 32;
const VAL_MASK: u64 = 0xFFFFFFFF;

const NANOS_PER_MICRO: usize = 1_000;

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

/// Unpacks a u64 into a timestamp (u32) and a value (u32).
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

    // Records the elapsed process time in microseconds.
    pub fn record_process_time(&self, elapsed_nanos: usize) {
        let elapsed_micros = elapsed_nanos / NANOS_PER_MICRO;
        self.record_to_slots(&self.process_time, elapsed_micros);
    }

    // Records the number of rows processed.
    pub fn record_process_rows(&self, rows: usize) {
        self.record_to_slots(&self.process_rows, rows);
    }

    fn record_to_slots(&self, slots: &[ExecutorStatsSlot; RING_BUFFER_SIZE], value: usize) {
        let now = SystemTime::now();
        let now_secs = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as usize;
        let index = now_secs % RING_BUFFER_SIZE;
        let slot = &slots[index];
        slot.record_metric(now_secs, value);
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
