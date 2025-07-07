use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::SystemTime;

const RING_BUFFER_SIZE: usize = 10;
const TS_SHIFT: u32 = 32;
const VAL_MASK: u64 = 0xFFFFFFFF;

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
        // Start the CAS loop.
        loop {
            // Atomically load the current packed value.
            let current_packed = self.0.load(Ordering::SeqCst);
            let (current_ts, current_val) = unpack(current_packed);

            let new_val;
            let new_ts;

            if current_ts == timestamp {
                // Time window is current. Accumulate the value.
                new_ts = current_ts;
                new_val = current_val.saturating_add(value_to_add);
            } else {
                // Time window has expired. Reset the value and update the timestamp.
                new_ts = timestamp;
                new_val = value_to_add;
            }

            let new_packed = pack(new_ts, new_val);

            // Attempt to swap the old value with the new one.
            match self.0.compare_exchange_weak(
                current_packed,
                new_packed,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return,    // Success, exit the loop.
                Err(_) => continue, // Contention, another thread updated. Retry.
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
    pub query_id: String,
    pub slots: [ExecutorStatsSlot; RING_BUFFER_SIZE],
}

impl ExecutorStats {
    pub fn new(query_id: String) -> Self {
        let slots = std::array::from_fn(|_| ExecutorStatsSlot::new());

        ExecutorStats { query_id, slots }
    }

    pub fn record(&self, elapsed_nano: usize) {
        let now = SystemTime::now();
        let now_secs = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as usize;
        let index = now_secs % RING_BUFFER_SIZE;
        let slot = &self.slots[index];
        slot.record_metric(now_secs, elapsed_nano);
    }
}
