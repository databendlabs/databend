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

use std::array;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

#[derive(Default)]
pub struct FusePruningStatistics {
    /// Segment read cost in nanoseconds.
    pub segments_read_cost: AtomicU64,
    /// Segment block metas decompress cost in nanoseconds.
    pub segments_decompress_cost: AtomicU64,

    /// Segment range pruning stats.
    pub segments_range_pruning_before: AtomicU64,
    pub segments_range_pruning_after: AtomicU64,
    /// Segment range pruning cost in nanoseconds.
    pub segments_range_pruning_cost: AtomicU64,

    /// Block range pruning stats.
    pub blocks_range_pruning_before: AtomicU64,
    pub blocks_range_pruning_after: AtomicU64,
    /// Block range pruning cost in nanoseconds.
    pub blocks_range_pruning_cost: AtomicU64,

    /// Block bloom filter pruning stats.
    pub blocks_bloom_pruning_before: AtomicU64,
    pub blocks_bloom_pruning_after: AtomicU64,
    /// Block bloom pruning cost in nanoseconds.
    pub blocks_bloom_pruning_cost: AtomicU64,
    /// Block bloom index read cost in nanoseconds.
    pub blocks_bloom_index_read_cost: AtomicU64,

    /// Block inverted index filter pruning stats.
    pub blocks_inverted_index_pruning_before: AtomicU64,
    pub blocks_inverted_index_pruning_after: AtomicU64,
    /// Block inverted index pruning cost in nanoseconds.
    pub blocks_inverted_index_pruning_cost: AtomicU64,

    /// Block vector index filter pruning stats.
    pub blocks_vector_index_pruning_before: AtomicU64,
    pub blocks_vector_index_pruning_after: AtomicU64,
    /// Block vector index pruning cost in nanoseconds.
    pub blocks_vector_index_pruning_cost: AtomicU64,

    /// Block spatial index filter pruning stats.
    pub blocks_spatial_index_pruning_before: AtomicU64,
    pub blocks_spatial_index_pruning_after: AtomicU64,
    /// Block spatial index pruning cost in nanoseconds.
    pub blocks_spatial_index_pruning_cost: AtomicU64,

    /// Block topn pruning stats.
    pub blocks_topn_pruning_before: AtomicU64,
    pub blocks_topn_pruning_after: AtomicU64,
    /// Block topn pruning cost in nanoseconds.
    pub blocks_topn_pruning_cost: AtomicU64,
}

impl FusePruningStatistics {
    pub fn add_segments_read_cost(&self, duration: Duration) {
        self.segments_read_cost
            .fetch_add(duration_to_nanos(duration), Ordering::Relaxed);
    }

    pub fn get_segments_read_cost(&self) -> u64 {
        nanos_to_display_micros(self.segments_read_cost.load(Ordering::Relaxed))
    }

    pub fn add_segments_decompress_cost(&self, duration: Duration) {
        self.segments_decompress_cost
            .fetch_add(duration_to_nanos(duration), Ordering::Relaxed);
    }

    pub fn get_segments_decompress_cost(&self) -> u64 {
        nanos_to_display_micros(self.segments_decompress_cost.load(Ordering::Relaxed))
    }

    pub fn set_segments_range_pruning_before(&self, v: u64) {
        self.segments_range_pruning_before
            .fetch_add(v, Ordering::Relaxed);
    }

    pub fn get_segments_range_pruning_before(&self) -> u64 {
        self.segments_range_pruning_before.load(Ordering::Relaxed)
    }

    pub fn set_segments_range_pruning_after(&self, v: u64) {
        self.segments_range_pruning_after
            .fetch_add(v, Ordering::Relaxed);
    }

    pub fn get_segments_range_pruning_after(&self) -> u64 {
        self.segments_range_pruning_after.load(Ordering::Relaxed)
    }

    pub fn add_segments_range_pruning_cost(&self, duration: Duration) {
        self.segments_range_pruning_cost
            .fetch_add(duration_to_nanos(duration), Ordering::Relaxed);
    }

    pub fn get_segments_range_pruning_cost(&self) -> u64 {
        nanos_to_display_micros(self.segments_range_pruning_cost.load(Ordering::Relaxed))
    }

    pub fn set_blocks_range_pruning_before(&self, v: u64) {
        self.blocks_range_pruning_before
            .fetch_add(v, Ordering::Relaxed);
    }

    pub fn get_blocks_range_pruning_before(&self) -> u64 {
        self.blocks_range_pruning_before.load(Ordering::Relaxed)
    }

    pub fn set_blocks_range_pruning_after(&self, v: u64) {
        self.blocks_range_pruning_after
            .fetch_add(v, Ordering::Relaxed);
    }

    pub fn get_blocks_range_pruning_after(&self) -> u64 {
        self.blocks_range_pruning_after.load(Ordering::Relaxed)
    }

    pub fn add_blocks_range_pruning_cost(&self, duration: Duration) {
        self.blocks_range_pruning_cost
            .fetch_add(duration_to_nanos(duration), Ordering::Relaxed);
    }

    pub fn get_blocks_range_pruning_cost(&self) -> u64 {
        nanos_to_display_micros(self.blocks_range_pruning_cost.load(Ordering::Relaxed))
    }

    pub fn set_blocks_bloom_pruning_before(&self, v: u64) {
        self.blocks_bloom_pruning_before
            .fetch_add(v, Ordering::Relaxed);
    }

    pub fn get_blocks_bloom_pruning_before(&self) -> u64 {
        self.blocks_bloom_pruning_before.load(Ordering::Relaxed)
    }

    pub fn set_blocks_bloom_pruning_after(&self, v: u64) {
        self.blocks_bloom_pruning_after
            .fetch_add(v, Ordering::Relaxed);
    }

    pub fn get_blocks_bloom_pruning_after(&self) -> u64 {
        self.blocks_bloom_pruning_after.load(Ordering::Relaxed)
    }

    pub fn add_blocks_bloom_pruning_cost(&self, duration: Duration) {
        self.blocks_bloom_pruning_cost
            .fetch_add(duration_to_nanos(duration), Ordering::Relaxed);
    }

    pub fn get_blocks_bloom_pruning_cost(&self) -> u64 {
        nanos_to_display_micros(self.blocks_bloom_pruning_cost.load(Ordering::Relaxed))
    }

    pub fn add_blocks_bloom_index_read_cost(&self, duration: Duration) {
        self.blocks_bloom_index_read_cost
            .fetch_add(duration_to_nanos(duration), Ordering::Relaxed);
    }

    pub fn get_blocks_bloom_index_read_cost(&self) -> u64 {
        nanos_to_display_micros(self.blocks_bloom_index_read_cost.load(Ordering::Relaxed))
    }

    pub fn set_blocks_inverted_index_pruning_before(&self, v: u64) {
        self.blocks_inverted_index_pruning_before
            .fetch_add(v, Ordering::Relaxed);
    }

    pub fn get_blocks_inverted_index_pruning_before(&self) -> u64 {
        self.blocks_inverted_index_pruning_before
            .load(Ordering::Relaxed)
    }

    pub fn set_blocks_inverted_index_pruning_after(&self, v: u64) {
        self.blocks_inverted_index_pruning_after
            .fetch_add(v, Ordering::Relaxed);
    }

    pub fn get_blocks_inverted_index_pruning_after(&self) -> u64 {
        self.blocks_inverted_index_pruning_after
            .load(Ordering::Relaxed)
    }

    pub fn add_blocks_inverted_index_pruning_cost(&self, duration: Duration) {
        self.blocks_inverted_index_pruning_cost
            .fetch_add(duration_to_nanos(duration), Ordering::Relaxed);
    }

    pub fn get_blocks_inverted_index_pruning_cost(&self) -> u64 {
        nanos_to_display_micros(
            self.blocks_inverted_index_pruning_cost
                .load(Ordering::Relaxed),
        )
    }

    pub fn set_blocks_vector_index_pruning_before(&self, v: u64) {
        self.blocks_vector_index_pruning_before
            .fetch_add(v, Ordering::Relaxed);
    }

    pub fn get_blocks_vector_index_pruning_before(&self) -> u64 {
        self.blocks_vector_index_pruning_before
            .load(Ordering::Relaxed)
    }

    pub fn set_blocks_vector_index_pruning_after(&self, v: u64) {
        self.blocks_vector_index_pruning_after
            .fetch_add(v, Ordering::Relaxed);
    }

    pub fn get_blocks_vector_index_pruning_after(&self) -> u64 {
        self.blocks_vector_index_pruning_after
            .load(Ordering::Relaxed)
    }

    pub fn add_blocks_vector_index_pruning_cost(&self, duration: Duration) {
        self.blocks_vector_index_pruning_cost
            .fetch_add(duration_to_nanos(duration), Ordering::Relaxed);
    }

    pub fn get_blocks_vector_index_pruning_cost(&self) -> u64 {
        nanos_to_display_micros(
            self.blocks_vector_index_pruning_cost
                .load(Ordering::Relaxed),
        )
    }

    pub fn set_blocks_spatial_index_pruning_before(&self, v: u64) {
        self.blocks_spatial_index_pruning_before
            .fetch_add(v, Ordering::Relaxed);
    }

    pub fn get_blocks_spatial_index_pruning_before(&self) -> u64 {
        self.blocks_spatial_index_pruning_before
            .load(Ordering::Relaxed)
    }

    pub fn set_blocks_spatial_index_pruning_after(&self, v: u64) {
        self.blocks_spatial_index_pruning_after
            .fetch_add(v, Ordering::Relaxed);
    }

    pub fn get_blocks_spatial_index_pruning_after(&self) -> u64 {
        self.blocks_spatial_index_pruning_after
            .load(Ordering::Relaxed)
    }

    pub fn add_blocks_spatial_index_pruning_cost(&self, duration: Duration) {
        self.blocks_spatial_index_pruning_cost
            .fetch_add(duration_to_nanos(duration), Ordering::Relaxed);
    }

    pub fn get_blocks_spatial_index_pruning_cost(&self) -> u64 {
        nanos_to_display_micros(
            self.blocks_spatial_index_pruning_cost
                .load(Ordering::Relaxed),
        )
    }

    pub fn set_blocks_topn_pruning_before(&self, v: u64) {
        self.blocks_topn_pruning_before
            .fetch_add(v, Ordering::Relaxed);
    }

    pub fn get_blocks_topn_pruning_before(&self) -> u64 {
        self.blocks_topn_pruning_before.load(Ordering::Relaxed)
    }

    pub fn set_blocks_topn_pruning_after(&self, v: u64) {
        self.blocks_topn_pruning_after
            .fetch_add(v, Ordering::Relaxed);
    }

    pub fn get_blocks_topn_pruning_after(&self) -> u64 {
        self.blocks_topn_pruning_after.load(Ordering::Relaxed)
    }

    pub fn add_blocks_topn_pruning_cost(&self, duration: Duration) {
        self.blocks_topn_pruning_cost
            .fetch_add(duration_to_nanos(duration), Ordering::Relaxed);
    }

    pub fn get_blocks_topn_pruning_cost(&self) -> u64 {
        nanos_to_display_micros(self.blocks_topn_pruning_cost.load(Ordering::Relaxed))
    }
}

fn nanos_to_display_micros(nanos: u64) -> u64 {
    let micros = nanos / 1_000;
    if micros == 0 && nanos > 0 { 1 } else { micros }
}

fn duration_to_nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

#[derive(Clone)]
pub struct PruningCostController {
    stats: Arc<FusePruningStatistics>,
    enabled: bool,
    timers: Arc<PruningCostTimers>,
}

impl PruningCostController {
    pub fn new(stats: Arc<FusePruningStatistics>, enabled: bool) -> Self {
        Self {
            stats,
            enabled,
            timers: Arc::new(PruningCostTimers::new()),
        }
    }

    pub fn timer(&self, kind: PruningCostKind) -> PruningCostGuard {
        if self.enabled {
            PruningCostGuard {
                stats: self.stats.clone(),
                token: Some(self.timers.acquire(kind)),
            }
        } else {
            PruningCostGuard {
                stats: self.stats.clone(),
                token: None,
            }
        }
    }

    pub fn stats(&self) -> Arc<FusePruningStatistics> {
        self.stats.clone()
    }

    pub fn measure<T>(&self, kind: PruningCostKind, op: impl FnOnce() -> T) -> T {
        let _guard = self.timer(kind);
        op()
    }

    pub async fn measure_async<F, T>(&self, kind: PruningCostKind, fut: F) -> T
    where F: Future<Output = T> {
        let _guard = self.timer(kind);
        fut.await
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum PruningCostKind {
    SegmentsRead,
    SegmentsDecompress,
    SegmentsRange,
    BlocksRange,
    BlocksBloomIndexRead,
    BlocksBloom,
    BlocksInverted,
    BlocksVector,
    BlocksSpatial,
    BlocksTopN,
}

const PRUNING_COST_KIND_COUNT: usize = 10;

impl PruningCostKind {
    const fn as_index(self) -> usize {
        match self {
            PruningCostKind::SegmentsRead => 0,
            PruningCostKind::SegmentsDecompress => 1,
            PruningCostKind::SegmentsRange => 2,
            PruningCostKind::BlocksRange => 3,
            PruningCostKind::BlocksBloomIndexRead => 4,
            PruningCostKind::BlocksBloom => 5,
            PruningCostKind::BlocksInverted => 6,
            PruningCostKind::BlocksVector => 7,
            PruningCostKind::BlocksSpatial => 8,
            PruningCostKind::BlocksTopN => 9,
        }
    }
}

pub struct PruningCostGuard {
    stats: Arc<FusePruningStatistics>,
    token: Option<PruningCostGuardToken>,
}

struct PruningCostGuardToken {
    kind: PruningCostKind,
    timers: Arc<PruningCostTimers>,
}

impl Drop for PruningCostGuard {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            token.finish(&self.stats);
        }
    }
}

impl PruningCostGuardToken {
    fn finish(self, stats: &Arc<FusePruningStatistics>) {
        let Some(elapsed) = self.timers.release(self.kind) else {
            return;
        };

        match self.kind {
            PruningCostKind::SegmentsRead => {
                stats.add_segments_read_cost(elapsed);
            }
            PruningCostKind::SegmentsDecompress => {
                stats.add_segments_decompress_cost(elapsed);
            }
            PruningCostKind::SegmentsRange => {
                stats.add_segments_range_pruning_cost(elapsed);
            }
            PruningCostKind::BlocksRange => {
                stats.add_blocks_range_pruning_cost(elapsed);
            }
            PruningCostKind::BlocksBloomIndexRead => {
                stats.add_blocks_bloom_index_read_cost(elapsed);
            }
            PruningCostKind::BlocksBloom => {
                stats.add_blocks_bloom_pruning_cost(elapsed);
            }
            PruningCostKind::BlocksInverted => {
                stats.add_blocks_inverted_index_pruning_cost(elapsed);
            }
            PruningCostKind::BlocksVector => {
                stats.add_blocks_vector_index_pruning_cost(elapsed);
            }
            PruningCostKind::BlocksSpatial => {
                stats.add_blocks_spatial_index_pruning_cost(elapsed);
            }
            PruningCostKind::BlocksTopN => {
                stats.add_blocks_topn_pruning_cost(elapsed);
            }
        }
    }
}

struct PruningCostTimers {
    epoch: Instant,
    states: [PruningCostState; PRUNING_COST_KIND_COUNT],
}

impl PruningCostTimers {
    fn new() -> Self {
        Self {
            epoch: Instant::now(),
            states: array::from_fn(|_| PruningCostState::new()),
        }
    }

    fn acquire(self: &Arc<Self>, kind: PruningCostKind) -> PruningCostGuardToken {
        let state = &self.states[kind.as_index()];
        if state.active.fetch_add(1, Ordering::AcqRel) == 0 {
            state.start.store(self.elapsed_nanos(), Ordering::Release);
        }

        PruningCostGuardToken {
            kind,
            timers: self.clone(),
        }
    }

    fn release(&self, kind: PruningCostKind) -> Option<Duration> {
        let state = &self.states[kind.as_index()];
        if state.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            let start = state.start.load(Ordering::Acquire);
            let now = self.elapsed_nanos();
            let elapsed_nanos = now.saturating_sub(start);
            Some(Duration::from_nanos(elapsed_nanos))
        } else {
            None
        }
    }

    fn elapsed_nanos(&self) -> u64 {
        duration_to_nanos(self.epoch.elapsed())
    }
}

struct PruningCostState {
    active: AtomicUsize,
    start: AtomicU64,
}

impl PruningCostState {
    const fn new() -> Self {
        Self {
            active: AtomicUsize::new(0),
            start: AtomicU64::new(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;
    use std::time::Duration;

    use super::*;

    #[test]
    fn pruning_cost_rounds_total_sub_microsecond_duration_for_display() {
        assert_eq!(nanos_to_display_micros(0), 0);
        assert_eq!(nanos_to_display_micros(1), 1);
        assert_eq!(nanos_to_display_micros(1_000), 1);

        let stats = FusePruningStatistics::default();
        stats.add_blocks_range_pruning_cost(Duration::from_nanos(400));
        stats.add_blocks_range_pruning_cost(Duration::from_nanos(400));
        assert_eq!(stats.get_blocks_range_pruning_cost(), 1);

        stats.add_blocks_range_pruning_cost(Duration::from_nanos(400));
        assert_eq!(stats.get_blocks_range_pruning_cost(), 1);
    }

    #[test]
    fn pruning_cost_controller_records_parallel_time_once() {
        let stats = Arc::new(FusePruningStatistics::default());
        let controller = PruningCostController::new(stats.clone(), true);
        let threads = 6;
        let barrier = Arc::new(Barrier::new(threads));
        let sleep_time = Duration::from_millis(100);

        let mut handles = Vec::with_capacity(threads);
        for _ in 0..threads {
            let barrier = barrier.clone();
            let controller = controller.clone();
            handles.push(databend_common_base::runtime::Thread::spawn(move || {
                barrier.wait();
                controller.measure(PruningCostKind::BlocksTopN, || {
                    thread::sleep(sleep_time);
                });
            }));
        }
        handles.into_iter().for_each(|h| h.join().unwrap());

        let recorded = stats.get_blocks_topn_pruning_cost();
        let lower = Duration::from_millis(80).as_micros() as u64;
        let upper = Duration::from_millis(220).as_micros() as u64;
        assert!(
            recorded >= lower,
            "recorded pruning cost {recorded}us is too small"
        );
        assert!(
            recorded <= upper,
            "recorded pruning cost {recorded}us exceeded expected bound"
        );
    }
}
