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

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::Duration;
use std::time::Instant;

#[derive(Default)]
pub struct FusePruningStatistics {
    /// Segment range pruning stats.
    pub segments_range_pruning_before: AtomicU64,
    pub segments_range_pruning_after: AtomicU64,
    pub segments_range_pruning_cost: AtomicU64,

    /// Block range pruning stats.
    pub blocks_range_pruning_before: AtomicU64,
    pub blocks_range_pruning_after: AtomicU64,
    pub blocks_range_pruning_cost: AtomicU64,

    /// Block bloom filter pruning stats.
    pub blocks_bloom_pruning_before: AtomicU64,
    pub blocks_bloom_pruning_after: AtomicU64,
    pub blocks_bloom_pruning_cost: AtomicU64,

    /// Block inverted index filter pruning stats.
    pub blocks_inverted_index_pruning_before: AtomicU64,
    pub blocks_inverted_index_pruning_after: AtomicU64,
    pub blocks_inverted_index_pruning_cost: AtomicU64,

    /// Block vector index filter pruning stats.
    pub blocks_vector_index_pruning_before: AtomicU64,
    pub blocks_vector_index_pruning_after: AtomicU64,
    pub blocks_vector_index_pruning_cost: AtomicU64,

    /// Block topn pruning stats.
    pub blocks_topn_pruning_before: AtomicU64,
    pub blocks_topn_pruning_after: AtomicU64,
    pub blocks_topn_pruning_cost: AtomicU64,
}

impl FusePruningStatistics {
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
            .fetch_add(duration_to_micros(duration), Ordering::Relaxed);
    }

    pub fn get_segments_range_pruning_cost(&self) -> u64 {
        self.segments_range_pruning_cost.load(Ordering::Relaxed)
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
            .fetch_add(duration_to_micros(duration), Ordering::Relaxed);
    }

    pub fn get_blocks_range_pruning_cost(&self) -> u64 {
        self.blocks_range_pruning_cost.load(Ordering::Relaxed)
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
            .fetch_add(duration_to_micros(duration), Ordering::Relaxed);
    }

    pub fn get_blocks_bloom_pruning_cost(&self) -> u64 {
        self.blocks_bloom_pruning_cost.load(Ordering::Relaxed)
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
            .fetch_add(duration_to_micros(duration), Ordering::Relaxed);
    }

    pub fn get_blocks_inverted_index_pruning_cost(&self) -> u64 {
        self.blocks_inverted_index_pruning_cost
            .load(Ordering::Relaxed)
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
            .fetch_add(duration_to_micros(duration), Ordering::Relaxed);
    }

    pub fn get_blocks_vector_index_pruning_cost(&self) -> u64 {
        self.blocks_vector_index_pruning_cost
            .load(Ordering::Relaxed)
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
            .fetch_add(duration_to_micros(duration), Ordering::Relaxed);
    }

    pub fn get_blocks_topn_pruning_cost(&self) -> u64 {
        self.blocks_topn_pruning_cost.load(Ordering::Relaxed)
    }
}

fn duration_to_micros(duration: Duration) -> u64 {
    duration.as_micros().min(u64::MAX as u128) as u64
}

#[derive(Clone)]
pub struct PruningCostController {
    stats: Arc<FusePruningStatistics>,
    enabled: bool,
    timers: Arc<Mutex<HashMap<PruningCostKind, Weak<PruningCostTimer>>>>,
}

impl PruningCostController {
    pub fn new(stats: Arc<FusePruningStatistics>, enabled: bool) -> Self {
        Self {
            stats,
            enabled,
            timers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn timer(&self, kind: PruningCostKind) -> PruningCostGuard {
        if self.enabled {
            let timer = {
                let mut timers = self.timers.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(existing) = timers.get(&kind).and_then(|w| w.upgrade()) {
                    existing
                } else {
                    let timer = Arc::new(PruningCostTimer {
                        start: Instant::now(),
                    });
                    timers.insert(kind, Arc::downgrade(&timer));
                    timer
                }
            };
            PruningCostGuard {
                stats: self.stats.clone(),
                token: Some(PruningCostGuardToken {
                    kind,
                    timer,
                    timers: self.timers.clone(),
                }),
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
    SegmentsRange,
    BlocksRange,
    BlocksBloom,
    BlocksInverted,
    BlocksVector,
    BlocksTopN,
}

pub struct PruningCostGuard {
    stats: Arc<FusePruningStatistics>,
    token: Option<PruningCostGuardToken>,
}

struct PruningCostGuardToken {
    kind: PruningCostKind,
    timer: Arc<PruningCostTimer>,
    timers: Arc<Mutex<HashMap<PruningCostKind, Weak<PruningCostTimer>>>>,
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
        if Arc::strong_count(&self.timer) == 1 {
            let elapsed = self.timer.start.elapsed();
            let mut timers = self.timers.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(existing) = timers.get(&self.kind) {
                if existing.ptr_eq(&Arc::downgrade(&self.timer)) {
                    timers.remove(&self.kind);
                }
            }
            match self.kind {
                PruningCostKind::SegmentsRange => {
                    stats.add_segments_range_pruning_cost(elapsed);
                }
                PruningCostKind::BlocksRange => {
                    stats.add_blocks_range_pruning_cost(elapsed);
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
                PruningCostKind::BlocksTopN => {
                    stats.add_blocks_topn_pruning_cost(elapsed);
                }
            }
        }
    }
}

struct PruningCostTimer {
    start: Instant,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;
    use std::time::Duration;

    use super::*;

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
            handles.push(thread::spawn(move || {
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
