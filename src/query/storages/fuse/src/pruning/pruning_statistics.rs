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

use std::future::Future;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
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
}

fn duration_to_micros(duration: Duration) -> u64 {
    duration.as_micros().min(u64::MAX as u128) as u64
}

#[derive(Clone)]
pub struct PruningCostController {
    stats: Arc<FusePruningStatistics>,
    enabled: bool,
}

impl PruningCostController {
    pub fn new(stats: Arc<FusePruningStatistics>, enabled: bool) -> Self {
        Self { stats, enabled }
    }

    pub fn timer(&self, kind: PruningCostKind) -> PruningCostGuard {
        if self.enabled {
            PruningCostGuard {
                start: Some(Instant::now()),
                stats: self.stats.clone(),
                kind,
            }
        } else {
            PruningCostGuard {
                start: None,
                stats: self.stats.clone(),
                kind,
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

#[derive(Clone, Copy)]
pub enum PruningCostKind {
    SegmentsRange,
    BlocksRange,
    BlocksBloom,
    BlocksInverted,
    BlocksVector,
}

pub struct PruningCostGuard {
    start: Option<Instant>,
    stats: Arc<FusePruningStatistics>,
    kind: PruningCostKind,
}

impl Drop for PruningCostGuard {
    fn drop(&mut self) {
        let Some(start) = self.start.take() else {
            return;
        };
        let elapsed = start.elapsed();
        match self.kind {
            PruningCostKind::SegmentsRange => {
                self.stats.add_segments_range_pruning_cost(elapsed);
            }
            PruningCostKind::BlocksRange => {
                self.stats.add_blocks_range_pruning_cost(elapsed);
            }
            PruningCostKind::BlocksBloom => {
                self.stats.add_blocks_bloom_pruning_cost(elapsed);
            }
            PruningCostKind::BlocksInverted => {
                self.stats.add_blocks_inverted_index_pruning_cost(elapsed);
            }
            PruningCostKind::BlocksVector => {
                self.stats.add_blocks_vector_index_pruning_cost(elapsed);
            }
        }
    }
}
