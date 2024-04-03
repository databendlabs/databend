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

#[derive(Default)]
pub struct FusePruningStatistics {
    /// Segment range pruning stats.
    pub segments_range_pruning_before: AtomicU64,
    pub segments_range_pruning_after: AtomicU64,

    /// Block range pruning stats.
    pub blocks_range_pruning_before: AtomicU64,
    pub blocks_range_pruning_after: AtomicU64,

    /// Block bloom filter pruning stats.
    pub blocks_bloom_pruning_before: AtomicU64,
    pub blocks_bloom_pruning_after: AtomicU64,

    /// Block inverted index filter pruning stats.
    pub blocks_inverted_index_pruning_before: AtomicU64,
    pub blocks_inverted_index_pruning_after: AtomicU64,
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
}
