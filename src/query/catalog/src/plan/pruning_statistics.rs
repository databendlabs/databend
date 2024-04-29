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

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone, Debug, Default)]
pub struct PruningStatistics {
    /// Segment range pruning stats.
    pub segments_range_pruning_before: usize,
    pub segments_range_pruning_after: usize,

    /// Block range pruning stats.
    pub blocks_range_pruning_before: usize,
    pub blocks_range_pruning_after: usize,

    /// Block bloom filter pruning stats.
    pub blocks_bloom_pruning_before: usize,
    pub blocks_bloom_pruning_after: usize,

    /// Block inverted index filter pruning stats.
    pub blocks_inverted_index_pruning_before: usize,
    pub blocks_inverted_index_pruning_after: usize,
}

impl PruningStatistics {
    pub fn merge(&mut self, other: &Self) {
        self.segments_range_pruning_before += other.segments_range_pruning_before;
        self.segments_range_pruning_after += other.segments_range_pruning_after;
        self.blocks_range_pruning_before += other.blocks_range_pruning_before;
        self.blocks_range_pruning_after += other.blocks_range_pruning_after;
        self.blocks_bloom_pruning_before += other.blocks_bloom_pruning_before;
        self.blocks_bloom_pruning_after += other.blocks_bloom_pruning_after;
        self.blocks_inverted_index_pruning_before += other.blocks_inverted_index_pruning_before;
        self.blocks_inverted_index_pruning_after += other.blocks_inverted_index_pruning_after;
    }
}
