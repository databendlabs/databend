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
#[serde(default)]
pub struct PruningStatistics {
    /// Segment range pruning stats.
    pub segments_range_pruning_before: usize,
    pub segments_range_pruning_after: usize,
    /// Segment range pruning cost in microseconds.
    pub segments_range_pruning_cost: u64,

    /// Block range pruning stats.
    pub blocks_range_pruning_before: usize,
    pub blocks_range_pruning_after: usize,
    /// Block range pruning cost in microseconds.
    pub blocks_range_pruning_cost: u64,

    /// Block bloom filter pruning stats.
    pub blocks_bloom_pruning_before: usize,
    pub blocks_bloom_pruning_after: usize,
    /// Block bloom pruning cost in microseconds.
    pub blocks_bloom_pruning_cost: u64,

    /// Block inverted index filter pruning stats.
    pub blocks_inverted_index_pruning_before: usize,
    pub blocks_inverted_index_pruning_after: usize,
    /// Block inverted index pruning cost in microseconds.
    pub blocks_inverted_index_pruning_cost: u64,

    /// Block vector index filter pruning stats.
    pub blocks_vector_index_pruning_before: usize,
    pub blocks_vector_index_pruning_after: usize,
    /// Block vector index pruning cost in microseconds.
    pub blocks_vector_index_pruning_cost: u64,

    /// Block topn pruning stats.
    pub blocks_topn_pruning_before: usize,
    pub blocks_topn_pruning_after: usize,
    /// Block topn pruning cost in microseconds.
    pub blocks_topn_pruning_cost: u64,
}

impl PruningStatistics {
    pub fn merge(&mut self, other: &Self, use_max_for_before_counts: bool) {
        if use_max_for_before_counts {
            // BlockMod/Broadcast: all nodes see the same segments/blocks
            self.segments_range_pruning_before = self.segments_range_pruning_before.max(other.segments_range_pruning_before);
            self.segments_range_pruning_after = self.segments_range_pruning_after.max(other.segments_range_pruning_after);
            self.blocks_range_pruning_before = self.blocks_range_pruning_before.max(other.blocks_range_pruning_before);
            self.blocks_bloom_pruning_before = self.blocks_bloom_pruning_before.max(other.blocks_bloom_pruning_before);
            self.blocks_inverted_index_pruning_before = self.blocks_inverted_index_pruning_before.max(other.blocks_inverted_index_pruning_before);
            self.blocks_vector_index_pruning_before = self.blocks_vector_index_pruning_before.max(other.blocks_vector_index_pruning_before);
            self.blocks_topn_pruning_before = self.blocks_topn_pruning_before.max(other.blocks_topn_pruning_before);
        } else {
            // Mod/Seq: each node sees different segments/blocks
            self.segments_range_pruning_before += other.segments_range_pruning_before;
            self.segments_range_pruning_after += other.segments_range_pruning_after;
            self.blocks_range_pruning_before += other.blocks_range_pruning_before;
            self.blocks_bloom_pruning_before += other.blocks_bloom_pruning_before;
            self.blocks_inverted_index_pruning_before += other.blocks_inverted_index_pruning_before;
            self.blocks_vector_index_pruning_before += other.blocks_vector_index_pruning_before;
            self.blocks_topn_pruning_before += other.blocks_topn_pruning_before;
        }

        // "after" counts and costs are always summed
        self.segments_range_pruning_cost += other.segments_range_pruning_cost;
        self.blocks_range_pruning_after += other.blocks_range_pruning_after;
        self.blocks_range_pruning_cost += other.blocks_range_pruning_cost;
        self.blocks_bloom_pruning_after += other.blocks_bloom_pruning_after;
        self.blocks_bloom_pruning_cost += other.blocks_bloom_pruning_cost;
        self.blocks_inverted_index_pruning_after += other.blocks_inverted_index_pruning_after;
        self.blocks_inverted_index_pruning_cost += other.blocks_inverted_index_pruning_cost;
        self.blocks_vector_index_pruning_after += other.blocks_vector_index_pruning_after;
        self.blocks_vector_index_pruning_cost += other.blocks_vector_index_pruning_cost;
        self.blocks_topn_pruning_after += other.blocks_topn_pruning_after;
        self.blocks_topn_pruning_cost += other.blocks_topn_pruning_cost;
    }
}
