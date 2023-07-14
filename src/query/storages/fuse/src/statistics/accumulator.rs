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

use std::sync::Arc;

use common_expression::BlockThresholds;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::StatisticsOfColumns;

#[derive(Default)]
pub struct StatisticsAccumulator {
    pub blocks_metas: Vec<Arc<BlockMeta>>,
    pub blocks_statistics: Vec<StatisticsOfColumns>,
    pub summary_row_count: u64,
    pub summary_block_count: u64,
    pub in_memory_size: u64,
    pub file_size: u64,
    pub index_size: u64,

    pub perfect_block_count: u64,
    pub thresholds: BlockThresholds,
}

impl StatisticsAccumulator {
    pub fn new(thresholds: BlockThresholds) -> Self {
        Self {
            thresholds,
            ..Default::default()
        }
    }

    pub fn add_with_block_meta(&mut self, block_meta: BlockMeta) {
        self.summary_row_count += block_meta.row_count;
        self.summary_block_count += 1;
        self.in_memory_size += block_meta.block_size;
        self.file_size += block_meta.file_size;
        self.index_size += block_meta.bloom_filter_index_size;
        self.blocks_statistics.push(block_meta.col_stats.clone());

        if self.thresholds.check_large_enough(
            block_meta.row_count as usize,
            block_meta.block_size as usize,
        ) {
            self.perfect_block_count += 1;
        }

        self.blocks_metas.push(Arc::new(block_meta));
    }

    pub fn summary(&self) -> StatisticsOfColumns {
        super::reduce_block_statistics(&self.blocks_statistics)
    }
}
