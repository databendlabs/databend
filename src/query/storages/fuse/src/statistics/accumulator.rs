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
use std::sync::Arc;

use common_exception::Result;
use common_expression::BlockThresholds;
use common_expression::ColumnId;
use common_expression::DataBlock;
use storages_common_table_meta::meta;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::ColumnMeta;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::StatisticsOfColumns;
use storages_common_table_meta::meta::Versioned;

use crate::statistics::block_statistics::BlockStatistics;

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

    pub fn add_block(
        &mut self,
        file_size: u64,
        col_metas: HashMap<ColumnId, ColumnMeta>,
        block_statistics: BlockStatistics,
        bloom_filter_index_location: Option<Location>,
        bloom_filter_index_size: u64,
        block_compression: meta::Compression,
    ) -> Result<()> {
        self.file_size += file_size;
        self.index_size += bloom_filter_index_size;
        self.summary_block_count += 1;
        self.in_memory_size += block_statistics.block_bytes_size;
        self.summary_row_count += block_statistics.block_rows_size;
        self.blocks_statistics
            .push(block_statistics.block_column_statistics.clone());

        let row_count = block_statistics.block_rows_size;
        let block_size = block_statistics.block_bytes_size;
        let col_stats = block_statistics.block_column_statistics.clone();
        let data_location = (block_statistics.block_file_location, DataBlock::VERSION);
        let cluster_stats = block_statistics.block_cluster_statistics;

        if self
            .thresholds
            .check_large_enough(row_count as usize, block_size as usize)
        {
            self.perfect_block_count += 1;
        }

        self.blocks_metas.push(Arc::new(BlockMeta::new(
            row_count,
            block_size,
            file_size,
            col_stats,
            col_metas,
            cluster_stats,
            data_location,
            bloom_filter_index_location,
            bloom_filter_index_size,
            block_compression,
        )));

        Ok(())
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

    pub fn summary(&self) -> Result<StatisticsOfColumns> {
        super::reduce_block_statistics(&self.blocks_statistics)
    }
}
