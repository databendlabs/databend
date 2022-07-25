//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::collections::HashMap;

use common_arrow::parquet::metadata::ThriftFileMetaData;
use common_datablocks::DataBlock;
use common_exception::Result;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::ClusterStatistics;
use common_fuse_meta::meta::ColumnId;
use common_fuse_meta::meta::ColumnMeta;
use common_fuse_meta::meta::ColumnStatistics;
use common_fuse_meta::meta::StatisticsOfColumns;
use common_fuse_meta::meta::Versioned;

use crate::storages::fuse::operations::column_metas;
use crate::storages::fuse::statistics::block_statistics::BlockStatistics;
use crate::storages::fuse::statistics::column_statistic;

#[derive(Default)]
pub struct StatisticsAccumulator {
    pub blocks_metas: Vec<BlockMeta>,
    pub blocks_statistics: Vec<StatisticsOfColumns>,
    pub summary_row_count: u64,
    pub summary_block_count: u64,
    pub in_memory_size: u64,
    pub file_size: u64,
}

impl StatisticsAccumulator {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn begin(
        mut self,
        block: &DataBlock,
        cluster_stats: Option<ClusterStatistics>,
    ) -> Result<PartiallyAccumulated> {
        let row_count = block.num_rows() as u64;
        let block_in_memory_size = block.memory_size() as u64;

        self.summary_block_count += 1;
        self.summary_row_count += row_count;
        self.in_memory_size += block_in_memory_size;
        let block_stats = column_statistic::gen_columns_statistics(block)?;
        self.blocks_statistics.push(block_stats.clone());
        Ok(PartiallyAccumulated {
            accumulator: self,
            block_row_count: block.num_rows() as u64,
            block_size: block.memory_size() as u64,
            block_columns_statistics: block_stats,
            block_cluster_statistics: cluster_stats,
        })
    }

    pub fn add_block(
        &mut self,
        file_size: u64,
        meta: ThriftFileMetaData,
        statistics: BlockStatistics,
    ) -> Result<()> {
        self.file_size += file_size;
        self.summary_block_count += 1;
        self.in_memory_size += statistics.block_bytes_size;
        self.summary_row_count += statistics.block_rows_size;
        self.blocks_statistics
            .push(statistics.block_column_statistics.clone());

        let row_count = statistics.block_rows_size;
        let block_size = statistics.block_bytes_size;
        let col_stats = statistics.block_column_statistics.clone();
        let location = (statistics.block_file_location, DataBlock::VERSION);
        let col_metas = column_metas(&meta)?;
        let cluster_stats = statistics.block_cluster_statistics;

        self.blocks_metas.push(BlockMeta::new(
            row_count,
            block_size,
            file_size,
            col_stats,
            col_metas,
            cluster_stats,
            location,
        ));

        Ok(())
    }

    pub fn summary(&self) -> Result<StatisticsOfColumns> {
        super::reduce_block_statistics(&self.blocks_statistics)
    }
}

pub struct PartiallyAccumulated {
    accumulator: StatisticsAccumulator,
    block_row_count: u64,
    block_size: u64,
    block_columns_statistics: HashMap<ColumnId, ColumnStatistics>,
    block_cluster_statistics: Option<ClusterStatistics>,
}

impl PartiallyAccumulated {
    pub fn end(
        mut self,
        file_size: u64,
        location: String,
        col_metas: HashMap<ColumnId, ColumnMeta>,
    ) -> StatisticsAccumulator {
        let mut stats = &mut self.accumulator;
        stats.file_size += file_size;

        let row_count = self.block_row_count;
        let block_size = self.block_size;
        let col_stats = self.block_columns_statistics;
        let cluster_stats = self.block_cluster_statistics;
        let location = (location, DataBlock::VERSION);

        let block_meta = BlockMeta::new(
            row_count,
            block_size,
            file_size,
            col_stats,
            col_metas,
            cluster_stats,
            location,
        );
        stats.blocks_metas.push(block_meta);
        self.accumulator
    }
}
