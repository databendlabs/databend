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

use std::collections::HashMap;

use common_arrow::parquet::metadata::ThriftFileMetaData;
use common_datablocks::DataBlock;
use common_exception::Result;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::ColumnMeta;
use common_fuse_meta::meta::Location;
use common_fuse_meta::meta::StatisticsOfColumns;
use common_fuse_meta::meta::Versioned;

use crate::operations::column_metas;
use crate::statistics::block_statistics::BlockStatistics;

#[derive(Default)]
pub struct StatisticsAccumulator {
    pub blocks_metas: Vec<BlockMeta>,
    pub blocks_statistics: Vec<StatisticsOfColumns>,
    pub summary_row_count: u64,
    pub summary_block_count: u64,
    pub in_memory_size: u64,
    pub file_size: u64,
    pub index_size: u64,
}

impl StatisticsAccumulator {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn add_block(
        &mut self,
        file_size: u64,
        file_meta: ThriftFileMetaData,
        block_statistics: BlockStatistics,
        bloom_filter_index_location: Option<Location>,
        bloom_filter_index_size: u64,
    ) -> Result<()> {
        let col_metas = column_metas(&file_meta)?;
        self.add(
            file_size,
            col_metas,
            block_statistics,
            bloom_filter_index_location,
            bloom_filter_index_size,
        )
    }

    pub fn add_with_block_meta(
        &mut self,
        block_meta: BlockMeta,
        block_statistics: BlockStatistics,
    ) -> Result<()> {
        let bloom_filter_index_location = block_meta.bloom_filter_index_location;
        let bloom_filter_index_size = block_meta.bloom_filter_index_size;
        let file_size = block_meta.file_size;
        let col_metas = block_meta.col_metas;

        self.add(
            file_size,
            col_metas,
            block_statistics,
            bloom_filter_index_location,
            bloom_filter_index_size,
        )
    }

    pub fn summary(&self) -> Result<StatisticsOfColumns> {
        super::reduce_block_statistics(&self.blocks_statistics)
    }

    fn add(
        &mut self,
        file_size: u64,
        column_meta: HashMap<u32, ColumnMeta>,
        block_statistics: BlockStatistics,
        bloom_filter_index_location: Option<Location>,
        bloom_filter_index_size: u64,
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

        self.blocks_metas.push(BlockMeta::new(
            row_count,
            block_size,
            file_size,
            col_stats,
            column_meta,
            cluster_stats,
            data_location,
            bloom_filter_index_location,
            bloom_filter_index_size,
        ));

        Ok(())
    }
}
