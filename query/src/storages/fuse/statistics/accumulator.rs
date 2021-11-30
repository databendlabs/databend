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

use common_datablocks::DataBlock;

use crate::storages::fuse::meta::BlockMeta;
use crate::storages::fuse::meta::ColumnId;
use crate::storages::fuse::statistics::util;
use crate::storages::index::BlockStatistics;
use crate::storages::index::ColumnStatistics;

#[derive(Default)]
pub struct StatisticsAccumulator {
    pub blocks_metas: Vec<BlockMeta>,
    pub blocks_stats: Vec<BlockStatistics>,
    pub summary_row_count: u64,
    pub summary_block_count: u64,
    pub in_memory_size: u64,
    pub file_size: u64,
    pub last_block_rows: u64,
    pub last_block_size: u64,
    pub last_block_col_stats: Option<HashMap<ColumnId, ColumnStatistics>>,
}

impl StatisticsAccumulator {
    pub fn new() -> Self {
        Default::default()
    }
}

impl StatisticsAccumulator {
    pub fn acc(&mut self, block: &DataBlock) -> common_exception::Result<()> {
        let row_count = block.num_rows() as u64;
        let block_in_memory_size = block.memory_size() as u64;

        self.summary_block_count += 1;
        self.summary_row_count += row_count;
        self.in_memory_size += block_in_memory_size;
        self.last_block_rows = block.num_rows() as u64;
        self.last_block_size = block.memory_size() as u64;
        let block_stats = util::block_stats(block)?;
        self.last_block_col_stats = Some(block_stats.clone());
        self.blocks_stats.push(block_stats);
        Ok(())
    }
}
