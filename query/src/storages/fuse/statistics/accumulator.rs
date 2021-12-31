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
use common_datavalues::prelude::DataColumn;
use common_datavalues::DataSchema;

use crate::storages::fuse::meta::BlockLocation;
use crate::storages::fuse::meta::BlockMeta;
use crate::storages::fuse::meta::ColumnId;
use crate::storages::index::BlockStatistics;
use crate::storages::index::ColumnStatistics;

#[derive(Default)]
pub struct StatisticsAccumulator {
    pub blocks_metas: Vec<BlockMeta>,
    pub blocks_statistics: Vec<BlockStatistics>,
    pub summary_row_count: u64,
    pub summary_block_count: u64,
    pub in_memory_size: u64,
    pub file_size: u64,
}

impl StatisticsAccumulator {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn begin(mut self, block: &DataBlock) -> common_exception::Result<PartiallyAccumulated> {
        let row_count = block.num_rows() as u64;
        let block_in_memory_size = block.memory_size() as u64;

        self.summary_block_count += 1;
        self.summary_row_count += row_count;
        self.in_memory_size += block_in_memory_size;
        let block_stats = Self::acc_columns(block)?;
        self.blocks_statistics.push(block_stats.clone());
        Ok(PartiallyAccumulated {
            accumulator: self,
            block_row_count: block.num_rows() as u64,
            block_size: block.memory_size() as u64,
            block_column_statistics: block_stats,
        })
    }

    pub fn summary(&self, schema: &DataSchema) -> common_exception::Result<BlockStatistics> {
        super::reduce_block_stats(&self.blocks_statistics, schema)
    }

    pub fn acc_columns(data_block: &DataBlock) -> common_exception::Result<BlockStatistics> {
        (0..)
            .into_iter()
            .zip(data_block.columns().iter())
            .map(|(idx, col)| {
                let min = match col {
                    DataColumn::Array(s) => s.min(),
                    DataColumn::Constant(v, _) => Ok(v.clone()),
                }?;

                let max = match col {
                    DataColumn::Array(s) => s.max(),
                    DataColumn::Constant(v, _) => Ok(v.clone()),
                }?;

                let null_count = match col {
                    DataColumn::Array(s) => s.null_count(),
                    DataColumn::Constant(v, _) => {
                        if v.is_null() {
                            1
                        } else {
                            0
                        }
                    }
                } as u64;

                let in_memory_size = col.get_array_memory_size() as u64;

                let col_stats = ColumnStatistics {
                    min,
                    max,
                    null_count,
                    in_memory_size,
                };

                Ok((idx, col_stats))
            })
            .collect()
    }
}

pub struct PartiallyAccumulated {
    accumulator: StatisticsAccumulator,
    block_row_count: u64,
    block_size: u64,
    block_column_statistics: HashMap<ColumnId, ColumnStatistics>,
}

impl PartiallyAccumulated {
    pub fn end(mut self, file_size: u64, location: String) -> StatisticsAccumulator {
        let mut stats = &mut self.accumulator;
        stats.file_size += file_size;
        let block_meta = BlockMeta {
            location: BlockLocation {
                path: location,
                meta_size: 0,
            },
            row_count: self.block_row_count,
            block_size: self.block_size,
            file_size,
            col_stats: self.block_column_statistics,
        };
        stats.blocks_metas.push(block_meta);
        self.accumulator
    }
}
