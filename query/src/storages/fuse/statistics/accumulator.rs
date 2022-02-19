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
use common_datavalues::prelude::*;
use common_datavalues::DataSchema;
use common_functions::aggregates::eval_aggr;

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
        let mut statistics = BlockStatistics::new();

        let rows = data_block.num_rows();
        for idx in 0..data_block.num_columns() {
            let col = data_block.column(idx);
            let field = data_block.schema().field(idx);
            let column_field = ColumnWithField::new(col.clone(), field.clone());

            let mut min = DataValue::Null;
            let mut max = DataValue::Null;

            let mins = eval_aggr("min", vec![], &[column_field.clone()], rows)?;
            let maxs = eval_aggr("max", vec![], &[column_field], rows)?;

            if mins.len() > 0 {
                min = mins.get(0);
            }
            if maxs.len() > 0 {
                max = maxs.get(0);
            }

            let (is_all_null, bitmap) = col.validity();
            let null_count = match (is_all_null, bitmap) {
                (true, _) => rows,
                (false, Some(bitmap)) => bitmap.null_count(),
                (false, None) => 0,
            };

            let in_memory_size = col.memory_size() as u64;
            let col_stats = ColumnStatistics {
                min,
                max,
                null_count: null_count as u64,
                in_memory_size,
            };

            statistics.insert(idx as u32, col_stats);
        }
        Ok(statistics)
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
