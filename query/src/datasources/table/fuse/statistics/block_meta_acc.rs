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

use crate::datasources::table::fuse::meta::BlockLocation;
use crate::datasources::table::fuse::meta::BlockMeta;
use crate::datasources::table::fuse::statistics::accumulator::StatisticsAccumulator;

#[derive(Default)]
pub struct BlockMetaAccumulator {
    pub blocks_metas: Vec<BlockMeta>,
}

impl BlockMetaAccumulator {
    pub fn new() -> Self {
        Default::default()
    }
}

impl BlockMetaAccumulator {
    pub fn acc(&mut self, file_size: u64, location: String, stats: &mut StatisticsAccumulator) {
        stats.file_size += file_size;
        let block_meta = BlockMeta {
            location: BlockLocation {
                location,
                meta_size: 0,
            },
            row_count: stats.last_block_rows,
            block_size: stats.last_block_size,
            col_stats: stats.last_block_col_stats.take().unwrap_or_default(),
        };
        self.blocks_metas.push(block_meta);
    }
}
