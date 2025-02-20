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

use chrono::DateTime;
use chrono::Utc;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;

use super::AbstractClusterStatistics;
use crate::meta::BlockMeta;
use crate::meta::ColumnMeta;
use crate::meta::ColumnStatistics;
use crate::meta::Compression;
use crate::meta::Location;
pub trait AbstractBlockMeta {
    fn row_count(&self) -> u64;
    fn block_size(&self) -> u64;
    fn file_size(&self) -> u64;
    fn col_stats(&self) -> &HashMap<ColumnId, ColumnStatistics>;
    fn col_metas(&self) -> &HashMap<ColumnId, ColumnMeta>;
    fn cluster_stats(&self) -> Option<&dyn AbstractClusterStatistics>;
    fn location(&self) -> &Location;
    fn bloom_filter_index_location(&self) -> Option<&Location>;
    fn bloom_filter_index_size(&self) -> u64;
    fn inverted_index_size(&self) -> Option<u64>;
    fn compression(&self) -> Compression;
    fn create_on(&self) -> Option<DateTime<Utc>>;
}

impl AbstractBlockMeta for BlockMeta {
    fn row_count(&self) -> u64 {
        self.row_count
    }

    fn block_size(&self) -> u64 {
        self.block_size
    }

    fn file_size(&self) -> u64 {
        self.file_size
    }

    fn col_stats(&self) -> &HashMap<ColumnId, ColumnStatistics> {
        &self.col_stats
    }

    fn col_metas(&self) -> &HashMap<ColumnId, ColumnMeta> {
        &self.col_metas
    }

    fn cluster_stats(&self) -> Option<&dyn AbstractClusterStatistics> {
        self.cluster_stats
            .as_ref()
            .map(|v| v as &dyn AbstractClusterStatistics)
    }

    fn location(&self) -> &Location {
        &self.location
    }

    fn bloom_filter_index_location(&self) -> Option<&Location> {
        self.bloom_filter_index_location.as_ref()
    }

    fn bloom_filter_index_size(&self) -> u64 {
        self.bloom_filter_index_size
    }

    fn inverted_index_size(&self) -> Option<u64> {
        self.inverted_index_size
    }

    fn compression(&self) -> Compression {
        self.compression
    }

    fn create_on(&self) -> Option<DateTime<Utc>> {
        self.create_on
    }
}

pub struct ColumnOrientedBlockMeta {
    blocks: DataBlock,
    index: usize,
}

impl ColumnOrientedBlockMeta {
    pub fn new(blocks: DataBlock, index: usize) -> Self {
        Self { blocks, index }
    }
}

impl AbstractBlockMeta for ColumnOrientedBlockMeta {
    fn row_count(&self) -> u64 {
        todo!()
    }

    fn block_size(&self) -> u64 {
        todo!()
    }

    fn file_size(&self) -> u64 {
        todo!()
    }

    fn col_stats(
        &self,
    ) -> &std::collections::HashMap<
        databend_common_expression::ColumnId,
        crate::meta::ColumnStatistics,
    > {
        todo!()
    }

    fn col_metas(
        &self,
    ) -> &std::collections::HashMap<databend_common_expression::ColumnId, crate::meta::ColumnMeta>
    {
        todo!()
    }

    fn cluster_stats(&self) -> Option<&dyn crate::meta::AbstractClusterStatistics> {
        todo!()
    }

    fn location(&self) -> &crate::meta::Location {
        todo!()
    }

    fn bloom_filter_index_location(&self) -> Option<&crate::meta::Location> {
        todo!()
    }

    fn bloom_filter_index_size(&self) -> u64 {
        todo!()
    }

    fn inverted_index_size(&self) -> Option<u64> {
        todo!()
    }

    fn compression(&self) -> crate::meta::Compression {
        todo!()
    }

    fn create_on(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        todo!()
    }
}
