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

use databend_common_expression::BlockMetaInfo;
use roaring::RoaringTreemap;

use super::condition_cache::CachedCondition;
use crate::io::BlockReadResult;
use crate::io::VirtualBlockReadResult;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;

pub struct ParquetDataSource {
    pub data: BlockReadResult,
    pub virtual_data: Option<VirtualBlockReadResult>,
    /// The condition cache entry of the block, see `condition_cache`.
    pub condition: Option<CachedCondition>,
    /// Whether the block was not read, as its entry has the rows and the scan outputs no column.
    pub io_skipped: bool,
}

impl ParquetDataSource {
    /// A block that is not read: the scan outputs only the rows of `condition`.
    pub fn skipped(condition: CachedCondition) -> Self {
        ParquetDataSource {
            data: BlockReadResult::merge(vec![]),
            virtual_data: None,
            condition: Some(condition),
            io_skipped: true,
        }
    }
}

pub enum BlockSource {
    Parquet(ParquetDataSource),
    /// A block read granule group by granule group in the deserializer, as the granule ranges
    /// of each group, each group within `max_block_size` rows.
    Granule(Vec<Vec<std::ops::Range<usize>>>),
    /// Original row offsets. Scores stay in the partition's index metadata.
    IndexRows(RoaringTreemap),
}

#[typetag::serde(name = "fuse_data_source")]
impl BlockMetaInfo for DataSourceWithMeta<BlockSource> {}
