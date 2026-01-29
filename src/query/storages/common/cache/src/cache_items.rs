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

use bytes::Bytes;
pub use databend_common_catalog::plan::PartStatistics;
pub use databend_common_catalog::plan::Partitions;
pub use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
pub use databend_storages_common_index::BloomIndexMeta;
pub use databend_storages_common_index::InvertedIndexFile;
pub use databend_storages_common_index::InvertedIndexMeta;
pub use databend_storages_common_index::VectorIndexFile;
pub use databend_storages_common_index::VectorIndexMeta;
pub use databend_storages_common_index::VirtualColumnFileMeta;
pub use databend_storages_common_index::filters::FilterImpl;
pub use databend_storages_common_table_meta::meta::BlockMeta;
pub use databend_storages_common_table_meta::meta::CompactSegmentInfo;
pub use databend_storages_common_table_meta::meta::SegmentInfo;
pub use databend_storages_common_table_meta::meta::SegmentStatistics;
pub use databend_storages_common_table_meta::meta::TableSnapshot;
pub use databend_storages_common_table_meta::meta::TableSnapshotStatistics;
pub use databend_storages_common_table_meta::meta::column_oriented_segment::ColumnOrientedSegment;
pub use parquet::file::metadata::ParquetMetaData;

use crate::HybridCache;

pub struct ColumnData(Bytes);

impl ColumnData {
    pub fn from_bytes(bytes: Bytes) -> Self {
        ColumnData(bytes)
    }

    pub fn from_merge_io_read_result(bytes: Vec<u8>) -> Self {
        ColumnData(bytes.into())
    }

    pub fn bytes(&self) -> Bytes {
        self.0.clone()
    }

    pub fn size(&self) -> usize {
        self.0.len()
    }
}

pub type ColumnDataCache = HybridCache<ColumnData>;
impl TryFrom<&ColumnData> for Vec<u8> {
    type Error = ErrorCode;
    fn try_from(value: &ColumnData) -> Result<Self, Self::Error> {
        Ok(value.0.to_vec())
    }
}

// copying should be avoided
impl TryFrom<Bytes> for ColumnData {
    type Error = ErrorCode;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        Ok(ColumnData(value))
    }
}
