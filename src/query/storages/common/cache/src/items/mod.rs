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

mod column_data;
mod iceberg;
mod index_data;
mod index_meta;
mod size;
mod table_meta;
mod vector_index;

pub use column_data::CachedColumnArray;
pub use column_data::ColumnData;
pub use column_data::SizedColumnArray;
pub use databend_common_catalog::plan::PartStatistics;
pub use databend_common_catalog::plan::Partitions;
pub use databend_common_catalog::table::Table;
pub use databend_storages_common_index::BloomIndexMeta;
pub use databend_storages_common_index::IndexFile;
pub use databend_storages_common_index::IndexMeta;
pub use databend_storages_common_index::InvertedIndexLookupBytes;
pub use databend_storages_common_index::InvertedIndexMeta;
pub use databend_storages_common_index::InvertedIndexPayloadBytes;
pub use databend_storages_common_index::SpatialIndexFile;
pub use databend_storages_common_index::SpatialIndexMeta;
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
pub use iceberg::IcebergTableCacheValue;
pub use parquet::file::metadata::ParquetMetaData;
pub use vector_index::CachedVectorIndex;

/// A value that can be cached. The kind is chosen by the `MemCache` holding it, so one type
/// may serve several kinds.
pub trait CachedItem: Send + Sync + 'static {
    fn mem_bytes(&self) -> usize;
}

/// For kinds whose values are handed out as `Arc<T>` beyond the cache. Holders of the inner
/// `Arc` are not seen by the store: such values are not pinned and drop out of `resident` once
/// evicted, as with the pre-refactor caches.
impl<T: CachedItem> CachedItem for std::sync::Arc<T> {
    fn mem_bytes(&self) -> usize {
        T::mem_bytes(self)
    }
}
