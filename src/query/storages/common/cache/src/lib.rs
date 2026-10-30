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

#![feature(write_all_vectored)]
#![feature(io_const_error)]
#![allow(clippy::collapsible_if, clippy::let_and_return)]

//! Process-wide caches.
//!
//! - `registry`: what a cache kind is (`CacheDef`) and the registry of the built-in kinds.
//! - `kinds`: the definitions of the built-in kinds, by domain, with their values.
//! - `legacy` and `shared`: the two backends; `backend` is the interface both implement.
//! - `disk`: the disk tier of the kinds that define one.
//! - `handle`: the typed cache a reader gets (`MemCache`); `read`: loading into it.
//! - `local`: plain LRU caches owned by a single component.

mod accessor;
pub mod backend;
mod disk;
mod handle;
pub mod kinds;
pub mod legacy;
mod local;
mod manager;
mod read;
mod registry;
pub mod shared;
mod size;
mod temp_dir;

pub use accessor::CacheAccessor;
pub use backend::KindLimit;
pub use backend::Unit;
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
pub use disk::*;
pub use handle::CacheRef;
pub use handle::MemCache;
pub use handle::Reservation;
pub use handle::Usage;
pub use kinds::column::CachedColumnArray;
pub use kinds::column::ColumnData;
pub use kinds::column::SizedColumnArray;
pub use kinds::iceberg::IcebergTableCacheValue;
pub use kinds::vector::CachedVectorIndex;
pub use local::CacheValue;
pub use local::InMemoryLruCache;
pub use manager::CacheManager;
pub use manager::CacheRow;
pub use parquet::file::metadata::ParquetMetaData;
pub use read::Reader;
pub use read::SingleFlight;
pub use read::TTLReader;
pub use read::TTLValue;
pub use registry::CacheDef;
pub use registry::CacheKind;
pub use registry::CacheRegistry;
pub use registry::CachedItem;
pub use registry::KindInfo;
pub use registry::Pool;
pub use shared::PoolCaps;
pub use temp_dir::*;
