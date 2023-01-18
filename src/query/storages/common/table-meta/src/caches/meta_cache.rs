//  Copyright 2022 Datafuse Labs.
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

use common_arrow::parquet::metadata::FileMetaData;
use storages_common_cache::InMemoryBytesCache;
use storages_common_cache::InMemoryItemCache;

use crate::meta::SegmentInfo;
use crate::meta::TableSnapshot;
use crate::meta::TableSnapshotStatistics;

/// In memory object cache of SegmentInfo
pub type SegmentInfoCache = InMemoryItemCache<SegmentInfo>;
/// In memory object cache of TableSnapshot
pub type TableSnapshotCache = InMemoryItemCache<TableSnapshot>;
/// In memory object cache of TableSnapshotStatistics
pub type TableSnapshotStatisticCache = InMemoryItemCache<TableSnapshotStatistics>;
/// In memory data cache of bloom index data.
/// For each indexed data block, the index data of column is cached individually
pub type BloomIndexCache = InMemoryBytesCache;
/// In memory object cache of parquet FileMetaData of bloom index data
pub type BloomIndexMetaCache = InMemoryItemCache<FileMetaData>;
/// In memory object cache of parquet FileMetaData of external parquet files
pub type FileMetaDataCache = InMemoryItemCache<FileMetaData>;
