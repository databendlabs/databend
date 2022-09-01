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

use std::sync::Arc;

use common_arrow::parquet::metadata::FileMetaData;
use common_base::base::tokio::sync::RwLock;
use common_cache::BytesMeter;
use common_cache::Cache;
use common_cache::Count;
use common_cache::DefaultHashBuilder;
use common_cache::LruCache;

use crate::meta::SegmentInfo;
use crate::meta::TableSnapshot;

// cache meters by counting number of items
pub type ItemCache<V> = Arc<RwLock<LruCache<String, Arc<V>, DefaultHashBuilder, Count>>>;

// cache meters by bytes
pub type BytesCache = Arc<RwLock<LruCache<String, Arc<Vec<u8>>, DefaultHashBuilder, BytesMeter>>>;

pub fn new_item_cache<V>(capacity: u64) -> ItemCache<V> {
    Arc::new(RwLock::new(LruCache::new(capacity)))
}

pub fn new_bytes_cache(capacity: u64) -> BytesCache {
    let c = LruCache::with_meter_and_hasher(capacity, BytesMeter, DefaultHashBuilder::new());
    Arc::new(RwLock::new(c))
}

pub type SegmentInfoCache = ItemCache<SegmentInfo>;
pub type TableSnapshotCache = ItemCache<TableSnapshot>;
/// Cache bloom filter.
/// For each index block, columns are cached individually.
pub type BloomIndexCache = BytesCache;
/// FileMetaCache of bloom filter index data.
/// Each cache item per block
pub type BloomIndexMetaCache = ItemCache<FileMetaData>;

pub type FileMetaDataCache = ItemCache<FileMetaData>;
