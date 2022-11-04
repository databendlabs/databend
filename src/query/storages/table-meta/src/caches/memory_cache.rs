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
use common_cache::BytesMeter;
use common_cache::Cache;
use common_cache::Count;
use common_cache::DefaultHashBuilder;
use common_cache::LruCache;
use parking_lot::lock_api::RwLockWriteGuard;
use parking_lot::RawRwLock;
use parking_lot::RwLock;

use crate::caches::TenantLabel;
use crate::meta::SegmentInfo;
use crate::meta::TableSnapshot;
use crate::meta::TableSnapshotStatistics;

pub type ItemCache<V> = RwLock<LruCache<String, Arc<V>, DefaultHashBuilder, Count>>;
pub type BytesCache = RwLock<LruCache<String, Arc<Vec<u8>>, DefaultHashBuilder, BytesMeter>>;

pub struct Labeled<T> {
    item: T,
    tenant_label: TenantLabel,
}

impl<T> Labeled<T> {
    pub fn label(&self) -> &TenantLabel {
        &self.tenant_label
    }
}

pub type ItemCacheWriteGuard<'a, T> = RwLockWriteGuard<'a, RawRwLock, LruCache<String, Arc<T>>>;
impl<T> Labeled<ItemCache<T>> {
    pub fn write(&self) -> ItemCacheWriteGuard<'_, T> {
        self.item.write()
    }
}

pub type ByteCacheWriteGuard<'a> =
    RwLockWriteGuard<'a, RawRwLock, LruCache<String, Arc<Vec<u8>>, DefaultHashBuilder, BytesMeter>>;
impl Labeled<BytesCache> {
    pub fn write(&self) -> ByteCacheWriteGuard<'_> {
        self.item.write()
    }
}

pub type LabeledItemCache<T> = Arc<Labeled<ItemCache<T>>>;
pub type LabeledBytesCache = Arc<Labeled<BytesCache>>;

pub fn new_item_cache<V>(capacity: u64, tenant_label: TenantLabel) -> LabeledItemCache<V> {
    let item = RwLock::new(LruCache::new(capacity));
    Arc::new(Labeled { item, tenant_label })
}

pub fn new_bytes_cache(capacity: u64, tenant_label: TenantLabel) -> LabeledBytesCache {
    let item = RwLock::new(LruCache::with_meter_and_hasher(
        capacity,
        BytesMeter,
        DefaultHashBuilder::new(),
    ));
    Arc::new(Labeled { item, tenant_label })
}

pub type SegmentInfoCache = LabeledItemCache<SegmentInfo>;
pub type TableSnapshotCache = LabeledItemCache<TableSnapshot>;
pub type TableSnapshotStatisticCache = LabeledItemCache<TableSnapshotStatistics>;
/// Cache bloom filter.
/// For each index block, columns are cached individually.
pub type BloomIndexCache = LabeledBytesCache;
/// FileMetaCache of bloom filter index data.
/// Each cache item per block
pub type BloomIndexMetaCache = LabeledItemCache<FileMetaData>;

pub type FileMetaDataCache = LabeledItemCache<FileMetaData>;
