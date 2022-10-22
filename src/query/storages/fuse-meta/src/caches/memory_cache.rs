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

pub type ItemCache<V> = RwLock<LruCache<String, Arc<V>, DefaultHashBuilder, Count>>;

pub struct Labeled<T> {
    item: T,
    tenant_label: TenantLabel,
}

impl<T> Labeled<ItemCache<T>> {
    pub fn write(&self) -> RwLockWriteGuard<'_, RawRwLock, LruCache<String, Arc<T>>> {
        self.item.write()
    }

    pub fn label(&self) -> &TenantLabel {
        &self.tenant_label
    }
}

pub type LabeledItemCache<T> = Arc<Labeled<ItemCache<T>>>;

// cache meters by bytes
pub type BytesCache = Arc<RwLock<LruCache<String, Arc<Vec<u8>>, DefaultHashBuilder, BytesMeter>>>;

pub fn new_item_cache<V>(capacity: u64, tenant_label: TenantLabel) -> LabeledItemCache<V> {
    let item = RwLock::new(LruCache::new(capacity));
    Arc::new(Labeled { item, tenant_label })
}

pub fn new_bytes_cache(capacity: u64) -> BytesCache {
    let c = LruCache::with_meter_and_hasher(capacity, BytesMeter, DefaultHashBuilder::new());
    Arc::new(RwLock::new(c))
}

pub type SegmentInfoCache = LabeledItemCache<SegmentInfo>;
pub type TableSnapshotCache = LabeledItemCache<TableSnapshot>;
/// Cache bloom filter.
/// For each index block, columns are cached individually.
pub type BloomIndexCache = BytesCache;
/// FileMetaCache of bloom filter index data.
/// Each cache item per block
pub type BloomIndexMetaCache = LabeledItemCache<FileMetaData>;

pub type FileMetaDataCache = LabeledItemCache<FileMetaData>;
