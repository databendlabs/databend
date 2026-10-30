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

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

use arrow::array::ArrayRef;
use databend_common_cache::MemSized;
use databend_common_expression::Column;

use crate::CacheAccessor;
use crate::InMemoryLruCache;
pub use crate::cache_items::*;
use crate::manager::CacheManager;
use crate::providers::HybridCache;

/// Count-limited cache of complete persisted raw-bundle footers.
pub type InvertedIndexMetaCache = HybridCache<InvertedIndexMeta>;
/// Byte-limited cache of term dictionaries and small fieldnorm/fast components.
/// Large `.fieldnorm` / `.fast` files are stored as payload pages instead.
pub type InvertedIndexLookupCache = HybridCache<InvertedIndexLookupBytes>;
/// Byte-limited cache of postings, positions, store, and other payload pages.
pub type InvertedIndexPayloadCache = HybridCache<InvertedIndexPayloadBytes>;
/// Memory-only cache of complete `.idx` and `.pos` components. Kept separate from pages so
/// large full warmups cannot evict frequently accessed small posting ranges.
pub type InvertedIndexFullPayloadCache = InMemoryLruCache<InvertedIndexPayloadBytes>;

/// In memory object cache of parquet FileMetaData of external parquet rs files
pub type ParquetMetaDataCache = InMemoryLruCache<ParquetMetaData>;

pub struct IcebergTableCacheValue {
    table: Arc<dyn Table>,
    refreshing: AtomicBool,
    loaded_at: Instant,
    credential_refresh_at: Option<Instant>,
}

impl IcebergTableCacheValue {
    pub fn new(table: Arc<dyn Table>, credential_refresh_at: Option<Instant>) -> Self {
        Self {
            table,
            refreshing: AtomicBool::new(false),
            loaded_at: Instant::now(),
            credential_refresh_at,
        }
    }

    pub fn table(&self) -> Arc<dyn Table> {
        self.table.clone()
    }

    pub(crate) fn loaded_at(&self) -> Instant {
        self.loaded_at
    }

    pub(crate) fn credential_refresh_at(&self) -> Option<Instant> {
        self.credential_refresh_at
    }

    pub(crate) fn is_refreshing(&self) -> bool {
        self.refreshing.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn set_refreshing(&self) {
        self.refreshing
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

pub type IcebergTableCache = InMemoryLruCache<IcebergTableCacheValue>;

/// In memory object cache of table column array
pub type ColumnArrayCache = InMemoryLruCache<SizedColumnArray>;
pub type ArrayRawDataUncompressedSize = usize;
pub type SizedColumnArray = (CachedColumnArray, ArrayRawDataUncompressedSize);

/// A deserialized column kept in the in-memory column array cache.
#[derive(Clone, Debug)]
pub enum CachedColumnArray {
    /// Arrow array as produced by the Parquet reader.
    Arrow(ArrayRef),
    /// Column in Databend's representation, e.g. an encoded column that has no Arrow counterpart.
    Column(Column),
}

impl From<ArrayRef> for CachedColumnArray {
    fn from(array: ArrayRef) -> Self {
        CachedColumnArray::Arrow(array)
    }
}

// Bind Type of cached objects to Caches
//
// The `Cache` should return
// - cache item of Type `T`
// - and implement `CacheAccessor` properly
pub trait CachedObject<T> {
    type Cache: CacheAccessor<V = T>;
    fn cache() -> Option<Self::Cache>;
}

impl CachedObject<IcebergTableCacheValue> for IcebergTableCacheValue {
    type Cache = IcebergTableCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_iceberg_table_cache()
    }
}

impl CachedObject<ParquetMetaData> for ParquetMetaData {
    type Cache = ParquetMetaDataCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_parquet_meta_data_cache()
    }
}

pub struct CacheValue<T> {
    inner: Arc<T>,
    mem_bytes: usize,
}

impl<T> CacheValue<T> {
    pub fn new(inner: T, mem_bytes: usize) -> Self {
        Self {
            inner: Arc::new(inner),
            mem_bytes,
        }
    }

    pub fn get_inner(&self) -> Arc<T> {
        self.inner.clone()
    }

    pub(crate) fn is_shared(&self) -> bool {
        Arc::strong_count(&self.inner) > 1
    }
}

impl From<IcebergTableCacheValue> for CacheValue<IcebergTableCacheValue> {
    fn from(value: IcebergTableCacheValue) -> Self {
        CacheValue {
            inner: Arc::new(value),
            mem_bytes: 0,
        }
    }
}

impl From<ColumnData> for CacheValue<ColumnData> {
    fn from(value: ColumnData) -> Self {
        CacheValue {
            mem_bytes: value.size(),
            inner: Arc::new(value),
        }
    }
}

impl From<InvertedIndexMeta> for CacheValue<InvertedIndexMeta> {
    fn from(value: InvertedIndexMeta) -> Self {
        CacheValue {
            mem_bytes: value.memory_size(),
            inner: Arc::new(value),
        }
    }
}

impl From<InvertedIndexLookupBytes> for CacheValue<InvertedIndexLookupBytes> {
    fn from(value: InvertedIndexLookupBytes) -> Self {
        CacheValue {
            mem_bytes: std::mem::size_of::<InvertedIndexLookupBytes>() + value.data.len(),
            inner: Arc::new(value),
        }
    }
}

impl From<InvertedIndexPayloadBytes> for CacheValue<InvertedIndexPayloadBytes> {
    fn from(value: InvertedIndexPayloadBytes) -> Self {
        CacheValue {
            mem_bytes: std::mem::size_of::<InvertedIndexPayloadBytes>() + value.data.len(),
            inner: Arc::new(value),
        }
    }
}

impl From<ParquetMetaData> for CacheValue<ParquetMetaData> {
    fn from(value: ParquetMetaData) -> Self {
        CacheValue {
            inner: Arc::new(value),
            mem_bytes: 0,
        }
    }
}

impl From<SizedColumnArray> for CacheValue<SizedColumnArray> {
    fn from(value: SizedColumnArray) -> Self {
        CacheValue {
            mem_bytes: value.1,
            inner: Arc::new(value),
        }
    }
}

pub struct FileSize(pub u64);

impl From<FileSize> for CacheValue<FileSize> {
    fn from(value: FileSize) -> Self {
        CacheValue {
            mem_bytes: value.0 as usize,
            inner: Arc::new(value),
        }
    }
}

impl<T> MemSized for CacheValue<T> {
    fn mem_bytes(&self) -> usize {
        self.mem_bytes
    }
}
