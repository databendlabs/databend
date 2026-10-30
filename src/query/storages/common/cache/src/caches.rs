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

pub use crate::cache_items::*;

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

/// A deserialized column and its uncompressed size in bytes.
pub type SizedColumnArray = (CachedColumnArray, usize);

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
}

impl<T> MemSized for CacheValue<T> {
    fn mem_bytes(&self) -> usize {
        self.mem_bytes
    }
}
