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

//! Column data: raw column chunks as read from storage, and deserialized columns.

use arrow::array::ArrayRef;
use bytes::Bytes;
use databend_common_expression::Column;

use crate::CacheDef;
use crate::CachedItem;
use crate::KindLimit;
use crate::Pool;

/// Raw column chunks. Readers keep them only in the disk cache (`disk_cache_column_data`), whose
/// data pool they fill: in memory, `Array` serves re-reads decoded. The memory side is off by
/// default and no reader fills it; `data_cache_in_memory_bytes` is no longer read.
pub struct Data;

impl CacheDef for Data {
    type Value = ColumnData;
    const NAME: &'static str = "memory_cache_column_data";
    const POOL: Pool = Pool::Data;
    const SHARED_DEFAULT: KindLimit = KindLimit::DISABLED;
    const DISK: bool = true;
}

/// Deserialized columns, by `TableDataCacheKey` with the type. Off by default in the legacy
/// backend, as on `main`.
pub struct Array;

impl CacheDef for Array {
    type Value = SizedColumnArray;
    const NAME: &'static str = "memory_cache_table_data";
    const POOL: Pool = Pool::Data;
}

pub struct ColumnData(Bytes);

impl ColumnData {
    pub fn from_bytes(bytes: Bytes) -> Self {
        ColumnData(bytes)
    }

    pub fn bytes(&self) -> Bytes {
        self.0.clone()
    }

    pub fn size(&self) -> usize {
        self.0.len()
    }
}

/// Key of `Array`: the block, the column, its column chunk and its logical type. Deserialized
/// arrays carry type metadata, so schema evolution must invalidate them even when the Parquet
/// column chunk is unchanged.
#[derive(Clone)]
pub struct TableDataCacheKey {
    cache_key: String,
}

impl TableDataCacheKey {
    pub fn new(block_path: &str, column_id: u32, offset: u64, len: u64, data_type: &str) -> Self {
        Self {
            cache_key: format!("{block_path}-{column_id}-{offset}-{len}-{data_type}"),
        }
    }
}

impl From<TableDataCacheKey> for String {
    fn from(value: TableDataCacheKey) -> Self {
        value.cache_key
    }
}

impl AsRef<str> for TableDataCacheKey {
    fn as_ref(&self) -> &str {
        &self.cache_key
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

impl CachedItem for ColumnData {
    fn mem_bytes(&self) -> usize {
        self.size()
    }
}

impl CachedItem for SizedColumnArray {
    fn mem_bytes(&self) -> usize {
        self.1
    }
}
