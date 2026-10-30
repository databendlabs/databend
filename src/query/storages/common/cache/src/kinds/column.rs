// Copyright 2026 Datafuse Labs
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

//! Column data: raw bytes as read from storage, and deserialized columns.

use arrow::array::ArrayRef;
use bytes::Bytes;
use databend_common_config::CacheStorageTypeInnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;

use crate::CacheDef;
use crate::CachedItem;
use crate::Disk;
use crate::DiskCodec;
use crate::KindLimit;
use crate::Pool;

/// Raw column bytes, by `TableDataCacheKey` without the type. Off in the shared backend
/// unless `cache.kind_max_bytes` sizes it; its disk tier is the table data cache.
pub struct Data;

impl CacheDef for Data {
    type Value = ColumnData;
    const NAME: &'static str = "memory_cache_column_data";
    const POOL: Pool = Pool::Data;
    const SHARED_LIMIT: KindLimit = KindLimit::DISABLED;
    const DISK: Option<Disk<Self::Value>> = Some(
        Disk::new("v1", |c| match c.data_cache_storage {
            CacheStorageTypeInnerConfig::Disk => c.disk_cache_config.max_bytes,
            CacheStorageTypeInnerConfig::None => 0,
        })
        .reload_policy(|c| c.data_cache_key_reload_policy.clone()),
    );
}

/// Deserialized columns, by `TableDataCacheKey` with the type. Off in the shared backend
/// unless `cache.kind_max_bytes` sizes it.
pub struct Array;

impl CacheDef for Array {
    type Value = SizedColumnArray;
    const NAME: &'static str = "memory_cache_table_data";
    const POOL: Pool = Pool::Data;
    const SHARED_LIMIT: KindLimit = KindLimit::DISABLED;
}

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

impl TryFrom<&ColumnData> for Vec<u8> {
    type Error = ErrorCode;
    fn try_from(value: &ColumnData) -> std::result::Result<Self, Self::Error> {
        Ok(value.0.to_vec())
    }
}

// copying should be avoided
impl TryFrom<Bytes> for ColumnData {
    type Error = ErrorCode;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        Ok(ColumnData(value))
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

impl DiskCodec for ColumnData {
    fn encode(&self) -> Result<Vec<u8>> {
        Vec::<u8>::try_from(self)
    }

    fn decode(bytes: Bytes) -> Result<Self> {
        Self::try_from(bytes)
    }
}
