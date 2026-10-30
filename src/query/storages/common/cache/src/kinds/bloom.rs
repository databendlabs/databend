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

//! Bloom indexes: the footer of each index file, and the filter of each column.

use std::mem::size_of;
use std::sync::Arc;

use bytes::Bytes;
use databend_common_exception::Result;
use databend_storages_common_index::BloomIndexMeta;
use databend_storages_common_index::filters::FilterImpl;

use crate::CacheDef;
use crate::CachedItem;
use crate::Disk;
use crate::DiskCodec;
use crate::Pool;
use crate::size::vec;

/// Footers of bloom index files, by index location.
pub struct IndexMeta;

impl CacheDef for IndexMeta {
    type Value = Arc<BloomIndexMeta>;
    const NAME: &'static str = "memory_cache_bloom_index_file_meta_data";
    const POOL: Pool = Pool::Meta;
    const DISK: Option<Disk<Self::Value>> = Some(Disk::new("bloom_meta_v1", |c| {
        c.disk_cache_table_bloom_index_meta_size
    }));
}

/// Bloom filters, one per index column, by `{index_location}-{filter_name}`.
pub struct Filter;

impl CacheDef for Filter {
    type Value = Arc<FilterImpl>;
    const NAME: &'static str = "memory_cache_bloom_index_filter";
    const POOL: Pool = Pool::Index;
    const DISK: Option<Disk<Self::Value>> = Some(Disk::new("bloom_filter_v1", |c| {
        c.disk_cache_table_bloom_index_data_size
    }));
}

impl CachedItem for BloomIndexMeta {
    fn mem_bytes(&self) -> usize {
        size_of::<Self>() + vec(&self.columns, |(name, _)| name.capacity())
    }
}

impl CachedItem for FilterImpl {
    fn mem_bytes(&self) -> usize {
        FilterImpl::mem_bytes(self)
    }
}

impl DiskCodec for BloomIndexMeta {
    fn encode(&self) -> Result<Vec<u8>> {
        Vec::<u8>::try_from(self)
    }

    fn decode(bytes: Bytes) -> Result<Self> {
        Self::try_from(bytes)
    }
}

impl DiskCodec for FilterImpl {
    fn encode(&self) -> Result<Vec<u8>> {
        Vec::<u8>::try_from(self)
    }

    fn decode(bytes: Bytes) -> Result<Self> {
        Self::try_from(bytes)
    }
}
