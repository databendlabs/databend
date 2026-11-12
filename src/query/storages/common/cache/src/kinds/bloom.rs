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

//! Bloom indexes: the footer of each index file, and the filter of each column.

use std::mem::size_of;
use std::sync::Arc;

use databend_storages_common_index::BloomIndexMeta;
use databend_storages_common_index::filters::FilterImpl;

use crate::CacheDef;
use crate::CachedItem;
use crate::Pool;
use crate::size::vec;

/// Footers of bloom index files, by index location.
pub struct IndexMeta;

impl CacheDef for IndexMeta {
    type Value = Arc<BloomIndexMeta>;
    const NAME: &'static str = "memory_cache_bloom_index_file_meta_data";
    const POOL: Pool = Pool::Meta;
    /// Index files are written once, under new locations.
    const SHARE_LOADS: bool = true;
    const DISK: bool = true;
}

/// Bloom filters, one per index column, by `{index_location}-{filter_name}`.
pub struct Filter;

impl CacheDef for Filter {
    type Value = Arc<FilterImpl>;
    const NAME: &'static str = "memory_cache_bloom_index_filter";
    const POOL: Pool = Pool::Index;
    const DISK: bool = true;
}

impl CachedItem for BloomIndexMeta {
    fn mem_bytes(&self) -> usize {
        size_of::<Self>() + vec(&self.columns, |(name, _)| name.capacity())
    }

    fn legacy_bytes(&self) -> usize {
        0
    }
}

impl CachedItem for FilterImpl {
    fn mem_bytes(&self) -> usize {
        FilterImpl::mem_bytes(self)
    }
}
