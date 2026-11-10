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

//! Vector indexes: the footer of each index file, decoded HNSW indexes, and FP16 refine pages.

use std::mem::size_of;
use std::mem::size_of_val;
use std::sync::Arc;

use databend_storages_common_index::HNSWIndex;
use databend_storages_common_index::VectorIndexFile;
use databend_storages_common_index::VectorIndexMeta;

use super::index_file::index_meta;
use crate::CacheDef;
use crate::CachedItem;
use crate::Pool;
use crate::size::btree_map;

/// Footers of vector index files, by index location.
pub struct Meta;

impl CacheDef for Meta {
    type Value = Arc<VectorIndexMeta>;
    const NAME: &'static str = "memory_cache_vector_index_file_meta_data";
    const POOL: Pool = Pool::Meta;
    /// Index files are written once, under new locations.
    const SHARE_LOADS: bool = true;
    const DISK: bool = true;
}

/// Decoded HNSW indexes, by index location and column. The compressed component bytes they are
/// decoded from are kept in the disk cache: every query of a table searches the HNSW index of
/// each of its blocks, so after a restart or an eviction they are read from local disk, not
/// from storage.
pub struct Index;

impl CacheDef for Index {
    type Value = CachedVectorIndex;
    const NAME: &'static str = "memory_cache_vector_index_file";
    const POOL: Pool = Pool::Index;
    const DISK: bool = true;
}

/// FP16 refine pages, by `{index_location}:{page}`.
pub struct Refine;

impl CacheDef for Refine {
    type Value = VectorIndexFile;
    const NAME: &'static str = "memory_cache_vector_index_refine";
    const POOL: Pool = Pool::Index;
    const DISK: bool = true;
}

/// A decoded HNSW index. Readers holding it keep it charged to the cache, so pinned indexes
/// count against the budget.
pub struct CachedVectorIndex {
    index: HNSWIndex,
    memory_size: usize,
}

impl CachedVectorIndex {
    pub fn new(index: HNSWIndex) -> Self {
        let memory_size = index.memory_size() + size_of::<Self>() - size_of::<HNSWIndex>()
            + 2 * size_of::<usize>(); // Arc reference counts.
        Self { index, memory_size }
    }

    pub fn index(&self) -> &HNSWIndex {
        &self.index
    }

    pub fn memory_size(&self) -> usize {
        self.memory_size
    }
}

impl CachedItem for CachedVectorIndex {
    fn mem_bytes(&self) -> usize {
        self.memory_size()
    }
}

impl CachedItem for VectorIndexMeta {
    fn mem_bytes(&self) -> usize {
        size_of::<Self>()
            + match self {
                VectorIndexMeta::Legacy(meta) => index_meta(meta),
                VectorIndexMeta::Bundle(footer) => {
                    2 * size_of::<usize>()
                        + size_of_val(footer.as_ref())
                        + footer.index_name.capacity()
                        + footer.index_version.capacity()
                        + btree_map(&footer.components, |k, _| k.capacity())
                }
            }
    }

    fn legacy_bytes(&self) -> usize {
        0
    }
}
