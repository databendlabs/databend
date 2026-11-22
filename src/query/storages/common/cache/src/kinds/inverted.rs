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

//! Inverted indexes: the footer of each index file, then whole lookup files or 64 KiB pages, the
//! bundles opened for search, and the term infos their searches looked up.

use std::mem::size_of;

use databend_storages_common_index::InvertedIndexLookupBytes;
use databend_storages_common_index::InvertedIndexMeta;
use databend_storages_common_index::InvertedIndexPayloadBytes;
use databend_storages_common_index::InvertedIndexSearcher;
use databend_storages_common_index::InvertedIndexTermInfo;

use crate::CacheDef;
use crate::CachedItem;
use crate::Pool;

/// Persisted footers, by `inverted_index_meta_cache_key`.
pub struct Meta;

impl CacheDef for Meta {
    type Value = InvertedIndexMeta;
    const NAME: &'static str = "memory_cache_inverted_index_meta_data";
    const POOL: Pool = Pool::Meta;
    const DISK: bool = true;
}

/// Term dictionaries and small fieldnorm / fast files, whole or paged.
pub struct Lookup;

impl CacheDef for Lookup {
    type Value = InvertedIndexLookupBytes;
    const NAME: &'static str = "memory_cache_inverted_index_lookup";
    const POOL: Pool = Pool::Index;
    const DISK: bool = true;
}

/// Pages of postings, positions, store and large fieldnorm / fast files.
pub struct Payload;

impl CacheDef for Payload {
    type Value = InvertedIndexPayloadBytes;
    const NAME: &'static str = "memory_cache_inverted_index_payload";
    const POOL: Pool = Pool::Index;
    const DISK: bool = true;
}

/// Complete `.idx` / `.pos` components, apart from pages so warmups cannot evict hot pages.
pub struct FullPayload;

impl CacheDef for FullPayload {
    type Value = InvertedIndexPayloadBytes;
    const NAME: &'static str = "memory_cache_inverted_index_full_payload";
    const POOL: Pool = Pool::Index;
    const DISK: bool = true;
}

/// Bundles opened once and shared by the searches of their block, by location and size. Each
/// is charged the most its shared pin keeps: see `InvertedIndexSearcher`.
///
/// Loads are not shared: each query opens a bundle with the storage operator of its table, so
/// a load failing with an old connection (`ALTER TABLE ... CONNECTION`) fails no other query.
pub struct Searcher;

impl CacheDef for Searcher {
    type Value = InvertedIndexSearcher;
    const NAME: &'static str = "memory_cache_inverted_index_searcher";
    const POOL: Pool = Pool::Index;
}

impl CachedItem for InvertedIndexSearcher {
    fn mem_bytes(&self) -> usize {
        self.memory_size()
    }
}

/// The infos of the terms the searches of each opened searcher looked up in its dictionary, by
/// `InvertedIndexSearcher::term_key`: a later search of the searcher reads the postings of a
/// term and scores it without looking it up again. The infos of a searcher that was dropped are
/// never looked up again, and leave as the pool turns over.
pub struct TermInfo;

impl CacheDef for TermInfo {
    type Value = InvertedIndexTermInfo;
    const NAME: &'static str = "memory_cache_inverted_index_term_info";
    const POOL: Pool = Pool::Index;
}

/// What the shared backend allocates for an entry of a term info, apart from the bytes of its
/// key: its slot in the pool with its share of the table and the free list, and the `Arc` of
/// the value with its reservation. Several times the info itself; other kinds leave it out,
/// their values being far larger. `test_term_info_is_charged_what_its_entry_takes` measures it,
/// about 300 bytes on 64-bit targets.
const TERM_INFO_ENTRY_BYTES: usize = 300;

impl CachedItem for InvertedIndexTermInfo {
    fn mem_bytes(&self) -> usize {
        TERM_INFO_ENTRY_BYTES + self.key_len
    }
}

impl CachedItem for InvertedIndexMeta {
    fn mem_bytes(&self) -> usize {
        self.memory_size()
    }
}

impl CachedItem for InvertedIndexLookupBytes {
    fn mem_bytes(&self) -> usize {
        size_of::<Self>() + self.data.len()
    }
}

impl CachedItem for InvertedIndexPayloadBytes {
    fn mem_bytes(&self) -> usize {
        size_of::<Self>() + self.data.len()
    }
}
