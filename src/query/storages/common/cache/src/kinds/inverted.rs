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

//! Inverted indexes: the footer of each index file, then whole lookup files or 64 KiB pages.

use std::mem::size_of;

use bytes::Bytes;
use databend_common_exception::Result;
use databend_storages_common_index::InvertedIndexLookupBytes;
use databend_storages_common_index::InvertedIndexMeta;
use databend_storages_common_index::InvertedIndexPayloadBytes;

use crate::CacheDef;
use crate::CachedItem;
use crate::Disk;
use crate::DiskCodec;
use crate::Pool;

/// Persisted footers, by `inverted_index_meta_cache_key`.
pub struct Meta;

impl CacheDef for Meta {
    type Value = InvertedIndexMeta;
    const NAME: &'static str = "memory_cache_inverted_index_meta_data";
    const POOL: Pool = Pool::Meta;
    const DISK: Option<Disk<Self::Value>> = Some(Disk::new("inverted_index_meta_v1", |c| {
        c.disk_cache_inverted_index_meta_size
    }));
}

/// Term dictionaries and small fieldnorm / fast files, whole or paged.
pub struct Lookup;

impl CacheDef for Lookup {
    type Value = InvertedIndexLookupBytes;
    const NAME: &'static str = "memory_cache_inverted_index_lookup";
    const POOL: Pool = Pool::Index;
    const DISK: Option<Disk<Self::Value>> = Some(Disk::new("inverted_index_lookup_v1", |c| {
        c.disk_cache_inverted_index_lookup_size
    }));
}

/// Pages of postings, positions, store and large fieldnorm / fast files.
pub struct Payload;

impl CacheDef for Payload {
    type Value = InvertedIndexPayloadBytes;
    const NAME: &'static str = "memory_cache_inverted_index_payload";
    const POOL: Pool = Pool::Index;
    const DISK: Option<Disk<Self::Value>> = Some(Disk::new("inverted_index_payload_v1", |c| {
        c.disk_cache_inverted_index_payload_size
    }));
}

/// Complete `.idx` / `.pos` components, apart from pages so warmups cannot evict hot pages.
pub struct FullPayload;

impl CacheDef for FullPayload {
    type Value = InvertedIndexPayloadBytes;
    const NAME: &'static str = "memory_cache_inverted_index_full_payload";
    const POOL: Pool = Pool::Index;
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

impl DiskCodec for InvertedIndexMeta {
    fn encode(&self) -> Result<Vec<u8>> {
        Vec::<u8>::try_from(self)
    }

    fn decode(bytes: Bytes) -> Result<Self> {
        Self::try_from(bytes)
    }
}

impl DiskCodec for InvertedIndexLookupBytes {
    fn encode(&self) -> Result<Vec<u8>> {
        Vec::<u8>::try_from(self)
    }

    fn decode(bytes: Bytes) -> Result<Self> {
        Self::try_from(bytes)
    }
}

impl DiskCodec for InvertedIndexPayloadBytes {
    fn encode(&self) -> Result<Vec<u8>> {
        Vec::<u8>::try_from(self)
    }

    fn decode(bytes: Bytes) -> Result<Self> {
        Self::try_from(bytes)
    }
}
