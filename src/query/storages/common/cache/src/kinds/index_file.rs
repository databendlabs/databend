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

//! The generic index file and its footer, the values of the spatial and vector index kinds.

use std::mem::size_of;

use bytes::Bytes;
use databend_common_exception::Result;
use databend_storages_common_index::IndexFile;
use databend_storages_common_index::IndexMeta;

use crate::CachedItem;
use crate::DiskCodec;
use crate::size::btree_map;
use crate::size::vec;

/// Heap bytes of an index footer.
pub(super) fn index_meta(m: &IndexMeta) -> usize {
    vec(&m.columns, |(name, _)| name.capacity())
        + btree_map(&m.metadata, |k, v| k.capacity() + v.capacity())
}

impl CachedItem for IndexMeta {
    fn mem_bytes(&self) -> usize {
        size_of::<Self>() + index_meta(self)
    }

    fn legacy_bytes(&self) -> usize {
        0
    }
}

impl CachedItem for IndexFile {
    fn mem_bytes(&self) -> usize {
        size_of::<Self>() + self.data.len()
    }
}

impl DiskCodec for IndexMeta {
    fn encode(&self) -> Result<Vec<u8>> {
        Vec::<u8>::try_from(self)
    }

    fn decode(bytes: Bytes) -> Result<Self> {
        Self::try_from(bytes)
    }
}

impl DiskCodec for IndexFile {
    fn encode(&self) -> Result<Vec<u8>> {
        Vec::<u8>::try_from(self)
    }

    fn decode(bytes: Bytes) -> Result<Self> {
        Self::try_from(bytes)
    }
}
