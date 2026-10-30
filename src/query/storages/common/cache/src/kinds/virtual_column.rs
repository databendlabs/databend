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

//! Footers of virtual column files.

use std::mem::size_of;
use std::sync::Arc;

use bytes::Bytes;
use databend_common_exception::Result;
use databend_storages_common_index::VirtualColumnFileMeta;
use databend_storages_common_index::VirtualColumnNode;

use crate::CacheDef;
use crate::CachedItem;
use crate::Disk;
use crate::DiskCodec;
use crate::Pool;
use crate::size::hash_map;
use crate::size::vec;

/// Virtual column file footers, by file location.
pub struct Meta;

impl CacheDef for Meta {
    type Value = Arc<VirtualColumnFileMeta>;
    const NAME: &'static str = "memory_cache_virtual_column_file_meta_data";
    const POOL: Pool = Pool::Meta;
    const DISK: Option<Disk<Self::Value>> = Some(Disk::new("virtual_column_meta_v1", |c| {
        c.disk_cache_virtual_column_meta_size
    }));
}

fn virtual_column_node(n: &VirtualColumnNode) -> usize {
    hash_map(&n.children, |_, child| virtual_column_node(child))
}

impl CachedItem for VirtualColumnFileMeta {
    fn mem_bytes(&self) -> usize {
        size_of::<Self>()
            + vec(&self.string_table, |s| s.capacity())
            + vec(&self.column_metas, |_| 0)
            + hash_map(&self.typed_shared_column_metas, |_, metas| {
                hash_map(metas, |_, _| 0)
            })
            + hash_map(&self.virtual_column_nodes, |_, n| virtual_column_node(n))
    }
}

impl DiskCodec for VirtualColumnFileMeta {
    fn encode(&self) -> Result<Vec<u8>> {
        Vec::<u8>::try_from(self)
    }

    fn decode(bytes: Bytes) -> Result<Self> {
        Self::try_from(bytes)
    }
}
