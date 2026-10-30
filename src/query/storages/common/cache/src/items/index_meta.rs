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

use std::mem::size_of;

use databend_storages_common_index::BloomIndexMeta;
use databend_storages_common_index::IndexMeta;
use databend_storages_common_index::InvertedIndexMeta;
use databend_storages_common_index::VectorIndexMeta;
use databend_storages_common_index::VirtualColumnFileMeta;
use databend_storages_common_index::VirtualColumnNode;

use super::size::btree_map;
use super::size::hash_map;
use super::size::vec;
use crate::CachedItem;

fn index_meta(m: &IndexMeta) -> usize {
    vec(&m.columns, |(name, _)| name.capacity())
        + btree_map(&m.metadata, |k, v| k.capacity() + v.capacity())
}

fn virtual_column_node(n: &VirtualColumnNode) -> usize {
    hash_map(&n.children, |_, child| virtual_column_node(child))
}

impl CachedItem for BloomIndexMeta {
    fn mem_bytes(&self) -> usize {
        size_of::<Self>() + vec(&self.columns, |(name, _)| name.capacity())
    }
}

impl CachedItem for IndexMeta {
    fn mem_bytes(&self) -> usize {
        size_of::<Self>() + index_meta(self)
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
}

impl CachedItem for InvertedIndexMeta {
    fn mem_bytes(&self) -> usize {
        self.memory_size()
    }
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
