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

use databend_storages_common_index::HNSWIndex;

/// A decoded HNSW index, cached as `CacheKind::VectorIndex`. Readers holding it keep it
/// charged to the cache, so pinned indexes count against the budget.
pub struct CachedVectorIndex {
    index: HNSWIndex,
    memory_size: usize,
}

impl CachedVectorIndex {
    pub fn new(index: HNSWIndex) -> Self {
        let memory_size = index.memory_size() + std::mem::size_of::<Self>()
            - std::mem::size_of::<HNSWIndex>()
            + 2 * std::mem::size_of::<usize>(); // Arc reference counts.
        Self { index, memory_size }
    }

    pub fn index(&self) -> &HNSWIndex {
        &self.index
    }

    pub fn memory_size(&self) -> usize {
        self.memory_size
    }
}
