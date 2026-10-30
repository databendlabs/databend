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

use databend_storages_common_index::IndexFile;
use databend_storages_common_index::InvertedIndexLookupBytes;
use databend_storages_common_index::InvertedIndexPayloadBytes;
use databend_storages_common_index::filters::FilterImpl;

use crate::CachedItem;
use crate::CachedVectorIndex;

impl CachedItem for FilterImpl {
    fn mem_bytes(&self) -> usize {
        FilterImpl::mem_bytes(self)
    }
}

impl CachedItem for CachedVectorIndex {
    fn mem_bytes(&self) -> usize {
        self.memory_size()
    }
}

impl CachedItem for IndexFile {
    fn mem_bytes(&self) -> usize {
        size_of::<Self>() + self.data.len()
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
