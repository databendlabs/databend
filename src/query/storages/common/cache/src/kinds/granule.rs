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

use arrow::array::Array;
use arrow::array::ArrayRef;

use crate::CacheDef;
use crate::CachedItem;
use crate::Pool;

/// Decoded mark columns of granule index files: the per-granule byte offsets of a block's
/// columns and the per-granule cluster-key mins, keyed by `{file_path}-{mark_name}`. A file is
/// written once under its location, so an entry never goes stale.
pub struct Marks;

impl CacheDef for Marks {
    type Value = GranuleMarkArray;
    const NAME: &'static str = "memory_cache_granule_index_marks";
    const POOL: Pool = Pool::Index;
}

/// One decoded mark column, one value per granule.
#[derive(Clone, Debug)]
pub struct GranuleMarkArray(pub ArrayRef);

impl CachedItem for GranuleMarkArray {
    fn mem_bytes(&self) -> usize {
        self.0.get_array_memory_size()
    }
}
