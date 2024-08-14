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

use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;
use std::sync::Arc;

use databend_common_cache::Count;
use databend_common_cache::CountableMeter;
use databend_common_metrics::cache::*;

#[derive(Copy, Clone, Debug)]
pub enum Unit {
    Bytes,
    Count,
}

impl Display for Unit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Unit::Bytes => f.write_str("bytes"),
            Unit::Count => f.write_str("count"),
        }
    }
}

// The cache accessor, crate users usually working on this interface while manipulating caches
pub trait CacheAccessor {
    type V;
    type M: CountableMeter<String, Arc<Self::V>> = Count;

    fn get<Q: AsRef<str>>(&self, k: Q) -> Option<Arc<Self::V>>;
    fn get_sized<Q: AsRef<str>>(&self, k: Q, len: u64) -> Option<Arc<Self::V>>;

    fn put(&self, key: String, value: Arc<Self::V>);
    fn evict(&self, k: &str) -> bool;
    fn contains_key(&self, k: &str) -> bool;
    fn size(&self) -> u64;
    fn capacity(&self) -> u64;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn name(&self) -> &str;
}
