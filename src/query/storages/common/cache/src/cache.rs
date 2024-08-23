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
use std::sync::Arc;

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
pub trait CacheAccessor: Send + Sync {
    type V: Send + Sync;

    fn get(&self, k: &String) -> Option<Arc<Self::V>>;
    fn get_sized(&self, k: &String, len: u64) -> Option<Arc<Self::V>>;

    fn insert(&self, key: String, value: Self::V) -> Arc<Self::V>;
    fn evict(&self, k: &String) -> bool;

    fn contains_key(&self, k: &String) -> bool;
    fn bytes_size(&self) -> u64;
    fn items_capacity(&self) -> u64;
    fn bytes_capacity(&self) -> u64;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn name(&self) -> &str;
}
