// Copyright 2022 Datafuse Labs.
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

use std::hash::BuildHasher;
use std::hash::Hash;
use std::sync::Arc;

use common_cache::Count;
use common_cache::CountableMeter;
use common_cache::DefaultHashBuilder;

// The cache accessor, crate users usually working on this interface while manipulating caches
pub trait CacheAccessor<K, V, S = DefaultHashBuilder, M = Count>
where
    K: Eq + Hash,
    S: BuildHasher,
    M: CountableMeter<K, Arc<V>>,
{
    fn get<Q: AsRef<str>>(&self, k: Q) -> Option<Arc<V>>;
    fn put(&self, key: K, value: Arc<V>);
    fn evict(&self, k: &str) -> bool;
    fn contains_key(&self, _k: &str) -> bool;
}
