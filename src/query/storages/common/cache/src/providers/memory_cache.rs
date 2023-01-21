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

use std::borrow::Borrow;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::sync::Arc;

use common_cache::BytesMeter;
use common_cache::Cache;
use common_cache::Count;
use common_cache::CountableMeter;
use common_cache::DefaultHashBuilder;
use common_cache::LruCache;
use parking_lot::RwLock;

use crate::cache::StorageCache;

pub type ItemCache<V> = LruCache<String, Arc<V>, DefaultHashBuilder, Count>;
pub type BytesCache = LruCache<String, Arc<Vec<u8>>, DefaultHashBuilder, BytesMeter>;

pub type InMemoryItemCacheHolder<T> = Arc<RwLock<ItemCache<T>>>;
pub type InMemoryBytesCacheHolder = Arc<RwLock<BytesCache>>;

pub struct InMemoryCacheBuilder;
impl InMemoryCacheBuilder {
    pub fn new_item_cache<V>(capacity: u64) -> InMemoryItemCacheHolder<V> {
        let cache = LruCache::new(capacity);
        Arc::new(RwLock::new(cache))
    }

    pub fn new_bytes_cache(capacity: u64) -> InMemoryBytesCacheHolder {
        let cache =
            LruCache::with_meter_and_hasher(capacity, BytesMeter, DefaultHashBuilder::new());
        Arc::new(RwLock::new(cache))
    }
}

impl<K, V, S, M> StorageCache<K, V> for LruCache<K, Arc<V>, S, M>
where
    M: CountableMeter<K, Arc<V>>,
    S: BuildHasher,
    K: Eq + Hash,
{
    type Meter = M;

    fn put(&mut self, key: K, value: Arc<V>) {
        Cache::put(self, key, value);
    }

    fn get<Q>(&mut self, k: &Q) -> Option<&Arc<V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        Cache::get(self, k)
    }

    fn evict<Q>(&mut self, k: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.pop(k).is_some()
    }
}
