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
use std::hash::Hash;
use std::sync::Arc;

use moka::sync::Cache as MokaCache;

use crate::cache::StorageCache;

pub type ItemCache<V> = MokaCache<String, Arc<V>>;
pub type BytesCache = MokaCache<String, Arc<Vec<u8>>>;

pub type InMemoryItemCacheHolder<T> = ItemCache<T>;
pub type InMemoryBytesCacheHolder = BytesCache;

pub struct InMemoryCacheBuilder;
impl InMemoryCacheBuilder {
    pub fn new_item_cache<V: Send + Sync + 'static>(capacity: u64) -> InMemoryItemCacheHolder<V> {
        MokaCache::<String, Arc<V>>::new(capacity)
    }

    pub fn new_bytes_cache(capacity: u64) -> InMemoryBytesCacheHolder {
        MokaCache::builder()
            .weigher(|_key, value: &Arc<Vec<u8>>| -> u32 {
                value.len().try_into().unwrap_or(u32::MAX)
            })
            .max_capacity(capacity)
            .build()
    }
}

impl<K, V> StorageCache<K, V> for MokaCache<K, Arc<V>>
where
    K: Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn put(&self, key: K, value: Arc<V>) {
        MokaCache::insert(self, key, value);
    }

    fn get<Q>(&self, k: &Q) -> Option<Arc<V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        MokaCache::get(self, k)
    }

    fn evict<Q>(&self, k: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        MokaCache::invalidate(self, k);
    }
}
