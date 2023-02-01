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

use std::sync::Arc;

pub trait CacheAccessor<K, V> {
    fn get(&self, k: &str) -> Option<Arc<V>>;
    fn put(&self, key: K, value: Arc<V>);
    fn evict(&self, k: &str) -> bool;
}

/// The minimum interface that cache providers should implement
pub trait StorageCache<K, V> {
    type Meter;
    type CachedItem;

    fn put(&mut self, key: K, value: Arc<V>);

    fn get(&mut self, k: &str) -> Option<Self::CachedItem>;

    fn evict(&mut self, k: &str) -> bool;
}

mod impls {
    use std::sync::Arc;

    use parking_lot::RwLock;

    use crate::cache::CacheAccessor;
    use crate::cache::StorageCache;

    impl<V, C> CacheAccessor<String, V> for Arc<RwLock<C>>
    where C: StorageCache<String, V, CachedItem = Arc<V>>
    {
        fn get(&self, k: &str) -> Option<C::CachedItem> {
            let mut guard = self.write();
            guard.get(k)
        }

        fn put(&self, k: String, v: Arc<V>) {
            let mut guard = self.write();
            guard.put(k, v);
        }

        fn evict(&self, k: &str) -> bool {
            let mut guard = self.write();
            guard.evict(k)
        }
    }

    impl<V, C> CacheAccessor<String, V> for Option<C>
    where C: CacheAccessor<String, V>
    {
        fn get(&self, k: &str) -> Option<Arc<V>> {
            self.as_ref().and_then(|cache| cache.get(k))
        }

        fn put(&self, k: String, v: Arc<V>) {
            if let Some(cache) = self {
                cache.put(k, v);
            }
        }

        fn evict(&self, k: &str) -> bool {
            if let Some(cache) = self {
                cache.evict(k)
            } else {
                false
            }
        }
    }
}
