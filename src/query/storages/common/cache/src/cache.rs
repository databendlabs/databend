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

pub trait CacheAccessor<K, V> {
    fn get<Q>(&self, k: &Q) -> Option<Arc<V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;

    fn put(&self, key: K, value: Arc<V>);
    fn evict<Q>(&self, k: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;
}

/// The minimum interface that cache providers should implement
pub trait StorageCache<K, V> {
    type Meter;
    fn put(&mut self, key: K, value: Arc<V>);

    fn get<Q>(&mut self, k: &Q) -> Option<&Arc<V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;

    fn evict<Q>(&mut self, k: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;
}

mod impls {
    use std::borrow::Borrow;
    use std::hash::Hash;
    use std::sync::Arc;

    use parking_lot::RwLock;

    use crate::cache::CacheAccessor;
    use crate::cache::StorageCache;

    impl<V, C> CacheAccessor<String, V> for Arc<RwLock<C>>
    where C: StorageCache<String, V>
    {
        fn get<Q>(&self, k: &Q) -> Option<Arc<V>>
        where
            String: Borrow<Q>,
            Q: Hash + Eq + ?Sized,
        {
            let mut guard = self.write();
            guard.get(k).cloned()
        }

        fn put(&self, k: String, v: Arc<V>) {
            let mut guard = self.write();
            guard.put(k, v);
        }

        fn evict<Q>(&self, k: &Q) -> bool
        where
            String: Borrow<Q>,
            Q: Hash + Eq + ?Sized,
        {
            let mut guard = self.write();
            guard.evict(k)
        }
    }

    impl<V, C> CacheAccessor<String, V> for Option<Arc<RwLock<C>>>
    where C: StorageCache<String, V>
    {
        fn get<Q>(&self, k: &Q) -> Option<Arc<V>>
        where
            String: Borrow<Q>,
            Q: Hash + Eq + ?Sized,
        {
            self.as_ref().and_then(|cache| cache.get(k))
        }

        fn put(&self, k: String, v: Arc<V>) {
            if let Some(cache) = self {
                cache.put(k, v);
            }
        }

        fn evict<Q>(&self, k: &Q) -> bool
        where
            String: Borrow<Q>,
            Q: Hash + Eq + ?Sized,
        {
            if let Some(cache) = self {
                cache.evict(k)
            } else {
                false
            }
        }
    }
}
