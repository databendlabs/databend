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

pub trait CacheAccessor<K, V>: Clone {
    fn get<Q>(&self, k: &Q) -> Option<Arc<V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;

    fn put(&self, key: K, value: Arc<V>);
    fn evict<Q>(&self, k: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;
}

/// The minimum interface that cache providers should implement
pub trait StorageCache<K, V>: Clone {
    fn put(&self, key: K, value: Arc<V>);

    fn get<Q>(&self, k: &Q) -> Option<Arc<V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;

    fn evict<Q>(&self, k: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;
}

mod impls {
    use std::borrow::Borrow;
    use std::hash::Hash;
    use std::sync::Arc;

    use crate::cache::CacheAccessor;
    use crate::cache::StorageCache;

    impl<V, C> CacheAccessor<String, V> for C
    where C: StorageCache<String, V> + Clone
    {
        fn get<Q>(&self, k: &Q) -> Option<Arc<V>>
        where
            String: Borrow<Q>,
            Q: Hash + Eq + ?Sized,
        {
            StorageCache::get(self, k)
        }

        fn put(&self, k: String, v: Arc<V>) {
            StorageCache::put(self, k, v)
        }

        fn evict<Q>(&self, k: &Q)
        where
            String: Borrow<Q>,
            Q: Hash + Eq + ?Sized,
        {
            StorageCache::evict(self, k)
        }
    }
}
