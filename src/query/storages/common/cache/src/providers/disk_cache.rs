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

use crate::cache::StorageCache;

// TODO: local disk file based LRU/LFU/xxxx cache
pub struct DiskCache {}

impl<K, V> StorageCache<K, V> for DiskCache {
    type Meter = ();

    fn put(&mut self, _key: K, _value: Arc<V>) {
        todo!()
    }

    fn get<Q>(&mut self, _k: &Q) -> Option<&Arc<V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        todo!()
    }

    fn evict<Q>(&mut self, _k: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        todo!()
    }
}
