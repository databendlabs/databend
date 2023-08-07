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

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::time::Duration;
use std::time::Instant;

#[derive(Debug)]
struct TtlValue<V> {
    timeout: Instant,
    value: V,
}
/// `CleanPolicy` provides three ways to clean up data.
/// `CleanPolicy::ReadModify` triggers cleanup on every read or modification.
/// `CleanPolicy::Modify` triggers cleanup on every modification.
/// `CleanPolicy::Manual` never triggers cleanup automatically.
#[derive(Debug)]
pub enum CleanPolicy {
    ReadModify,
    Modify,
    Manual,
}
/// `TtlHashMap` is a `HashMap` wrapper with expiration features.
#[derive(Debug)]
pub struct TtlHashMap<K, V>
where K: Eq + Hash
{
    map: HashMap<K, TtlValue<V>>,

    ttl: Duration,

    pub clean_policy: CleanPolicy,
}

impl<K, V> TtlHashMap<K, V>
where K: Eq + Hash
{
    /// Create `TtlHashMap` with time-to-leval, `CleanPolicy::ReadModify` will be take for this initialization
    pub fn new(ttl: Duration) -> Self {
        Self::new_with_clean_policy(ttl, CleanPolicy::ReadModify)
    }

    /// Create `TtlHashMap` with time-to-leval and `CleanPolicy`
    pub fn new_with_clean_policy(ttl: Duration, clean_policy: CleanPolicy) -> Self {
        TtlHashMap {
            map: HashMap::new(),
            ttl,
            clean_policy,
        }
    }

    pub fn cleanup(&mut self) {
        let now = Instant::now();
        self.map.retain(|_, v| v.timeout > now)
    }

    pub fn contains_key<Q: ?Sized>(&self, k: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.map.contains_key(k)
    }

    pub fn get<Q: ?Sized>(&mut self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        if let CleanPolicy::ReadModify = self.clean_policy {
            self.cleanup()
        }

        self.map.get(k).map(|v| &(v.value))
    }

    pub fn get_mut<Q: ?Sized>(&mut self, k: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        if let CleanPolicy::ReadModify = self.clean_policy {
            self.cleanup()
        }

        self.map.get_mut(k).map(|v| &mut (v.value))
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        match self.clean_policy {
            CleanPolicy::ReadModify | CleanPolicy::Modify => self.cleanup(),
            _ => {}
        }

        let timeout = Instant::now() + self.ttl;
        let ret = self.map.insert(key, TtlValue { timeout, value });

        ret.map(|v| v.value)
    }

    pub fn remove<Q: ?Sized>(&mut self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        match self.clean_policy {
            CleanPolicy::ReadModify | CleanPolicy::Modify => self.cleanup(),
            _ => {}
        }
        self.map.remove(k).map(|v| v.value)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}
