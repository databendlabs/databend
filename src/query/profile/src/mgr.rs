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

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use databend_common_base::base::GlobalInstance;

use crate::prof::QueryProfile;

/// Default capacity of the LRU cache of query profiles.
const DEFAULT_QUERY_PROFILE_LIMIT: usize = 20;

/// Manager of query profiling.
/// This is a singleton in every databend-query process.
pub struct QueryProfileManager {
    /// The LRU cache of query profiles.
    profiles: Lru,
}

impl QueryProfileManager {
    fn new(capacity: usize) -> Self {
        QueryProfileManager {
            profiles: Lru::new(capacity),
        }
    }

    pub fn init() {
        GlobalInstance::set(Arc::new(Self::new(DEFAULT_QUERY_PROFILE_LIMIT)));
    }

    pub fn instance() -> Arc<Self> {
        GlobalInstance::get()
    }

    /// Try to get the query profile by query ID.
    pub fn get(&self, query_id: &str) -> Option<Arc<QueryProfile>> {
        self.profiles.get(query_id)
    }

    /// Inserts a query profile.
    pub fn insert(&self, query_profile: Arc<QueryProfile>) {
        self.profiles
            .insert(query_profile.query_id.clone(), query_profile);
    }

    /// Lists all query profiles.
    pub fn list_all(&self) -> Vec<Arc<QueryProfile>> {
        self.profiles.list_all()
    }
}

impl Default for QueryProfileManager {
    fn default() -> Self {
        QueryProfileManager::new(DEFAULT_QUERY_PROFILE_LIMIT)
    }
}

/// An LRU cache of query profiles. The expired query profiles
/// will be removed.
struct Lru {
    /// The maximum number of query profiles to keep in memory.
    /// If the number of query profiles exceeds this number,
    /// the oldest one will be removed.
    capacity: usize,

    /// The query profiles.
    /// The key is the query ID.
    /// The value is the query profile.
    profiles: DashMap<String, Arc<QueryProfile>>,

    /// An LRU list of query IDs.
    lru: Mutex<VecDeque<String>>,
}

impl Lru {
    /// Creates a new LRU cache.
    pub fn new(capacity: usize) -> Self {
        Lru {
            capacity,
            profiles: DashMap::with_capacity(capacity),
            lru: Mutex::new(VecDeque::with_capacity(capacity)),
        }
    }

    /// Gets the query profile by the query ID.
    /// Notice that this method required to acquire the shared lock of the LRU list.
    /// So don't call this method when the lock is already acquired.
    pub fn get(&self, query_id: &str) -> Option<Arc<QueryProfile>> {
        self.profiles.get(query_id).map(|v| v.value().clone())
    }

    /// Inserts a query profile.
    /// This operation is thread-safe.
    pub fn insert(&self, query_id: String, query_profile: Arc<QueryProfile>) {
        // Lock the LRU list to ensure the consistency between the LRU list and the query profiles.
        let mut lru = self.lru.lock().unwrap();

        if let Entry::Occupied(mut prof) = self.profiles.entry(query_id.clone()) {
            prof.insert(query_profile);
            return;
        }

        if self.profiles.len() >= self.capacity {
            if let Some(query_id) = lru.pop_front() {
                self.profiles.remove(&query_id);
            }
        }

        self.profiles.insert(query_id.clone(), query_profile);
        lru.push_back(query_id);
    }

    /// Lists all query profiles.
    /// Notice that this method required to acquire the shared lock of the LRU list.
    /// So don't call this method when the lock is already acquired.
    pub fn list_all(&self) -> Vec<Arc<QueryProfile>> {
        self.profiles.iter().map(|v| v.value().clone()).collect()
    }
}
