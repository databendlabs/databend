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

use std::sync::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

pub struct ResultCacheState {
    partitions_shas: RwLock<Vec<String>>,
    cache_key_extras: RwLock<Vec<String>>,
    cacheable: AtomicBool,
}

impl Default for ResultCacheState {
    fn default() -> Self {
        Self {
            partitions_shas: Default::default(),
            cache_key_extras: Default::default(),
            cacheable: AtomicBool::new(true),
        }
    }
}

impl ResultCacheState {
    pub fn add_partitions_sha(&self, sha: String) {
        let mut shas = self.partitions_shas.write().unwrap();
        if !shas.contains(&sha) {
            shas.push(sha);
        }
    }

    pub fn partitions_shas(&self) -> Vec<String> {
        let mut shas = self.partitions_shas.read().unwrap().clone();
        shas.sort();
        shas
    }

    pub fn add_cache_key_extra(&self, extra: String) {
        let mut extras = self.cache_key_extras.write().unwrap();
        if !extras.contains(&extra) {
            extras.push(extra);
        }
    }

    pub fn cache_key_extras(&self) -> Vec<String> {
        let mut extras = self.cache_key_extras.read().unwrap().clone();
        extras.sort();
        extras
    }

    pub fn cacheable(&self) -> bool {
        self.cacheable.load(Ordering::Acquire)
    }

    pub fn set_cacheable(&self, cacheable: bool) {
        self.cacheable.store(cacheable, Ordering::Release);
    }
}
