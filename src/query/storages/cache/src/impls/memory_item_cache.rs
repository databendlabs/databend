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

use common_cache::Count;
use common_cache::DefaultHashBuilder;
use common_cache::LruCache;
use common_exception::Result;
use opendal::Object;
use parking_lot::RwLock;

use crate::CacheSettings;
use crate::ObjectCache;

type ItemCache<V> = RwLock<LruCache<String, Arc<V>, DefaultHashBuilder, Count>>;

pub struct MemoryItemCache<V> {
    lru: ItemCache<V>,
}

impl<V> MemoryItemCache<V> {
    pub fn create(settings: &CacheSettings) -> MemoryItemCache<V> {
        Self {
            lru: RwLock::new(LruCache::new(settings.memory_item_capacity)),
        }
    }
}

#[async_trait::async_trait]
impl<V> ObjectCache<V> for MemoryItemCache<V> {
    async fn read_object(&self, object: &Object, start: u64, end: u64) -> Result<V> {
        todo!()
    }

    async fn write_object(&self, object: &Object, t: V) -> Result<()> {
        todo!()
    }

    async fn remove_object(&self, object: &Object) -> Result<()> {
        todo!()
    }
}
