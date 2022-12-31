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

use common_cache::BytesMeter;
use common_cache::Cache;
use common_cache::Count;
use common_cache::DefaultHashBuilder;
use common_cache::LruCache;
use common_exception::Result;
use opendal::Object;
use parking_lot::RwLock;

use crate::CacheSettings;
use crate::ObjectCache;

pub type BytesCache = RwLock<LruCache<String, Arc<Vec<u8>>, DefaultHashBuilder, BytesMeter>>;

pub struct MemoryBytesCache {
    lru: BytesCache,
}

impl MemoryBytesCache {
    pub fn create(settings: &CacheSettings) -> MemoryBytesCache {
        Self {
            lru: RwLock::new(LruCache::with_meter_and_hasher(
                capacity,
                BytesMeter,
                DefaultHashBuilder::new(),
            )),
        }
    }
}

#[async_trait::async_trait]
impl ObjectCache<Vec<u8>> for MemoryBytesCache {
    async fn read_object(&self, object: &Object, start: u64, end: u64) -> Result<Vec<u8>> {
        todo!()
    }

    async fn write_object(&self, object: &Object, t: Vec<u8>) -> Result<()> {
        todo!()
    }

    async fn remove_object(&self, object: &Object) -> Result<()> {
        todo!()
    }
}
