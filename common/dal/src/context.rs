// Copyright 2021 Datafuse Labs.
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

use common_cache::LruDiskCache;
use common_exception::Result;
use common_infallible::RwLock;

use crate::cache::DalCache;

#[derive(Clone, Debug, Default)]
pub struct DalMetrics {
    pub read_bytes: usize,
    pub write_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct DalContext {
    metrics: Arc<RwLock<DalMetrics>>,
    pub cache: Arc<Option<DalCache>>,
}

impl DalContext {
    pub fn create(
        table_cache_enabled: bool,
        path: String,
        cache_size_mb: u64,
        cache_buffer_mb_size: u64,
    ) -> Result<Self> {
        let cache = if table_cache_enabled {
            let bytes_size = cache_size_mb * 1024 * 1024;
            let cache = Arc::new(common_base::tokio::sync::RwLock::new(LruDiskCache::new(
                path, bytes_size,
            )?));
            Arc::new(Some(DalCache::create(cache, cache_buffer_mb_size)))
        } else {
            Arc::new(None)
        };
        Ok(DalContext {
            metrics: Arc::new(Default::default()),
            cache,
        })
    }

    /// Increment read bytes.
    pub fn inc_read_bytes(&self, bytes: usize) {
        let mut metrics = self.metrics.write();
        metrics.read_bytes += bytes;
    }

    /// Increment write bytes.
    pub fn inc_write_bytes(&self, bytes: usize) {
        let mut metrics = self.metrics.write();
        metrics.write_bytes += bytes;
    }

    pub fn get_metrics(&self) -> DalMetrics {
        self.metrics.read().clone()
    }
}
