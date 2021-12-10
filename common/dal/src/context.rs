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

#[derive(Clone, Debug, Default)]
pub struct DalMetrics {
    pub read_bytes: usize,
    pub write_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct DalContext {
    metrics: Arc<RwLock<DalMetrics>>,
    table_cache_enabled: bool,
    snapshots_cache: Option<Arc<RwLock<LruDiskCache>>>,
    segments_cache: Option<Arc<RwLock<LruDiskCache>>>,
}

impl DalContext {
    pub fn create(table_cache_enabled: bool, path: String, size_mb: u64) -> Result<Self> {
        let (snapshots_cache, segments_cache) = if table_cache_enabled {
            let bytes_size = size_mb * 1024 * 1024;
            let snapshots_cache = LruDiskCache::new(path.clone(), bytes_size)?;
            let segments_cache = LruDiskCache::new(path.clone(), bytes_size)?;
            (
                Some(Arc::new(RwLock::new(snapshots_cache))),
                Some(Arc::new(RwLock::new(segments_cache))),
            )
        } else {
            (None, None)
        };
        Ok(DalContext {
            metrics: Arc::new(Default::default()),
            table_cache_enabled,
            snapshots_cache,
            segments_cache,
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
