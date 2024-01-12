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

use std::sync::atomic::AtomicUsize;

use serde::Deserialize;
use serde::Serialize;

#[derive(Default)]
pub struct DataCacheMetrics {
    bytes_from_storage: AtomicUsize,
    bytes_from_disk_cache: AtomicUsize,
    bytes_from_mem_cache: AtomicUsize,
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct DataCacheMetricValues {
    pub bytes_from_storage: usize,
    pub bytes_from_disk_cache: usize,
    pub bytes_from_mem_cache: usize,
}

impl DataCacheMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn as_values(&self) -> DataCacheMetricValues {
        DataCacheMetricValues {
            bytes_from_storage: self
                .bytes_from_storage
                .load(std::sync::atomic::Ordering::Relaxed),
            bytes_from_disk_cache: self
                .bytes_from_disk_cache
                .load(std::sync::atomic::Ordering::Relaxed),
            bytes_from_mem_cache: self
                .bytes_from_mem_cache
                .load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    pub fn add_cache_metrics(
        &self,
        bytes_from_storage: usize,
        from_disk_cache: usize,
        from_mem_cache: usize,
    ) {
        self.bytes_from_storage
            .fetch_add(bytes_from_storage, std::sync::atomic::Ordering::Relaxed);
        self.bytes_from_disk_cache
            .fetch_add(from_disk_cache, std::sync::atomic::Ordering::Relaxed);
        self.bytes_from_mem_cache
            .fetch_add(from_mem_cache, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn merge(&self, other: DataCacheMetricValues) {
        self.bytes_from_storage.fetch_add(
            other.bytes_from_storage,
            std::sync::atomic::Ordering::Relaxed,
        );
        self.bytes_from_disk_cache.fetch_add(
            other.bytes_from_disk_cache,
            std::sync::atomic::Ordering::Relaxed,
        );
        self.bytes_from_mem_cache.fetch_add(
            other.bytes_from_mem_cache,
            std::sync::atomic::Ordering::Relaxed,
        );
    }
}
