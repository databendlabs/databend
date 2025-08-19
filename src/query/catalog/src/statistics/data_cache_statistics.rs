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
    pub bytes_from_remote_disk: AtomicUsize,
    bytes_from_local_disk: AtomicUsize,
    bytes_from_memory: AtomicUsize,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct DataCacheMetricValues {
    pub bytes_from_remote_disk: usize,
    pub bytes_from_local_disk: usize,
    pub bytes_from_memory: usize,
}

impl DataCacheMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn as_values(&self) -> DataCacheMetricValues {
        DataCacheMetricValues {
            bytes_from_remote_disk: self
                .bytes_from_remote_disk
                .load(std::sync::atomic::Ordering::Relaxed),
            bytes_from_local_disk: self
                .bytes_from_local_disk
                .load(std::sync::atomic::Ordering::Relaxed),
            bytes_from_memory: self
                .bytes_from_memory
                .load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    pub fn add_cache_metrics(
        &self,
        bytes_from_storage: usize,
        from_disk_cache: usize,
        from_mem_cache: usize,
    ) {
        self.bytes_from_remote_disk
            .fetch_add(bytes_from_storage, std::sync::atomic::Ordering::Relaxed);
        self.bytes_from_local_disk
            .fetch_add(from_disk_cache, std::sync::atomic::Ordering::Relaxed);
        self.bytes_from_memory
            .fetch_add(from_mem_cache, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn merge(&self, other: DataCacheMetricValues) {
        self.bytes_from_remote_disk.fetch_add(
            other.bytes_from_remote_disk,
            std::sync::atomic::Ordering::Relaxed,
        );
        self.bytes_from_local_disk.fetch_add(
            other.bytes_from_local_disk,
            std::sync::atomic::Ordering::Relaxed,
        );
        self.bytes_from_memory.fetch_add(
            other.bytes_from_memory,
            std::sync::atomic::Ordering::Relaxed,
        );
    }
}
