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

use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;

use bytes::Bytes;
use crossbeam_channel::TrySendError;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_config::DiskCacheKeyReloadPolicy;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::cache::*;
use log::error;
use log::info;

use crate::CacheAccessor;
use crate::LruDiskCacheBuilder;
use crate::providers::LruDiskCacheHolder;

struct CacheItem {
    key: String,
    value: Bytes,
}

#[derive(Clone)]
pub struct TableDataCacheKey {
    cache_key: String,
}

impl TableDataCacheKey {
    pub fn new(block_path: &str, column_id: u32, offset: u64, len: u64) -> Self {
        Self {
            cache_key: format!("{block_path}-{column_id}-{offset}-{len}"),
        }
    }
}

impl From<TableDataCacheKey> for String {
    fn from(value: TableDataCacheKey) -> Self {
        value.cache_key
    }
}

impl AsRef<str> for TableDataCacheKey {
    fn as_ref(&self) -> &str {
        &self.cache_key
    }
}

#[derive(Clone)]
pub struct DiskCacheAccessor<T = LruDiskCacheHolder> {
    name: String,
    lru_disk_cache: T,
    population_queue: crossbeam_channel::Sender<CacheItem>,
    _cache_populator: DiskCachePopulator,
}

#[cfg(test)]
impl DiskCacheAccessor {
    fn is_populate_queue_drained(&self) -> bool {
        self._cache_populator.receiver.is_empty()
    }
    pub fn till_no_pending_items_in_queue(&self) {
        while !self.is_populate_queue_drained() {}
    }
}

pub struct DiskCacheBuilder;

impl DiskCacheBuilder {
    pub fn try_build_disk_cache(
        name: String,
        path: &PathBuf,
        population_queue_size: u32,
        disk_cache_bytes_size: usize,
        disk_cache_reload_policy: DiskCacheKeyReloadPolicy,
        sync_data: bool,
    ) -> Result<DiskCacheAccessor<LruDiskCacheHolder>> {
        let disk_cache = LruDiskCacheBuilder::new_disk_cache(
            path,
            disk_cache_bytes_size,
            disk_cache_reload_policy,
            sync_data,
        )?;
        let (tx, rx) = crossbeam_channel::bounded(population_queue_size as usize);
        let num_population_thread = 1;
        Ok(DiskCacheAccessor {
            name,
            lru_disk_cache: disk_cache.clone(),
            population_queue: tx,
            _cache_populator: DiskCachePopulator::new(rx, disk_cache, num_population_thread)?,
        })
    }
}

impl CacheAccessor for DiskCacheAccessor {
    type V = Bytes;

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn get<Q: AsRef<str>>(&self, k: Q) -> Option<Arc<Bytes>> {
        metrics_inc_cache_access_count(1, &self.name);
        let k = k.as_ref();
        if let Some(item) = self.lru_disk_cache.get(k) {
            let size = item.len();
            Profile::record_usize_profile(ProfileStatisticsName::ScanBytesFromLocalDisk, size);
            metrics_inc_cache_hit_count(1, &self.name);
            Some(item)
        } else {
            metrics_inc_cache_miss_count(1, &self.name);
            None
        }
    }

    fn get_sized<Q: AsRef<str>>(&self, k: Q, len: u64) -> Option<Arc<Self::V>> {
        let Some(cached_value) = self.get(k) else {
            metrics_inc_cache_miss_bytes(len, &self.name);
            return None;
        };

        Some(cached_value)
    }

    fn insert(&self, k: String, v: Bytes) -> Arc<Bytes> {
        // check if already cached
        if !self.lru_disk_cache.contains_key(&k) {
            // populate the cache is necessary
            let msg = CacheItem {
                key: k,
                value: v.clone(),
            };
            match self.population_queue.try_send(msg) {
                Ok(_) => {
                    metrics_inc_cache_population_pending_count(1, &self.name);
                }
                Err(TrySendError::Full(_)) => {
                    metrics_inc_cache_population_pending_count(-1, &self.name);
                    metrics_inc_cache_population_overflow_count(1, &self.name);
                }
                Err(TrySendError::Disconnected(_)) => {
                    error!("table data cache population thread is down");
                }
            }
        }
        Arc::new(v)
    }

    fn evict(&self, k: &str) -> bool {
        self.lru_disk_cache.evict(k)
    }

    fn contains_key(&self, k: &str) -> bool {
        self.lru_disk_cache.contains_key(k)
    }

    fn bytes_size(&self) -> u64 {
        self.lru_disk_cache.bytes_size()
    }

    fn items_capacity(&self) -> u64 {
        self.lru_disk_cache.items_capacity()
    }

    fn bytes_capacity(&self) -> u64 {
        self.lru_disk_cache.bytes_capacity()
    }

    fn len(&self) -> usize {
        self.lru_disk_cache.len()
    }

    fn clear(&self) {
        // For disk cache, clear nothing
    }
}

struct CachePopulationWorker<T> {
    cache: T,
    population_queue: crossbeam_channel::Receiver<CacheItem>,
}

impl<T: CacheAccessor<V = Bytes> + Send + Sync + 'static> CachePopulationWorker<T> {
    fn populate(&self) {
        loop {
            match self.population_queue.recv() {
                Ok(CacheItem { key, value }) => {
                    {
                        if self.cache.contains_key(&key) {
                            continue;
                        }
                    }
                    self.cache.insert(key, value);
                    metrics_inc_cache_population_pending_count(-1, self.cache.name());
                }
                Err(e) => {
                    info!("table data cache worker shutdown, due to error: {:?}", e);
                    break;
                }
            }
        }
    }

    fn start(self: Arc<Self>) -> Result<JoinHandle<()>> {
        let thread_builder = std::thread::Builder::new().name(self.cache.name().to_owned());
        thread_builder.spawn(move || self.populate()).map_err(|e| {
            ErrorCode::StorageOther(format!("spawn cache population worker thread failed, {e}"))
        })
    }
}

#[derive(Clone)]
struct DiskCachePopulator {
    #[cfg(test)]
    receiver: crossbeam_channel::Receiver<CacheItem>,
}

impl DiskCachePopulator {
    #[cfg(test)]
    fn new<T>(
        incoming: crossbeam_channel::Receiver<CacheItem>,
        cache: T,
        _num_worker_thread: usize,
    ) -> Result<Self>
    where
        T: CacheAccessor<V = Bytes> + Send + Sync + 'static,
    {
        let receiver = incoming.clone();
        Self::kick_off(incoming, cache, _num_worker_thread)?;
        Ok(Self { receiver })
    }

    #[cfg(not(test))]
    fn new<T>(
        incoming: crossbeam_channel::Receiver<CacheItem>,
        cache: T,
        _num_worker_thread: usize,
    ) -> Result<Self>
    where
        T: CacheAccessor<V = Bytes> + Send + Sync + 'static,
    {
        Self::kick_off(incoming, cache, _num_worker_thread)?;
        Ok(Self {})
    }

    fn kick_off<T>(
        incoming: crossbeam_channel::Receiver<CacheItem>,
        cache: T,
        _num_worker_thread: usize,
    ) -> Result<()>
    where
        T: CacheAccessor<V = Bytes> + Send + Sync + 'static,
    {
        let worker = Arc::new(CachePopulationWorker {
            cache,
            population_queue: incoming,
        });
        let _join_handler = worker.start();
        Ok(())
    }
}
