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
use databend_common_cache::Count;
use databend_common_cache::DefaultHashBuilder;
use databend_common_config::DiskCacheKeyReloadPolicy;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::cache::*;
use log::error;
use log::info;

use crate::providers::LruDiskCacheHolder;
use crate::CacheAccessor;
use crate::CacheAccessorExt;
use crate::LruDiskCacheBuilder;

struct CacheItem {
    key: String,
    value: Arc<Bytes>,
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
pub struct TableDataCache<T = LruDiskCacheHolder> {
    external_cache: T,
    population_queue: crossbeam_channel::Sender<CacheItem>,
    _cache_populator: DiskCachePopulator,
}

pub const DISK_TABLE_DATA_CACHE_NAME: &str = "disk_cache_table_data";

pub struct TableDataCacheBuilder;

impl TableDataCacheBuilder {
    pub fn new_table_data_disk_cache(
        path: &PathBuf,
        population_queue_size: u32,
        disk_cache_bytes_size: u64,
        disk_cache_reload_policy: DiskCacheKeyReloadPolicy,
        sync_data: bool,
    ) -> Result<TableDataCache<LruDiskCacheHolder>> {
        let disk_cache = LruDiskCacheBuilder::new_disk_cache(
            path,
            disk_cache_bytes_size,
            disk_cache_reload_policy,
            sync_data,
        )?;
        let (tx, rx) = crossbeam_channel::bounded(population_queue_size as usize);
        let num_population_thread = 1;
        Ok(TableDataCache {
            external_cache: disk_cache.clone(),
            population_queue: tx,
            _cache_populator: DiskCachePopulator::new(rx, disk_cache, num_population_thread)?,
        })
    }
}

impl CacheAccessorExt<String, Bytes, DefaultHashBuilder, Count> for TableDataCache {
    fn get_with_len<Q: AsRef<str>>(&self, k: Q, len: u64) -> Option<Arc<Bytes>> {
        let r = self.get(k);
        if r.is_none() {
            metrics_inc_cache_miss_count(len, DISK_TABLE_DATA_CACHE_NAME);
        }
        r
    }
}

impl CacheAccessorExt<String, Bytes, DefaultHashBuilder, Count> for Option<TableDataCache> {
    fn get_with_len<Q: AsRef<str>>(&self, k: Q, len: u64) -> Option<Arc<Bytes>> {
        self.as_ref().and_then(|cache| cache.get_with_len(k, len))
    }
}

impl CacheAccessor<String, Bytes, DefaultHashBuilder, Count> for TableDataCache {
    fn get<Q: AsRef<str>>(&self, k: Q) -> Option<Arc<Bytes>> {
        metrics_inc_cache_access_count(1, DISK_TABLE_DATA_CACHE_NAME);
        let k = k.as_ref();
        if let Some(item) = self.external_cache.get(k) {
            Profile::record_usize_profile(ProfileStatisticsName::ScanCacheBytes, item.len());
            metrics_inc_cache_hit_count(1, DISK_TABLE_DATA_CACHE_NAME);
            Some(item)
        } else {
            metrics_inc_cache_miss_count(1, DISK_TABLE_DATA_CACHE_NAME);
            None
        }
    }

    fn put(&self, k: String, v: Arc<Bytes>) {
        // check if already cached
        if !self.external_cache.contains_key(&k) {
            // populate the cache is necessary
            let msg = CacheItem { key: k, value: v };
            match self.population_queue.try_send(msg) {
                Ok(_) => {
                    metrics_inc_cache_population_pending_count(1, DISK_TABLE_DATA_CACHE_NAME);
                }
                Err(TrySendError::Full(_)) => {
                    metrics_inc_cache_population_pending_count(-1, DISK_TABLE_DATA_CACHE_NAME);
                    metrics_inc_cache_population_overflow_count(1, DISK_TABLE_DATA_CACHE_NAME);
                }
                Err(TrySendError::Disconnected(_)) => {
                    error!("table data cache population thread is down");
                }
            }
        }
    }

    fn evict(&self, k: &str) -> bool {
        self.external_cache.evict(k)
    }

    fn contains_key(&self, k: &str) -> bool {
        self.external_cache.contains_key(k)
    }

    fn size(&self) -> u64 {
        self.external_cache.size()
    }

    fn len(&self) -> usize {
        self.external_cache.len()
    }
}

struct CachePopulationWorker<T> {
    cache: T,
    population_queue: crossbeam_channel::Receiver<CacheItem>,
}

impl<T> CachePopulationWorker<T>
where T: CacheAccessor<String, Bytes, DefaultHashBuilder, Count> + Send + Sync + 'static
{
    fn populate(&self) {
        loop {
            match self.population_queue.recv() {
                Ok(CacheItem { key, value }) => {
                    {
                        if self.cache.contains_key(&key) {
                            continue;
                        }
                    }
                    self.cache.put(key, value);
                    metrics_inc_cache_population_pending_count(-1, DISK_TABLE_DATA_CACHE_NAME);
                }
                Err(_) => {
                    info!("table data cache worker shutdown");
                    break;
                }
            }
        }
    }

    fn start(self: Arc<Self>) -> Result<JoinHandle<()>> {
        let thread_builder =
            std::thread::Builder::new().name("table-data-cache-population".to_owned());
        thread_builder.spawn(move || self.populate()).map_err(|e| {
            ErrorCode::StorageOther(format!("spawn cache population worker thread failed, {e}"))
        })
    }
}

#[derive(Clone)]
struct DiskCachePopulator;

impl DiskCachePopulator {
    fn new<T>(
        incoming: crossbeam_channel::Receiver<CacheItem>,
        cache: T,
        _num_worker_thread: usize,
    ) -> Result<Self>
    where
        T: CacheAccessor<String, Bytes, DefaultHashBuilder, Count> + Send + Sync + 'static,
    {
        let worker = Arc::new(CachePopulationWorker {
            cache,
            population_queue: incoming,
        });
        let _join_handler = worker.start()?;
        Ok(Self)
    }
}
