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

use std::io::Read;
use std::sync::Arc;
use std::thread::JoinHandle;

use common_cache::Cache;
pub use common_cache::LruDiskCache as DiskCache;
use common_exception::ErrorCode;
use common_exception::Result;
use crossbeam_channel::TrySendError;
use parking_lot::RwLock;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::CacheAccessor;
use crate::InMemoryBytesCacheHolder;
use crate::InMemoryCacheBuilder;

struct CacheItem {
    key: String,
    value: Arc<Vec<u8>>,
}

/// Tiered cache which consist of
/// A in-memory cache
/// A ring that keep the reference of bytes
/// A slow disk or redis based persistent cache
#[derive(Clone)]
pub struct DiskBytesCache {
    inner_memory_cache: InMemoryBytesCacheHolder,
    inner_external_cache: Arc<RwLock<DiskCache>>,
    population_queue: crossbeam_channel::Sender<CacheItem>,
    _cache_populator: DiskCachePopulator,
}

pub struct DiskCacheBuilder;
impl DiskCacheBuilder {
    pub fn new_disk_cache(
        path: &str,
        in_memory_cache_mb_size: u64,
        population_queue_size: u32,
        disk_cache_size: u64,
    ) -> Result<DiskBytesCache> {
        let external_cache = DiskCache::new(path, disk_cache_size * 1024 * 1024)
            .map_err(|e| ErrorCode::StorageOther(format!("create disk cache failed, {e}")))?;
        let inner = Arc::new(RwLock::new(external_cache));
        let (rx, tx) = crossbeam_channel::bounded(population_queue_size as usize);
        Ok(DiskBytesCache {
            inner_memory_cache: InMemoryCacheBuilder::new_bytes_cache(
                in_memory_cache_mb_size * 1024 * 1024,
            ),
            inner_external_cache: inner.clone(),
            population_queue: rx,
            _cache_populator: DiskCachePopulator::new(tx, inner, 1)?,
        })
    }
}

impl CacheAccessor<String, Vec<u8>> for DiskBytesCache {
    fn get(&self, k: &str) -> Option<Arc<Vec<u8>>> {
        // check in memory cache first
        {
            if let Some(item) = self.inner_memory_cache.get(k) {
                return Some(item);
            }
        }

        // check disk cache
        let read_file = || {
            let mut file = {
                let mut inner = self.inner_external_cache.write();
                inner.get_file(k)?
            };
            let mut v = vec![];
            file.read_to_end(&mut v)?;
            Ok::<_, Box<dyn std::error::Error>>(v)
        };

        match read_file() {
            Ok(mut bytes) => {
                if let Err(e) = validate_checksum(bytes.as_slice()) {
                    error!("data cache, of key {k},  crc validation failure: {e}");
                    {
                        // remove the invalid cache, error of removal ignored
                        let mut inner = self.inner_external_cache.write();
                        let _ = inner.remove(k);
                    }
                    None
                } else {
                    // trim the checksum bytes and return
                    let total_len = bytes.len();
                    let body_len = total_len - 4;
                    bytes.truncate(body_len);
                    let item = Arc::new(bytes);
                    // also put the cached item into in-memory cache
                    self.inner_memory_cache.put(k.to_owned(), item.clone());
                    Some(item)
                }
            }
            Err(e) => {
                error!("get disk cache item failed, {}", e);
                None
            }
        }
    }

    fn put(&self, k: String, v: Arc<Vec<u8>>) {
        // put it into the in-memory cache first
        {
            let mut in_memory_cache = self.inner_memory_cache.write();
            in_memory_cache.put(k.clone(), v.clone());
        }

        // check if external(disk/redis) already have it.
        let contains = {
            let external_cache = self.inner_external_cache.read();
            external_cache.contains_key(&k)
        };

        if !contains {
            // populate the cache to external cache(disk/redis) asyncly
            let msg = CacheItem { key: k, value: v };
            match self.population_queue.try_send(msg) {
                Ok(_) => {}
                Err(TrySendError::Full(_)) => {
                    self::metrics::metrics_inc_population_overflow_count(1);
                    warn!("disk cache population queue is full");
                }
                Err(TrySendError::Disconnected(_)) => {
                    error!("disk cache population thread is down");
                }
            }
        }
    }

    fn evict(&self, k: &str) -> bool {
        if let Err(e) = {
            self.inner_memory_cache.evict(k);
            let mut inner = self.inner_external_cache.write();
            inner.remove(k)
        } {
            error!("evict disk cache item failed {}", e);
            false
        } else {
            true
        }
    }
}

#[derive(Clone)]
struct CachePopulationWorker {
    cache: Arc<RwLock<DiskCache>>,
    population_queue: crossbeam_channel::Receiver<CacheItem>,
}

impl CachePopulationWorker {
    fn populate(&self) {
        loop {
            match self.population_queue.recv() {
                Ok(CacheItem { key, value }) => {
                    {
                        let inner = self.cache.read();
                        if inner.contains_key(&key) {
                            continue;
                        }
                    }
                    if let Err(e) = {
                        let crc = crc32fast::hash(value.as_slice());
                        let crc_bytes = crc.to_le_bytes();
                        let mut inner = self.cache.write();
                        inner.insert_bytes(&key, &[value.as_slice(), &crc_bytes])
                    } {
                        error!("populate disk cache failed {}", e);
                    } else {
                        self::metrics::metrics_inc_disk_cache_population_count(1);
                    }
                }
                Err(_) => {
                    info!("cache work shutdown");
                    break;
                }
            }
        }
    }

    fn start(self: Arc<Self>) -> Result<JoinHandle<()>> {
        let thread_builder = std::thread::Builder::new().name("cache-population".to_owned());
        thread_builder.spawn(move || self.populate()).map_err(|e| {
            ErrorCode::StorageOther(format!("spawn cache population worker thread failed, {e}"))
        })
    }
}

#[derive(Clone)]
struct DiskCachePopulator {
    _workers: Vec<Arc<CachePopulationWorker>>,
}

impl DiskCachePopulator {
    fn new(
        incoming: crossbeam_channel::Receiver<CacheItem>,
        cache: Arc<RwLock<DiskCache>>,
        _num_worker_thread: usize,
    ) -> Result<Self> {
        let worker = Arc::new(CachePopulationWorker {
            cache,
            population_queue: incoming,
        });
        let _join_handler = worker.clone().start()?;
        Ok(Self {
            _workers: vec![worker],
        })
    }

    #[allow(dead_code)]
    pub fn shutdown(&self) {
        // by drop the sender
        // and timed join the join_handlers
    }
}

/// Assuming that the crc32 is at the end of `bytes` and encoded as le u32.
// Although parquet page has built-in crc, but it is optional (and not generated in parquet2)
// Later, if cache data is put into redis, we can reuse the checksum logic
fn validate_checksum(bytes: &[u8]) -> Result<()> {
    let total_len = bytes.len();
    if total_len <= 4 {
        Err(ErrorCode::StorageOther(format!(
            "crc checksum validation failure: invalid file length {total_len}"
        )))
    } else {
        // checksum validation
        let crc_bytes: [u8; 4] = bytes[total_len - 4..].try_into().unwrap();
        let crc = u32::from_le_bytes(crc_bytes);
        let crc_calculated = crc32fast::hash(&bytes[0..total_len - 4]);
        if crc == crc_calculated {
            Ok(())
        } else {
            Err(ErrorCode::StorageOther(format!(
                "crc checksum validation failure, key : crc checksum not match, crc kept in file {crc}, crc calculated {crc_calculated}"
            )))
        }
    }
}

mod metrics {
    use metrics::increment_gauge;

    #[inline]
    pub fn metrics_inc_population_overflow_count(c: u64) {
        increment_gauge!("data_block_cache_population_overflow", c as f64);
    }

    #[inline]
    pub fn metrics_inc_disk_cache_population_count(c: u64) {
        increment_gauge!("data_block_cache_population_overflow", c as f64);
    }
}
