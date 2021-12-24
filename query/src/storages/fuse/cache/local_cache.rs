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

use std::ffi::OsString;
use std::io::Read;
use std::sync::Arc;

use async_trait::async_trait;
use common_base::tokio::sync::RwLock;
use common_cache::basic::BytesMeter;
use common_cache::basic::Cache;
use common_cache::basic::DefaultHashBuilder;
use common_cache::basic::LruCache;
use common_cache::basic::LruDiskCache;
use common_cache::storage::StorageCache;
use common_dal::DataAccessor;
use common_exception::ErrorCode;
use common_exception::Result;
use common_metrics::label_counter;
use common_metrics::label_counter_with_val;

use crate::storages::fuse::FUSE_TBL_BLOCK_PREFIX;
use crate::storages::fuse::FUSE_TBL_SEGMENT_PREFIX;
use crate::storages::fuse::FUSE_TBL_SNAPSHOT_PREFIX;

enum ObjectType {
    TableSnapshot,
    TableSegment,
    TableBlock,
    Unknown,
}

fn get_object_type(location: &str) -> ObjectType {
    if location.starts_with(FUSE_TBL_SNAPSHOT_PREFIX) {
        return ObjectType::TableSnapshot;
    }
    if location.starts_with(FUSE_TBL_SEGMENT_PREFIX) {
        return ObjectType::TableSegment;
    }
    if location.starts_with(FUSE_TBL_BLOCK_PREFIX) {
        return ObjectType::TableBlock;
    }
    ObjectType::Unknown
}

const CACHE_READ_BYTES_FROM_REMOTE: &str = "cache_read_bytes_from_remote";
const CACHE_READ_BYTES_FROM_LOCAL: &str = "cache_read_bytes_from_local";
const CACHE_ACCESS_COUNT: &str = "cache_access_count";
const CACHE_ACCESS_HIT_COUNT: &str = "cache_access_hit_count";

struct CacheDeferMetrics<'a> {
    tenant_id: &'a str,
    cluster_id: &'a str,
    cache_hit: bool,
    read_bytes: u64,
}

impl Drop for CacheDeferMetrics<'_> {
    fn drop(&mut self) {
        label_counter(CACHE_ACCESS_COUNT, self.tenant_id, self.cluster_id);
        if self.cache_hit {
            label_counter(CACHE_ACCESS_HIT_COUNT, self.tenant_id, self.cluster_id);
            label_counter_with_val(
                CACHE_READ_BYTES_FROM_LOCAL,
                self.read_bytes,
                self.tenant_id,
                self.cluster_id,
            );
        } else {
            label_counter_with_val(
                CACHE_READ_BYTES_FROM_REMOTE,
                self.read_bytes,
                self.tenant_id,
                self.cluster_id,
            );
        }
    }
}

pub struct LocalCacheConfig {
    pub memory_cache_size_mb: u64,
    pub disk_cache_size_mb: u64,
    pub disk_cache_root: String,
    pub tenant_id: String,
    pub cluster_id: String,
}

type MemCache = Arc<RwLock<LruCache<OsString, Vec<u8>, DefaultHashBuilder, BytesMeter>>>;

// TODO maybe distinct segments cache and snapshots cache
#[derive(Clone, Debug)]
pub struct LocalCache {
    pub disk_cache: Arc<RwLock<LruDiskCache>>,
    pub mem_cache: MemCache,
    tenant_id: String,
    cluster_id: String,
}

impl LocalCache {
    pub fn create(conf: LocalCacheConfig) -> Result<Box<dyn StorageCache>> {
        let disk_cache = Arc::new(RwLock::new(LruDiskCache::new(
            conf.disk_cache_root,
            conf.disk_cache_size_mb * 1024 * 1024,
        )?));
        Ok(Box::new(LocalCache {
            mem_cache: Arc::new(RwLock::new(LruCache::with_meter(
                conf.memory_cache_size_mb * 1024 * 1024,
                BytesMeter,
            ))),
            disk_cache,
            tenant_id: conf.tenant_id,
            cluster_id: conf.cluster_id,
        }))
    }
}

#[async_trait]
impl StorageCache for LocalCache {
    async fn get(&self, location: &str, da: &dyn DataAccessor) -> Result<Vec<u8>> {
        let loc: OsString = location.to_owned().into();
        let mut metrics = CacheDeferMetrics {
            tenant_id: self.tenant_id.as_str(),
            cluster_id: self.cluster_id.as_str(),
            cache_hit: false,
            read_bytes: 0,
        };

        match get_object_type(location) {
            ObjectType::TableSnapshot | ObjectType::TableSegment => {
                // get data from memory cache
                let mut mem_cache = self.mem_cache.write().await;
                if let Some(data) = mem_cache.get(&loc) {
                    metrics.cache_hit = true;
                    metrics.read_bytes = data.len() as u64;

                    return Ok(data.clone());
                }
                let data = da.read(location).await?;
                mem_cache.put(loc, data.clone());
                metrics.read_bytes = data.len() as u64;
                return Ok(data);
            }
            ObjectType::TableBlock => {
                // get data from disk cache
                let mut disk_cache = self.disk_cache.write().await;
                if disk_cache.contains_key(loc.clone()) {
                    let mut path = disk_cache.get(loc)?;
                    let data = read_all(&mut path)?;

                    metrics.cache_hit = true;
                    metrics.read_bytes = data.len() as u64;
                    return Ok(data);
                }
                let data = da.read(location).await?;
                disk_cache.insert_bytes(loc, data.as_slice())?;
                metrics.read_bytes = data.len() as u64;
                return Ok(data);
            }
            ObjectType::Unknown => Err(ErrorCode::UnexpectedError(format!(
                "'{}' is not supported reading from fuse local cache",
                location
            ))),
        }
    }
}

fn read_all<R: Read>(r: &mut R) -> Result<Vec<u8>> {
    let mut v = vec![];
    r.read_to_end(&mut v)?;
    Ok(v)
}
