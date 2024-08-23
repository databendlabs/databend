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

use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use databend_common_config::DiskCacheKeyReloadPolicy;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::cache::metrics_inc_cache_miss_bytes;
use log::error;
use log::warn;
use parking_lot::RwLock;

use crate::providers::disk_cache::DiskCache;
use crate::CacheAccessor;

impl CacheAccessor for LruDiskCacheHolder {
    type V = Bytes;
    fn name(&self) -> &str {
        "LruDiskCacheHolder"
    }

    fn get(&self, k: &String) -> Option<Arc<Bytes>> {
        {
            let mut cache = self.write();
            cache.get_cache_path(k)
        }
        .and_then(|cache_file_path| {
            // check disk cache
            let get_cache_content = || {
                let mut v = vec![];
                let mut file = File::open(cache_file_path)?;
                file.read_to_end(&mut v)?;
                Ok::<_, Box<dyn std::error::Error>>(v)
            };

            match get_cache_content() {
                Ok(mut bytes) => {
                    if let Err(e) = validate_checksum(bytes.as_slice()) {
                        error!("disk cache, of key {k},  crc validation failure: {e}");
                        {
                            // remove the invalid cache, error of removal ignored
                            let r = {
                                let mut cache = self.write();
                                cache.remove(k)
                            };
                            if let Err(e) = r {
                                warn!("failed to remove invalid cache item, key {k}. {e}");
                            }
                        }
                        None
                    } else {
                        // trim the checksum bytes and return
                        let total_len = bytes.len();
                        let body_len = total_len - 4;
                        bytes.truncate(body_len);
                        let item = Arc::new(bytes.into());
                        Some(item)
                    }
                }
                Err(e) => {
                    error!("get disk cache item failed, cache_key {k}. {e}");
                    None
                }
            }
        })
    }

    fn get_sized(&self, k: &String, len: u64) -> Option<Arc<Self::V>> {
        let Some(cached_value) = self.get(k) else {
            metrics_inc_cache_miss_bytes(len, self.name());
            return None;
        };

        Some(cached_value)
    }

    fn insert(&self, key: String, value: Bytes) -> Arc<Bytes> {
        let crc = crc32fast::hash(value.as_ref());
        let crc_bytes = crc.to_le_bytes();
        let mut cache = self.write();
        if let Err(e) = cache.insert_bytes(&key, &[value.as_ref(), &crc_bytes]) {
            error!("put disk cache item failed {}", e);
        }
        Arc::new(value)
    }

    fn evict(&self, k: &String) -> bool {
        if let Err(e) = {
            let mut cache = self.write();
            cache.remove(k)
        } {
            error!("evict disk cache item failed {}", e);
            false
        } else {
            true
        }
    }

    fn contains_key(&self, k: &String) -> bool {
        let cache = self.read();
        cache.contains_key(k)
    }

    fn bytes_size(&self) -> u64 {
        let cache = self.read();
        cache.size()
    }

    fn items_capacity(&self) -> u64 {
        let cache = self.read();
        cache.items_capacity()
    }

    fn bytes_capacity(&self) -> u64 {
        let cache = self.read();
        cache.bytes_capacity()
    }

    fn len(&self) -> usize {
        let cache = self.read();
        cache.len()
    }
}

/// The crc32 checksum is stored at the end of `bytes` and encoded as le u32.
// Although parquet page has built-in crc, but it is optional (and not generated in parquet2)
fn validate_checksum(bytes: &[u8]) -> Result<()> {
    let total_len = bytes.len();
    if total_len <= 4 {
        Err(ErrorCode::StorageOther(format!(
            "crc checksum validation failure: invalid file length {total_len}"
        )))
    } else {
        // total_len > 4 is ensured
        let crc_bytes: [u8; 4] = bytes[total_len - 4..].try_into().unwrap();
        let crc_provided = u32::from_le_bytes(crc_bytes);
        let crc_calculated = crc32fast::hash(&bytes[0..total_len - 4]);
        if crc_provided == crc_calculated {
            Ok(())
        } else {
            Err(ErrorCode::StorageOther(format!(
                "crc checksum validation failure, key : crc checksum not match, crc provided {crc_provided}, crc calculated {crc_calculated}"
            )))
        }
    }
}

pub type LruDiskCache = DiskCache;
pub type LruDiskCacheHolder = Arc<RwLock<LruDiskCache>>;

pub struct LruDiskCacheBuilder;

impl LruDiskCacheBuilder {
    pub fn new_disk_cache(
        path: &PathBuf,
        disk_cache_bytes_size: usize,
        disk_cache_reload_policy: DiskCacheKeyReloadPolicy,
        sync_data: bool,
    ) -> Result<LruDiskCacheHolder> {
        let external_cache = DiskCache::new(
            path,
            disk_cache_bytes_size,
            disk_cache_reload_policy,
            sync_data,
        )
        .map_err(|e| ErrorCode::StorageOther(format!("create disk cache failed, {e}")))?;
        Ok(Arc::new(RwLock::new(external_cache)))
    }
}
