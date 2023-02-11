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

use common_cache::Count;
pub use common_cache::LruDiskCache as DiskCache;
use common_exception::ErrorCode;
use common_exception::Result;
use parking_lot::RwLock;
use tracing::error;

use crate::CacheAccessor;

/// Tiered cache which consist of
/// A in-memory cache
/// A ring that keep the reference of bytes
/// A slow disk or redis based persistent cache
#[derive(Clone)]
pub struct DiskBytesCache {
    inner: Arc<RwLock<DiskCache>>,
}

pub struct DiskCacheBuilder;
impl DiskCacheBuilder {
    pub fn new_disk_cache(path: &str, disk_cache_bytes_size: u64) -> Result<DiskBytesCache> {
        let external_cache = DiskCache::new(path, disk_cache_bytes_size)
            .map_err(|e| ErrorCode::StorageOther(format!("create disk cache failed, {e}")))?;
        let inner = Arc::new(RwLock::new(external_cache));
        Ok(DiskBytesCache { inner })
    }
}

impl CacheAccessor<String, Vec<u8>, common_cache::DefaultHashBuilder, Count> for DiskBytesCache {
    fn get<Q: AsRef<str>>(&self, k: Q) -> Option<Arc<Vec<u8>>> {
        let k = k.as_ref();
        // check disk cache
        let read_file = || {
            let mut file = {
                let mut inner = self.inner.write();
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
                        let mut inner = self.inner.write();
                        let _ = inner.remove(k);
                    }
                    None
                } else {
                    // trim the checksum bytes and return
                    let total_len = bytes.len();
                    let body_len = total_len - 4;
                    bytes.truncate(body_len);
                    let item = Arc::new(bytes);
                    Some(item)
                }
            }
            Err(e) => {
                error!("get disk cache item failed, {}", e);
                None
            }
        }
    }

    fn put(&self, key: String, value: Arc<Vec<u8>>) {
        let crc = crc32fast::hash(value.as_slice());
        let crc_bytes = crc.to_le_bytes();
        let mut inner = self.inner.write();
        if let Err(e) = inner.insert_bytes(&key, &[value.as_slice(), &crc_bytes]) {
            error!("put disk cache item failed {}", e);
        }
    }

    fn evict(&self, k: &str) -> bool {
        if let Err(e) = {
            let mut inner = self.inner.write();
            inner.remove(k)
        } {
            error!("evict disk cache item failed {}", e);
            false
        } else {
            true
        }
    }

    fn contains_key(&self, k: &str) -> bool {
        let inner = self.inner.read();
        inner.contains_key(k)
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
