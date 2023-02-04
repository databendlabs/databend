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

pub use common_cache::LruDiskCache as DiskCache;
use common_exception::ErrorCode;
use common_exception::Result;
use parking_lot::RwLock;
use tracing::error;

use crate::CacheAccessor;

#[derive(Clone)]
pub struct DiskBytesCache {
    inner: Arc<RwLock<DiskCache>>,
}

pub struct DiskCacheBuilder;
impl DiskCacheBuilder {
    pub fn new_disk_cache(path: &str, capacity: u64) -> Result<DiskBytesCache> {
        let cache = DiskCache::new(path, capacity)
            .map_err(|e| ErrorCode::StorageOther(format!("create disk cache failed, {e}")))?;
        let inner = Arc::new(RwLock::new(cache));
        Ok(DiskBytesCache { inner })
    }
}

impl CacheAccessor<String, Vec<u8>> for DiskBytesCache {
    fn get(&self, k: &str) -> Option<Arc<Vec<u8>>> {
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
            Ok(bytes) => Some(Arc::new(bytes)),
            Err(e) => {
                error!("get disk cache item failed, {}", e);
                None
            }
        }
    }

    fn put(&self, k: String, v: Arc<Vec<u8>>) {
        if let Err(e) = {
            let mut inner = self.inner.write();
            inner.insert_bytes(&k, &v)
        } {
            error!("populate disk cache failed {}", e);
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
}
