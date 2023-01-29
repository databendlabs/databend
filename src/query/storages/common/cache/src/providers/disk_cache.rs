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

use std::borrow::Borrow;
use std::hash::Hash;
use std::io::Read;
use std::sync::Arc;

pub use common_cache::LruDiskCache as DiskCache;
use common_exception::ErrorCode;
use common_exception::Result;
use parking_lot::RwLock;
use tracing::error;

use crate::cache::StorageCache;

pub type DiskBytesCache = Arc<RwLock<DiskCache>>;

pub struct DiskCacheBuilder;
impl DiskCacheBuilder {
    pub fn new_disk_cache(path: &str, capacity: u64) -> Result<DiskBytesCache> {
        let cache = DiskCache::new(path, capacity)
            .map_err(|e| ErrorCode::StorageOther(format!("create disk cache failed, {}", e)))?;
        Ok(Arc::new(RwLock::new(cache)))
    }
}

impl StorageCache<String, Vec<u8>> for DiskCache {
    type Meter = ();

    fn put(&mut self, key: String, value: Arc<Vec<u8>>) {
        if let Err(e) = self.insert_bytes(key, &value) {
            error!("populate disk cache failed {}", e);
        }
    }

    fn get<Q>(&mut self, k: &Q) -> Option<Arc<Vec<u8>>>
    where
        String: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut read_file = || {
            let mut file = self.get_file(k)?;
            let mut v = vec![];
            file.read_to_end(&mut v)?;
            Ok::<_, Box<dyn std::error::Error>>(v)
        };

        match read_file() {
            Ok(bytes) => Some(Arc::new(bytes)),
            Err(e) => {
                error!("get disk cache item failed {}", e);
                None
            }
        }
    }

    fn evict<Q>(&mut self, k: &Q) -> bool
    where
        String: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if let Err(e) = self.remove(k) {
            error!("evict disk cache item failed {}", e);
            false
        } else {
            true
        }
    }
}
