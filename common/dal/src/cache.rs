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

use std::collections::VecDeque;
use std::ffi::OsStr;
use std::io::Read;
use std::sync::Arc;

use common_base::tokio::sync::RwLock;
use common_cache::LruDiskCache;
use common_exception::Result;

use crate::DataAccessor;

// TODO maybe distinct segments cache and snapshots cache
#[derive(Clone, Debug)]
pub struct DalCache {
    pub cache: Arc<RwLock<LruDiskCache>>,
    input_tasks: Arc<RwLock<VecDeque<CacheTask>>>,
    buffer_bytes_capacity: u64,
}

#[derive(Clone, Debug)]
struct CacheTask {}

impl DalCache {
    pub fn create(cache: Arc<RwLock<LruDiskCache>>, cache_buffer_mb_size: u64) -> Self {
        Self {
            cache,
            input_tasks: Arc::new(RwLock::new(VecDeque::new())),
            buffer_bytes_capacity: cache_buffer_mb_size * 1024 * 1024,
        }
    }

    pub async fn read<K: AsRef<OsStr>>(&self, loc: K, da: &dyn DataAccessor) -> Result<Vec<u8>> {
        let loc = loc.as_ref().to_owned();
        let mut cache = self.cache.write().await;
        if cache.contains_key(loc.clone()) {
            let mut path = cache.get(loc)?;
            let data = read_all(&mut path)?;
            Ok(data)
        } else {
            let bytes = da.read(loc.to_str().unwrap()).await?;
            // this will put a task in a queue
            cache.insert_bytes(loc, bytes.as_slice())?;
            Ok(bytes)
        }
    }
}

fn read_all<R: Read>(r: &mut R) -> Result<Vec<u8>> {
    let mut v = vec![];
    r.read_to_end(&mut v)?;
    Ok(v)
}
