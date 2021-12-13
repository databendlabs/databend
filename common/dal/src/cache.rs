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

use std::ffi::OsStr;
use std::io::Read;
use std::sync::Arc;

use common_base::tokio::sync::RwLock;
use common_cache::LruDiskCache;
use common_exception::Result;
use common_metrics::label_counter;
use common_metrics::label_counter_with_val;

use crate::DataAccessor;

const CACHE_READ_BYTES_FROM_REMOTE: &str = "cache_read_bytes_from_remote";
const CACHE_READ_BYTES_FROM_LOCAL: &str = "cache_read_bytes_from_local";
const CACHE_ACCESS_COUNT: &str = "cache_access_count";
const CACHE_ACCESS_HIT_COUNT: &str = "cache_access_hit_count";

// TODO maybe distinct segments cache and snapshots cache
#[derive(Clone, Debug)]
pub struct DalCache {
    pub cache: Arc<RwLock<LruDiskCache>>,
    tenant_id: String,
    cluster_id: String,
}

impl DalCache {
    pub fn create(cache: Arc<RwLock<LruDiskCache>>, tenant_id: String, cluster_id: String) -> Self {
        Self {
            cache,
            tenant_id,
            cluster_id,
        }
    }

    pub async fn read<K: AsRef<OsStr>>(&self, loc: K, da: &dyn DataAccessor) -> Result<Vec<u8>> {
        let loc = loc.as_ref().to_owned();
        let mut cache = self.cache.write().await;

        label_counter(
            CACHE_ACCESS_COUNT,
            self.tenant_id.as_str(),
            self.cluster_id.as_str(),
        );

        if cache.contains_key(loc.clone()) {
            let mut path = cache.get(loc)?;
            let data = read_all(&mut path)?;

            label_counter(
                CACHE_ACCESS_HIT_COUNT,
                self.tenant_id.as_str(),
                self.cluster_id.as_str(),
            );

            label_counter_with_val(
                CACHE_READ_BYTES_FROM_LOCAL,
                data.len() as u64,
                self.tenant_id.as_str(),
                self.cluster_id.as_str(),
            );

            Ok(data)
        } else {
            let bytes = da.read(loc.to_str().unwrap()).await?;

            cache.insert_bytes(loc, bytes.as_slice())?;

            label_counter_with_val(
                CACHE_READ_BYTES_FROM_REMOTE,
                bytes.len() as u64,
                self.tenant_id.as_str(),
                self.cluster_id.as_str(),
            );

            Ok(bytes)
        }
    }
}

fn read_all<R: Read>(r: &mut R) -> Result<Vec<u8>> {
    let mut v = vec![];
    r.read_to_end(&mut v)?;
    Ok(v)
}
