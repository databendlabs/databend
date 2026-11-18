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

use std::ops::Deref;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use databend_meta_client::ClientHandle;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::SeqValue;
use databend_meta_runtime::DatabendRuntime;
use futures::FutureExt;

use crate::meta_cache_types::MetaCacheTypes;
use crate::meta_client_source;

/// A local copy of the keys under the directory `{dir}/` in meta-service, kept up to date by a
/// watch.
///
/// The `try_*_ready` reads never wait for the copy. While it initializes or reconnects, which
/// holds its lock, or when meta-service cannot be watched, they return `None`, and the caller
/// reads meta-service instead. A read polls the copy once: callers that share a `Cache` behind a
/// lock hold it only for the read, and release it before reading meta-service.
///
/// What they return lags a change by the watch delay, a change made by this process included:
/// use them where a briefly stale value is acceptable, never to read the seq of a
/// compare-and-swap. They hide a key whose TTL has passed, as meta-service does: meta-service
/// removes the key, and tells the watch, only when it applies a later write.
///
/// A directory is used as given, and its keys are those under `{dir}/`: `a` is the directory
/// `a`, and `a/` the empty-named directory inside it, whose keys are `a//...`.
pub struct Cache {
    /// The watched directory.
    dir: String,
    pub(crate) inner: sub_cache::Cache<MetaCacheTypes>,
}

impl Deref for Cache {
    type Target = sub_cache::Cache<MetaCacheTypes>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Cache {
    /// The value of `key` if the copy can answer at once, see [`Cache`].
    ///
    /// `None` if the copy cannot answer, lacks `key`, or holds it past its TTL. The caller reads
    /// meta-service for every `None`: a key the copy lacks may be one the watch has not delivered
    /// yet.
    pub fn try_get_ready(&mut self, key: &str) -> Option<SeqV> {
        let now_ms = now_ms();
        self.inner
            .try_access(|data| {
                data.data
                    .get(key)
                    .filter(|value| !value.is_expired(now_ms))
                    .cloned()
            })
            .now_or_never()?
            .ok()?
    }

    /// The keys and values under the directory `{dir}/` if the copy can answer at once, see
    /// [`Cache`].
    ///
    /// `None` if the copy cannot answer, or `dir` is neither the watched directory nor under it.
    pub fn try_list_dir_ready(&mut self, dir: &str) -> Option<Vec<(String, SeqV)>> {
        let covered = dir
            .strip_prefix(self.dir.as_str())
            .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'));
        if !covered {
            return None;
        }
        let now_ms = now_ms();
        let mut items = self.inner.try_list_dir(dir).now_or_never()?.ok()?;
        items.retain(|(_, value)| !value.is_expired(now_ms));
        Some(items)
    }

    /// Create a new cache of the directory `dir`, watched through `meta_client`.
    ///
    /// The cache will start watching for changes immediately.
    ///
    /// # Parameters
    ///
    /// * `meta_client` - The meta client to interact with the remote data store.
    /// * `dir` - The cache holds the keys under `{dir}/`, see [`Cache`]. It also identifies the
    ///   cache instance.
    /// * `name` - The name of the cache, used for debugging and logging purposes.
    pub async fn new(
        meta_client: Arc<ClientHandle<DatabendRuntime>>,
        dir: impl ToString,
        name: impl ToString,
    ) -> Self {
        let dir = dir.to_string();
        let name = name.to_string();
        let inner = sub_cache::Cache::new(
            meta_client_source::MetaClientSource {
                client: meta_client,
                name: name.clone(),
            },
            &dir,
            name,
        )
        .await;

        Cache { dir, inner }
    }
}

/// The time in ms since the Unix epoch, as meta-service compares it with the TTL of a key.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}
