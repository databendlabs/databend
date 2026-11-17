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

use std::sync::Arc;
use std::sync::Mutex;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use databend_meta_client::ClientHandle;
use databend_meta_client::kvapi::KVApi;
use databend_meta_client::kvapi::KvApiExt;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::SeqValue;
use databend_meta_runtime::DatabendRuntime;
use futures::FutureExt;

use crate::Cache;

/// Reads of the keys under one prefix, answered by a local copy that a meta-service watch keeps
/// up to date, or by meta-service when the copy cannot answer at once.
///
/// Reads never wait for the copy: while it initializes or reconnects, or when meta-service
/// cannot be watched, they go to meta-service. A ready copy answers for every key under the
/// prefix, absent keys included, but lags a change by the watch delay, a change made by this
/// process included. Use it where a briefly stale value is acceptable, never to read the seq of
/// a compare-and-swap.
///
/// A key whose TTL has passed is absent, as from meta-service: meta-service removes it, and the
/// watch tells, only when it applies a later write.
pub struct WatchedPrefix {
    /// Without the trailing `/`: the copy holds the keys `{prefix}/...`.
    prefix: String,
    /// A plain mutex: it is held only to poll the copy once, never across an await, so readers
    /// do not queue behind one another.
    cache: Mutex<Cache>,
}

impl WatchedPrefix {
    /// Start watching the keys `{prefix}/...`. A trailing `/` of `prefix` is ignored.
    pub async fn new(
        client: Arc<ClientHandle<DatabendRuntime>>,
        prefix: &str,
        name: impl ToString,
    ) -> Self {
        let prefix = prefix.trim_end_matches('/').to_string();
        let cache = Cache::new(client, &prefix, name).await;
        WatchedPrefix {
            prefix,
            cache: Mutex::new(cache),
        }
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    fn covers(&self, key: &str) -> bool {
        key.strip_prefix(self.prefix.as_str())
            .is_some_and(|rest| rest.starts_with('/'))
    }

    /// Run `f` on the copy if it can answer now.
    fn ready<T>(&self, f: impl FnOnce(&mut Cache) -> Option<T>) -> Option<T> {
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut cache)
    }

    /// The values of `keys`, all from the copy at one point if it is ready and covers them, or
    /// else all from `kv` in one read.
    pub async fn mget_kv<KV>(
        &self,
        kv: &KV,
        keys: &[String],
    ) -> Result<Vec<Option<SeqV>>, KV::Error>
    where
        KV: KVApi + ?Sized,
    {
        let local = keys.iter().all(|key| self.covers(key)).then(|| {
            let now_ms = now_ms();
            self.ready(|cache| {
                cache
                    .try_access(|data| {
                        keys.iter()
                            .map(|key| live(data.data.get(key), now_ms).cloned())
                            .collect::<Vec<_>>()
                    })
                    .now_or_never()?
                    .ok()
            })
        });
        match local.flatten() {
            Some(values) => Ok(values),
            None => kv.mget_kv(keys).await,
        }
    }

    /// The keys and values under `dir`, from the copy if it is ready and covers `dir`, or else
    /// from `kv`. Like `list_kv` of `{dir}/`: one trailing `/` of `dir` is ignored.
    pub async fn list_kv<KV>(&self, kv: &KV, dir: &str) -> Result<Vec<(String, SeqV)>, KV::Error>
    where KV: KVApi + ?Sized {
        let dir = dir.strip_suffix('/').unwrap_or(dir);
        let local = (dir == self.prefix || self.covers(dir)).then(|| {
            // Not `try_list_dir`: it strips every trailing `/`, and would list `a/b` for `a/b//`.
            let range = format!("{dir}/")..format!("{dir}0");
            let now_ms = now_ms();
            self.ready(|cache| {
                cache
                    .try_access(|data| {
                        data.data
                            .range(range)
                            .filter(|(_, value)| !value.is_expired(now_ms))
                            .map(|(key, value)| (key.clone(), value.clone()))
                            .collect::<Vec<_>>()
                    })
                    .now_or_never()?
                    .ok()
            })
        });
        match local.flatten() {
            Some(items) => Ok(items),
            None => {
                let prefix = format!("{dir}/");
                kv.list_kv_collect(ListOptions::unlimited(prefix.as_str()))
                    .await
            }
        }
    }
}

/// `value` unless its TTL has passed by `now_ms`.
fn live(value: Option<&SeqV>, now_ms: u64) -> Option<&SeqV> {
    value.filter(|value| !value.is_expired(now_ms))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}
