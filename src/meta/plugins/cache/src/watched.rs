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

use std::collections::BTreeMap;
use std::future;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use databend_meta_client::ClientHandle;
use databend_meta_client::kvapi::KVApi;
use databend_meta_client::kvapi::KvApiExt;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::runtime_api::SpawnApi;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::SeqValue;
use databend_meta_runtime::DatabendRuntime;
use futures::FutureExt;
use futures::TryStreamExt;
use log::debug;
use log::info;
use log::warn;
use parking_lot::Mutex;
use tokio::task::JoinHandle;

use crate::Cache;

/// How often a [`WatchedPrefix`] checks its copy against meta-service by default.
pub const RESYNC_INTERVAL: Duration = Duration::from_secs(60);

/// How long a drift found by a check is given to be explained by changes still on their way.
const RECHECK_DELAY: Duration = Duration::from_secs(2);

/// How long a rebuilt copy may take to initialize before the old one is kept.
const REBUILD_TIMEOUT: Duration = Duration::from_secs(30);

/// A key of the copy that expires within this many ms is not taken for a lost delete: the clock
/// of meta-service, which hides expired keys from a read, may be ahead of ours.
const TTL_SLACK_MS: u64 = 5_000;

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
///
/// The copy trusts the watch to deliver every change. In case one is lost, every resync interval
/// the keys under the prefix are read from meta-service and compared with the copy: a change the
/// copy should hold and does not, still missing after a recheck, gets the copy rebuilt from a new
/// watch. A lost change so lasts about one resync interval. The check is a plain read: unlike a
/// new watch, it does not hold up the writes of meta-service.
pub struct WatchedPrefix {
    /// Without the trailing `/`: the copy holds the keys `{prefix}/...`.
    prefix: String,
    /// Held only to poll the copy once, or to swap in a rebuilt one, never across an await, so
    /// readers do not queue behind one another.
    cache: Arc<Mutex<Cache>>,
    /// The task that checks and rebuilds the copy, see `Resync`.
    resync: JoinHandle<()>,
}

impl Drop for WatchedPrefix {
    fn drop(&mut self) {
        self.resync.abort();
    }
}

impl WatchedPrefix {
    /// Start watching the keys `{prefix}/...`, checked every [`RESYNC_INTERVAL`]. A trailing `/`
    /// of `prefix` is ignored.
    pub async fn new(
        client: Arc<ClientHandle<DatabendRuntime>>,
        prefix: &str,
        name: impl ToString,
    ) -> Self {
        Self::with_resync_interval(client, prefix, name, RESYNC_INTERVAL).await
    }

    /// Like `new`, checking the copy every `resync_interval`.
    pub async fn with_resync_interval(
        client: Arc<ClientHandle<DatabendRuntime>>,
        prefix: &str,
        name: impl ToString,
        resync_interval: Duration,
    ) -> Self {
        let prefix = prefix.trim_end_matches('/').to_string();
        let name = name.to_string();
        let cache = Cache::new(client.clone(), &prefix, &name).await;
        let cache = Arc::new(Mutex::new(cache));
        let task = Resync {
            cache: cache.clone(),
            client,
            prefix: prefix.clone(),
            name: name.clone(),
            interval: resync_interval,
        };
        let resync = DatabendRuntime::spawn(task.run(), Some(format!("{name}-resync")));
        WatchedPrefix {
            prefix,
            cache,
            resync,
        }
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    fn covers(&self, key: &str) -> bool {
        key.strip_prefix(self.prefix.as_str())
            .is_some_and(|rest| rest.starts_with('/'))
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
            ready(&self.cache, |cache| {
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
            ready(&self.cache, |cache| {
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

/// Run `f` on the copy if it can answer now.
fn ready<T>(cache: &Mutex<Cache>, f: impl FnOnce(&mut Cache) -> Option<T>) -> Option<T> {
    f(&mut cache.lock())
}

/// The task of a [`WatchedPrefix`] that checks its copy against meta-service, and rebuilds the
/// copy if it lost a change.
struct Resync {
    cache: Arc<Mutex<Cache>>,
    client: Arc<ClientHandle<DatabendRuntime>>,
    prefix: String,
    name: String,
    interval: Duration,
}

impl Resync {
    async fn run(self) {
        let interval = self.interval.max(Duration::from_millis(100));
        loop {
            tokio::time::sleep(interval + jitter(interval)).await;
            let Some(drift) = self.find_drift(interval.min(RECHECK_DELAY)).await else {
                continue;
            };
            warn!(
                "{}: the watched copy of {}/ lost {} writes and {} deletes; rebuild it",
                self.name, self.prefix, drift.lost_writes, drift.lost_deletes
            );
            self.rebuild().await;
        }
    }

    /// The changes the copy lost, if a check finds some and a recheck after `recheck_delay`
    /// still does.
    async fn find_drift(&self, recheck_delay: Duration) -> Option<Drift> {
        // The copy has applied every change up to `seen_seq` before meta-service is read.
        let seen_seq = ready(&self.cache, |cache| {
            cache.try_access(|data| data.last_seq).now_or_never()?.ok()
        })?;
        let pulled = match pull(&self.client, &self.prefix).await {
            Ok(pulled) => pulled,
            Err(e) => {
                info!("{}: skip a check of {}/: {}", self.name, self.prefix, e);
                return None;
            }
        };
        let drift = self.drift(seen_seq, &pulled)?;
        debug!(
            "{}: the watched copy of {}/ may have lost changes: {:?}; recheck",
            self.name, self.prefix, drift
        );
        tokio::time::sleep(recheck_delay).await;
        self.drift(seen_seq, &pulled)
    }

    /// The changes the copy lost by now, if it is ready and lost some.
    fn drift(&self, seen_seq: u64, pulled: &BTreeMap<String, SeqV>) -> Option<Drift> {
        let now_ms = now_ms();
        ready(&self.cache, |cache| {
            cache
                .try_access(|data| Drift::of(seen_seq, &data.data, pulled, now_ms))
                .now_or_never()?
                .ok()
        })
        .filter(|drift| !drift.is_empty())
    }

    /// Replace the copy with one from a new watch, once that one is ready. The old copy answers
    /// until then, and stays if the new one does not become ready.
    async fn rebuild(&self) {
        let mut fresh = Cache::new(self.client.clone(), &self.prefix, &self.name).await;
        // The watcher of `fresh` holds its data until the initial read is applied.
        match tokio::time::timeout(REBUILD_TIMEOUT, fresh.try_access(|_| ())).await {
            Ok(Ok(())) => {
                let old = std::mem::replace(&mut *self.cache.lock(), fresh);
                // Dropped outside the lock: it stops the old watcher.
                drop(old);
                info!(
                    "{}: rebuilt the watched copy of {}/",
                    self.name, self.prefix
                );
            }
            Ok(Err(e)) => warn!(
                "{}: keep the watched copy of {}/: a new watch failed: {}",
                self.name, self.prefix, e
            ),
            Err(_) => warn!(
                "{}: keep the watched copy of {}/: a new watch did not initialize in {:?}",
                self.name, self.prefix, REBUILD_TIMEOUT
            ),
        }
    }
}

/// Changes a copy lost, found by comparing it with the keys read from meta-service.
#[derive(Debug, Default, PartialEq, Eq)]
struct Drift {
    lost_writes: usize,
    lost_deletes: usize,
}

impl Drift {
    /// The changes `copy` lost, if `pulled` was read from meta-service after `copy` had applied
    /// every change up to `seen_seq`.
    ///
    /// A watch delivers the changes in seq order. A value of `pulled` written by `seen_seq` is
    /// then in `copy`, unless `copy` holds a later one: else its write was lost. A key of `copy`
    /// written by `seen_seq` and absent from `pulled` was deleted, or expired, before `pulled`
    /// was read: if `copy` still holds it unexpired, its delete was lost, or is still on its way,
    /// which a recheck tells apart. A delete `copy` applies after `pulled` was read counts as a
    /// lost write: it costs one needless rebuild.
    fn of(
        seen_seq: u64,
        copy: &BTreeMap<String, SeqV>,
        pulled: &BTreeMap<String, SeqV>,
        now_ms: u64,
    ) -> Self {
        let lost_writes = pulled
            .iter()
            .filter(|(key, value)| {
                value.seq <= seen_seq && copy.get(*key).is_none_or(|held| held.seq < value.seq)
            })
            .count();
        let lost_deletes = copy
            .iter()
            .filter(|(key, value)| {
                value.seq <= seen_seq
                    && !value.is_expired(now_ms + TTL_SLACK_MS)
                    && !pulled.contains_key(*key)
            })
            .count();
        Drift {
            lost_writes,
            lost_deletes,
        }
    }

    fn is_empty(&self) -> bool {
        self.lost_writes == 0 && self.lost_deletes == 0
    }
}

/// The unexpired keys `{prefix}/...` in meta-service, read by its leader.
async fn pull(
    client: &ClientHandle<DatabendRuntime>,
    prefix: &str,
) -> Result<BTreeMap<String, SeqV>, MetaError> {
    let items = client.list(&format!("{prefix}/")).await?;
    items
        .map_err(MetaError::from)
        .try_filter_map(|item| {
            future::ready(Ok(item.value.map(|value| (item.key, SeqV::from(value)))))
        })
        .try_collect()
        .await
}

/// Up to a quarter of `interval`, so that the nodes started together do not all read
/// meta-service at once.
fn jitter(interval: Duration) -> Duration {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    interval / 4 * (nanos % 1_000) / 1_000
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

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use databend_meta_client::DEFAULT_GRPC_MESSAGE_SIZE;
    use databend_meta_client::MetaGrpcClient;
    use databend_meta_client::types::KVMeta;
    use databend_meta_client::types::UpsertKV;
    use databend_meta_test_harness::meta_service_test_harness;
    use databend_meta_test_harness::start_metasrv_cluster;

    use super::*;

    fn map(entries: &[(&str, u64)]) -> BTreeMap<String, SeqV> {
        entries
            .iter()
            .map(|(key, seq)| (key.to_string(), SeqV::new(*seq, vec![])))
            .collect()
    }

    fn drift(lost_writes: usize, lost_deletes: usize) -> Drift {
        Drift {
            lost_writes,
            lost_deletes,
        }
    }

    #[test]
    fn test_drift() {
        let copy = map(&[("a", 1), ("b", 3), ("c", 5)]);
        assert_eq!(Drift::of(5, &copy, &copy, 0), drift(0, 0));
        // Writes after `seen_seq` are still on their way: `d` is new, `c` changed.
        let pulled = map(&[("a", 1), ("b", 3), ("c", 7), ("d", 6)]);
        assert_eq!(Drift::of(5, &copy, &pulled, 0), drift(0, 0));
        // The copy holds a later value of `c` than the read.
        let pulled = map(&[("a", 1), ("b", 3), ("c", 4)]);
        assert_eq!(Drift::of(5, &copy, &pulled, 0), drift(0, 0));
        // The write of `a` at 4 and the insert of `e` at 2 were lost, so was the delete of `b`.
        let pulled = map(&[("a", 4), ("c", 5), ("e", 2)]);
        assert_eq!(Drift::of(5, &copy, &pulled, 0), drift(2, 1));
        // A key written after `seen_seq` and absent from the read is not taken for a lost delete.
        let copy = map(&[("a", 1), ("f", 6)]);
        assert_eq!(Drift::of(5, &copy, &map(&[("a", 1)]), 0), drift(0, 0));
    }

    /// A key of the copy absent from the read, because meta-service takes it for expired, is not
    /// taken for a lost delete, unless it expires well after now.
    #[test]
    fn test_drift_expiring_keys() {
        let now_ms = 1_700_000_000_000;
        let expiring = |expire_at_ms: u64| {
            let meta = KVMeta::new_expires_at(expire_at_ms);
            SeqV::new_with_meta(1, Some(meta), vec![])
        };
        let pulled = BTreeMap::new();
        let copy = BTreeMap::from([("t".to_string(), expiring(now_ms + 1_000))]);
        assert_eq!(Drift::of(5, &copy, &pulled, now_ms), drift(0, 0));
        let copy = BTreeMap::from([("t".to_string(), expiring(now_ms + 60_000))]);
        assert_eq!(Drift::of(5, &copy, &pulled, now_ms), drift(0, 1));
    }

    /// Whether the copy comes to hold `pp/a` = `a` and not `pp/ghost`.
    async fn wait_for(watched: &WatchedPrefix, a: &[u8]) -> bool {
        for _ in 0..200 {
            let got = ready(&watched.cache, |cache| {
                cache
                    .try_access(|d| {
                        let got_a = d.data.get("pp/a").map(|v| v.data.as_slice() == a);
                        got_a == Some(true) && !d.data.contains_key("pp/ghost")
                    })
                    .now_or_never()?
                    .ok()
            });
            if got == Some(true) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    }

    /// Lose the write of `pp/a` and the delete of `pp/ghost`, as if the watch dropped them.
    fn lose_changes(watched: &WatchedPrefix) -> anyhow::Result<()> {
        let mut cache = watched.cache.lock();
        let mut data = cache
            .cache_data()
            .now_or_never()
            .ok_or_else(|| anyhow!("the copy is busy"))?;
        let data = data
            .as_mut()
            .map_err(|e| anyhow!("the copy is not ready: {e}"))?;
        data.data.remove("pp/a");
        data.data
            .insert("pp/ghost".to_string(), SeqV::new(1, b"g".to_vec()));
        Ok(())
    }

    /// Changes the watch lost are repaired by a rebuild of the copy.
    #[test_harness::test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
    #[fastrace::trace]
    async fn test_watched_prefix_repairs_lost_changes() -> anyhow::Result<()> {
        let tcs = start_metasrv_cluster::<DatabendRuntime>(&[0]).await?;
        let address = tcs[0]
            .config
            .grpc
            .api_address()
            .ok_or_else(|| anyhow!("no grpc address"))?;
        let client = MetaGrpcClient::<DatabendRuntime>::try_create(
            vec![address],
            "root",
            "xxx",
            Some(Duration::from_secs(2)),
            Some(Duration::from_secs(10)),
            None,
            DEFAULT_GRPC_MESSAGE_SIZE,
        )?;
        client.upsert_kv(UpsertKV::update("pp/a", b"a")).await?;
        let interval = Duration::from_secs(1);
        let watched =
            WatchedPrefix::with_resync_interval(client.clone(), "pp", "test-resync", interval)
                .await;
        assert!(
            wait_for(&watched, b"a").await,
            "the copy did not become ready"
        );

        lose_changes(&watched)?;
        assert!(
            wait_for(&watched, b"a").await,
            "the lost changes were not repaired"
        );

        // The rebuilt copy follows changes.
        client.upsert_kv(UpsertKV::update("pp/a", b"a2")).await?;
        assert!(
            wait_for(&watched, b"a2").await,
            "the rebuilt copy did not follow a change"
        );
        Ok(())
    }
}
