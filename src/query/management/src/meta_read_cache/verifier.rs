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

//! Bound the staleness of a watch stream that stops without an error.
//!
//! The meta client sets neither TCP nor HTTP/2 keep-alive and the watch stream has no heartbeat,
//! so a half-open connection, or a follower that stopped receiving logs, leaves the cache stale
//! forever. Every interval, each prefix is compared with an ordinary read, which meta-service
//! forwards to the leader.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::Ordering;
use std::time::Duration;

use databend_common_meta_store::MetaStore;
use databend_meta_client::kvapi::KvApiExt;
use databend_meta_client::kvapi::ListOptions;
use log::info;
use log::warn;

use super::MetaReadCache;
use super::prefix_cache::PrefixCache;
use super::prefix_cache::is_live;
use super::prefix_cache::now_ms;

/// Mismatching verifications in a row before a prefix is re-fetched; one mismatch can be a write
/// that the leader returned and the stream has not delivered yet.
const MISMATCHES_TO_RESYNC: u32 = 2;

/// After a mismatch, verify again this soon instead of a full interval later: a write in flight
/// is delivered within milliseconds, a stalled stream is still behind.
const RECHECK_DELAY: Duration = Duration::from_secs(1);

/// A verification read slower than this means the shared client, and likely the watch stream on
/// it, is stuck.
const READ_TIMEOUT: Duration = Duration::from_secs(3);

/// Log the counters every this many rounds.
const LOG_EVERY_ROUNDS: u64 = 6;

pub(crate) fn spawn(cache: Weak<MetaReadCache>, meta: Arc<MetaStore>, interval: Duration) {
    databend_common_base::runtime::spawn(async move {
        let mut round = 0u64;
        let mut delay = interval;
        loop {
            tokio::time::sleep(delay).await;
            let Some(cache) = cache.upgrade() else {
                return;
            };
            // Concurrently, so that one stuck read does not delay the other prefixes.
            futures::future::join_all(cache.prefixes.iter().map(|p| verify(p, &meta, interval)))
                .await;
            let recheck = cache
                .prefixes
                .iter()
                .any(|p| p.mismatches.load(Ordering::Relaxed) > 0);
            delay = if recheck {
                RECHECK_DELAY.min(interval)
            } else {
                interval
            };
            round += 1;
            if round.is_multiple_of(LOG_EVERY_ROUNDS) {
                info!("meta read cache stats: {:?}", cache.stats());
            }
        }
    });
}

async fn verify(p: &PrefixCache, meta: &MetaStore, interval: Duration) {
    if let Some(age) = p.oldest_unobserved_fence() {
        if age > interval {
            p.resync(&format!("a local write is not delivered after {age:?}"));
            return;
        }
    }

    // The watch stream and this read share one client. If the read cannot complete, the stream
    // may be stuck the same way (e.g. a half-open connection) while the caller's own client works,
    // so stop trusting the cache instead of waiting for the client timeout.
    let read = meta.list_kv_collect(ListOptions::unlimited(p.dir.as_str()));
    let timeout = READ_TIMEOUT.min(interval);
    let leader = match tokio::time::timeout(timeout, read).await {
        Ok(Ok(items)) => {
            let now = now_ms();
            items
                .into_iter()
                .filter(|(_, v)| is_live(v, now))
                .map(|(k, v)| (k, v.seq))
                .collect::<BTreeMap<_, _>>()
        }
        Ok(Err(e)) => {
            p.resync(&format!("verify read failed: {e}"));
            return;
        }
        Err(_) => {
            p.resync(&format!("verify read did not complete in {timeout:?}"));
            return;
        }
    };

    let Some(local) = p.snapshot(now_ms()) else {
        // Initializing or re-fetching; readers fall back meanwhile.
        return;
    };

    if leader == local {
        p.verified();
        return;
    }

    let n = p.mismatches.fetch_add(1, Ordering::Relaxed) + 1;
    let behind = leader
        .iter()
        .filter(|(k, seq)| local.get(*k).is_none_or(|s| s < *seq))
        .count();
    let extra = local.keys().filter(|k| !leader.contains_key(*k)).count();
    warn!(
        "meta read cache {}: differs from the leader ({} behind, {} extra), {} time(s) in a row",
        p.name, behind, extra, n
    );
    if n >= MISMATCHES_TO_RESYNC {
        p.mismatches.store(0, Ordering::Relaxed);
        p.resync("differs from the leader in consecutive verifications");
    }
}
