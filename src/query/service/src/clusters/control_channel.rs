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

use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use arrow_flight::flight_service_client::FlightServiceClient;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_grpc::ConnectionFactory;
use databend_common_grpc::Http2KeepAliveConfig;
use databend_common_settings::FlightKeepAliveParams;
use log::info;
use log::warn;
use tonic::transport::Channel;

use super::cluster::rpc_client_options;
use crate::servers::flight::FlightClient;
use crate::servers::flight::FlightOperation;
use crate::servers::flight::add_flight_error_context;
use crate::servers::flight::keep_alive::build_keep_alive_config;

/// Distributed query setup sends a few small unary requests to every node.
/// Sending them over one cached HTTP/2 channel per node avoids a TCP handshake
/// and a TIME_WAIT socket per request.
///
/// A channel is reused only if a request on it succeeded within this window, so
/// a channel idle for longer is replaced instead of being trusted.
const CONTROL_CHANNEL_MAX_IDLE: Duration = Duration::from_secs(30);

/// PINGs let a cached channel notice a silently dropped connection; tonic then
/// reconnects on the next request. The interval is the gRPC client minimum and
/// the timeout the gRPC default (gRFC A8), so a dead connection is found within
/// about one idle window.
const CONTROL_CHANNEL_HTTP2_KEEP_ALIVE: Http2KeepAliveConfig = Http2KeepAliveConfig {
    interval: Duration::from_secs(10),
    timeout: Duration::from_secs(20),
    while_idle: true,
};

static CONTROL_CHANNELS: LazyLock<ControlChannelCache<Channel>> =
    LazyLock::new(ControlChannelCache::default);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ControlChannelKey {
    node_id: String,
    address: String,
    // TCP keep-alive is fixed when the connection is created, so channels with
    // different keep-alive settings are not shared.
    keep_alive: (Option<Duration>, Option<Duration>, Option<u32>),
}

impl ControlChannelKey {
    fn new(node_id: &str, address: &str, keep_alive: FlightKeepAliveParams) -> Self {
        ControlChannelKey {
            node_id: node_id.to_string(),
            address: address.to_string(),
            keep_alive: (keep_alive.time, keep_alive.interval, keep_alive.retries),
        }
    }
}

struct CachedControlChannel<C> {
    id: u64,
    channel: C,
    last_success: Instant,
}

struct ControlChannelCache<C> {
    next_id: AtomicU64,
    entries: parking_lot::Mutex<HashMap<ControlChannelKey, CachedControlChannel<C>>>,
}

impl<C> Default for ControlChannelCache<C> {
    fn default() -> Self {
        ControlChannelCache {
            next_id: AtomicU64::new(0),
            entries: parking_lot::Mutex::new(HashMap::new()),
        }
    }
}

/// Drops channels without a successful request within [`CONTROL_CHANNEL_MAX_IDLE`].
/// Dropping the last handle of a channel closes its connection, so a peer that
/// is no longer contacted does not keep a connection open.
fn drop_idle_channels<C>(
    entries: &mut HashMap<ControlChannelKey, CachedControlChannel<C>>,
    now: Instant,
) {
    entries.retain(|key, entry| {
        let idle = now.saturating_duration_since(entry.last_success);
        if idle <= CONTROL_CHANNEL_MAX_IDLE {
            return true;
        }
        info!(
            "Dropped idle flight control channel to node {} at {}, no successful request for {:?}",
            key.node_id, key.address, idle
        );
        false
    });
}

impl<C: Clone> ControlChannelCache<C> {
    /// Returns the cached channel and its id if a request on it succeeded
    /// within [`CONTROL_CHANNEL_MAX_IDLE`].
    fn get(&self, key: &ControlChannelKey, now: Instant) -> Option<(u64, C)> {
        let mut entries = self.entries.lock();
        drop_idle_channels(&mut entries, now);
        entries
            .get(key)
            .map(|entry| (entry.id, entry.channel.clone()))
    }

    /// Records a successful request. `id` is `None` for a newly created channel,
    /// which then becomes the cached channel for `key`.
    fn record_success(&self, key: ControlChannelKey, id: Option<u64>, channel: C, now: Instant) {
        let mut entries = self.entries.lock();
        match id {
            Some(id) => {
                // Refresh only the channel we used; if it was evicted or replaced
                // meanwhile, keep what is cached now.
                if let Some(entry) = entries.get_mut(&key)
                    && entry.id == id
                {
                    entry.last_success = now;
                }
            }
            None => {
                if entries.contains_key(&key) {
                    info!(
                        "Replaced cached flight control channel to node {} at {} with a newly created one",
                        key.node_id, key.address
                    );
                }
                let id = self.next_id.fetch_add(1, Ordering::Relaxed);
                entries.insert(key, CachedControlChannel {
                    id,
                    channel,
                    last_success: now,
                });
            }
        }
        // After the refresh, so a slow request that succeeded keeps its channel.
        drop_idle_channels(&mut entries, now);
    }

    /// Drops the cached channel after a failed request, unless it was already
    /// replaced by another channel.
    fn evict(&self, key: &ControlChannelKey, id: u64, now: Instant) {
        let mut entries = self.entries.lock();
        if entries.get(key).is_some_and(|entry| entry.id == id) {
            entries.remove(key);
        }
        drop_idle_channels(&mut entries, now);
    }
}

#[async_backtrace::framed]
async fn create_control_channel(
    config: &InnerConfig,
    remote_node_id: &str,
    address: &str,
    keep_alive: FlightKeepAliveParams,
) -> Result<Channel> {
    let (timeout, rpc_tls_config) = rpc_client_options(config);
    let keep_alive_config = build_keep_alive_config(keep_alive);
    let target = address.to_string();

    // tonic spawns the channel's background tasks on the current runtime. The
    // channel outlives this query, so create it on the global runtime.
    let channel = GlobalIORuntime::instance()
        .spawn(async move {
            ConnectionFactory::create_rpc_channel_with_http2_keep_alive(
                target,
                timeout,
                rpc_tls_config,
                keep_alive_config,
                CONTROL_CHANNEL_HTTP2_KEEP_ALIVE,
            )
            .await
        })
        .await
        .map_err(|cause| {
            ErrorCode::TokioError(format!("Failed to create flight channel: {}", cause))
        })?;

    let channel = channel.map_err(|error| {
        add_flight_error_context(
            ErrorCode::from(error),
            FlightOperation::Connect,
            &config.query.node_id,
            remote_node_id,
        )
    })?;

    info!(
        "Created flight control channel to node {} at {}",
        remote_node_id, address
    );
    Ok(channel)
}

/// The channel used for one control-plane request: the cached channel for the
/// peer, or a newly created one.
pub(super) struct ControlChannel {
    key: ControlChannelKey,
    cached_id: Option<u64>,
    channel: Channel,
}

impl ControlChannel {
    pub(super) async fn acquire(
        config: &InnerConfig,
        node_id: &str,
        address: &str,
        keep_alive: FlightKeepAliveParams,
    ) -> Result<ControlChannel> {
        let key = ControlChannelKey::new(node_id, address, keep_alive);
        let (cached_id, channel) = match CONTROL_CHANNELS.get(&key, Instant::now()) {
            Some((id, channel)) => (Some(id), channel),
            None => (
                None,
                create_control_channel(config, node_id, address, keep_alive).await?,
            ),
        };
        Ok(ControlChannel {
            key,
            cached_id,
            channel,
        })
    }

    pub(super) fn client(&self, local_node_id: &str, remote_node_id: &str) -> FlightClient {
        FlightClient::new(
            FlightServiceClient::new(self.channel.clone()),
            local_node_id,
            remote_node_id,
        )
    }

    /// Caches the channel after a successful request, or drops the cached
    /// channel after a failed one.
    pub(super) fn finish<T>(self, result: &Result<T>) {
        match (result, self.cached_id) {
            (Ok(_), _) => CONTROL_CHANNELS.record_success(
                self.key,
                self.cached_id,
                self.channel,
                Instant::now(),
            ),
            (Err(cause), Some(id)) => {
                warn!(
                    "Dropped cached flight control channel to node {} at {} after a failed request: {}",
                    self.key.node_id, self.key.address, cause
                );
                CONTROL_CHANNELS.evict(&self.key, id, Instant::now());
            }
            (Err(_), None) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(node_id: &str) -> ControlChannelKey {
        ControlChannelKey::new(node_id, "127.0.0.1:9090", FlightKeepAliveParams::default())
    }

    #[test]
    fn test_control_channel_cached_after_success() {
        let cache = ControlChannelCache::<u32>::default();
        let now = Instant::now();
        assert!(cache.get(&key("n1"), now).is_none());

        cache.record_success(key("n1"), None, 7, now);
        let (_, channel) = cache.get(&key("n1"), now).unwrap();
        assert_eq!(channel, 7);
        assert!(cache.get(&key("n2"), now).is_none());
    }

    #[test]
    fn test_control_channel_expires_without_success() {
        let cache = ControlChannelCache::<u32>::default();
        let now = Instant::now();
        cache.record_success(key("n1"), None, 7, now);

        let later = now + CONTROL_CHANNEL_MAX_IDLE + Duration::from_secs(1);
        assert!(cache.get(&key("n1"), later).is_none());
        assert!(cache.entries.lock().is_empty());
    }

    #[test]
    fn test_control_channel_success_refreshes_idle_window() {
        let cache = ControlChannelCache::<u32>::default();
        let now = Instant::now();
        cache.record_success(key("n1"), None, 7, now);
        let (id, _) = cache.get(&key("n1"), now).unwrap();

        let refreshed = now + CONTROL_CHANNEL_MAX_IDLE;
        cache.record_success(key("n1"), Some(id), 7, refreshed);
        let later = refreshed + CONTROL_CHANNEL_MAX_IDLE;
        assert_eq!(cache.get(&key("n1"), later).unwrap().0, id);
    }

    #[test]
    fn test_control_channel_evicted_after_failure() {
        let cache = ControlChannelCache::<u32>::default();
        let now = Instant::now();
        cache.record_success(key("n1"), None, 7, now);
        let (id, _) = cache.get(&key("n1"), now).unwrap();

        cache.evict(&key("n1"), id, now);
        assert!(cache.get(&key("n1"), now).is_none());
    }

    #[test]
    fn test_control_channel_stale_id_keeps_newer_channel() {
        let cache = ControlChannelCache::<u32>::default();
        let now = Instant::now();
        cache.record_success(key("n1"), None, 7, now);
        let (old_id, _) = cache.get(&key("n1"), now).unwrap();
        // Another request replaced the channel, e.g. after a concurrent miss.
        cache.record_success(key("n1"), None, 8, now);

        // A late failure on the old channel must not drop the new one.
        cache.evict(&key("n1"), old_id, now);
        let (new_id, channel) = cache.get(&key("n1"), now).unwrap();
        assert_eq!(channel, 8);

        // A late success on the old channel must not refresh the new one.
        let later = now + CONTROL_CHANNEL_MAX_IDLE;
        cache.record_success(key("n1"), Some(old_id), 7, later);
        let expired = now + CONTROL_CHANNEL_MAX_IDLE + Duration::from_secs(1);
        assert!(cache.get(&key("n1"), expired).is_none());
        assert_ne!(old_id, new_id);
    }

    #[test]
    fn test_control_channel_new_entry_purges_expired() {
        let cache = ControlChannelCache::<u32>::default();
        let now = Instant::now();
        cache.record_success(key("n1"), None, 7, now);

        let later = now + CONTROL_CHANNEL_MAX_IDLE + Duration::from_secs(1);
        cache.record_success(key("n2"), None, 8, later);
        let entries = cache.entries.lock();
        assert_eq!(entries.len(), 1);
        assert!(entries.contains_key(&key("n2")));
    }

    #[test]
    fn test_control_channel_lookup_drops_expired_peers() {
        let cache = ControlChannelCache::<u32>::default();
        let now = Instant::now();
        cache.record_success(key("gone"), None, 7, now);

        // Looking up another peer drops the expired channel of a peer that is
        // no longer contacted.
        let later = now + CONTROL_CHANNEL_MAX_IDLE + Duration::from_secs(1);
        assert!(cache.get(&key("n1"), later).is_none());
        assert!(cache.entries.lock().is_empty());
    }

    #[test]
    fn test_control_channel_slow_success_keeps_channel() {
        let cache = ControlChannelCache::<u32>::default();
        let now = Instant::now();
        cache.record_success(key("n1"), None, 7, now);
        let (id, _) = cache.get(&key("n1"), now).unwrap();

        // A request that took longer than the idle window still succeeded on
        // this channel, so the channel stays cached.
        let finished = now + CONTROL_CHANNEL_MAX_IDLE + Duration::from_secs(1);
        cache.record_success(key("n1"), Some(id), 7, finished);
        assert_eq!(cache.get(&key("n1"), finished).unwrap().0, id);
    }

    #[test]
    fn test_control_channel_key_separates_keep_alive() {
        let keep_alive = FlightKeepAliveParams {
            time: Some(Duration::from_secs(30)),
            interval: None,
            retries: None,
        };
        let with_keep_alive = ControlChannelKey::new("n1", "127.0.0.1:9090", keep_alive);
        assert_ne!(key("n1"), with_keep_alive);
    }
}
