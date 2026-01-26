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

//! Defines meta-service metric.
//!
//! The metric key is built in form of `<namespace>_<sub_system>_<field>`.
//!
//! The `namespace` is `metasrv`.
//! The `subsystem` can be:
//! - server: for metrics about server itself.
//! - raft_network: for metrics about communication between nodes in raft protocol.
//! - raft_storage: for metrics about the local storage of a raft node.
//! - meta_network: for metrics about meta-service grpc api.
//!
//! The `field` is arbitrary string.

use std::time::Instant;

use databend_base::counter;
use prometheus_client::encoding::text::encode as prometheus_encode;

pub mod server_metrics {
    use std::sync::LazyLock;

    use databend_common_meta_raft_store::raft_log_v004::RaftLogStat;
    use databend_common_meta_types::raft_types::NodeId;
    use prometheus_client::metrics::counter::Counter;
    use prometheus_client::metrics::family::Family;
    use prometheus_client::metrics::gauge::Gauge;

    use crate::metrics::registry::load_global_registry;

    macro_rules! key {
        ($key: literal) => {
            concat!("metasrv_server_", $key)
        };
    }

    #[derive(Default, Debug, Clone)]
    pub struct SnapshotStat {
        /// The total number of blocks in the snapshot.
        pub block_count: Gauge,
        /// The total size in bytes of the block data section in the snapshot.
        pub data_size: Gauge,
        /// The total size in bytes of the block index section in the snapshot.
        pub index_size: Gauge,
        /// The average size in bytes of a block.
        pub avg_block_size: Gauge,
        /// The average number of keys per block.
        pub avg_keys_per_block: Gauge,
        /// The total number of read block from cache or from disk.
        pub read_block: Gauge,
        /// The total number of read block from cache.
        pub read_block_from_cache: Gauge,
        /// The total number of read block from disk.
        pub read_block_from_disk: Gauge,
    }

    struct ServerMetrics {
        current_leader_id: Gauge,
        is_leader: Gauge,
        node_is_health: Gauge,
        leader_changes: Counter,
        applying_snapshot: Gauge,

        /// Primary index is index by string key. Each primary index has an optional expire index key.
        ///
        /// `snapshot_key_count = snapshot_primary_index_count + snapshot_expire_index_count`
        snapshot_key_count: Gauge,

        /// `snapshot_key_count = snapshot_primary_index_count + snapshot_expire_index_count`
        snapshot_primary_index_count: Gauge,

        /// `snapshot_key_count = snapshot_primary_index_count + snapshot_expire_index_count`
        snapshot_expire_index_count: Gauge,

        snapshot_stat: SnapshotStat,

        raft_log_cache_items: Gauge,
        raft_log_cache_used_size: Gauge,
        raft_log_wal_open_chunk_size: Gauge,
        raft_log_wal_offset: Gauge,
        raft_log_wal_closed_chunk_count: Gauge,
        raft_log_wal_closed_chunk_total_size: Gauge,

        raft_log_size: Gauge,
        last_log_index: Gauge,
        last_seq: Gauge,
        current_term: Gauge,
        proposals_applied: Gauge,
        proposals_pending: Gauge,
        proposals_failed: Counter,
        read_failed: Counter,
        watchers: Gauge,
        version: Family<Vec<(String, String)>, Gauge>,
    }

    impl ServerMetrics {
        fn init() -> Self {
            let metrics = Self {
                current_leader_id: Gauge::default(),
                is_leader: Gauge::default(),
                node_is_health: Gauge::default(),
                leader_changes: Counter::default(),
                applying_snapshot: Gauge::default(),

                snapshot_key_count: Gauge::default(),
                snapshot_primary_index_count: Gauge::default(),
                snapshot_expire_index_count: Gauge::default(),

                snapshot_stat: Default::default(),

                raft_log_cache_items: Gauge::default(),
                raft_log_cache_used_size: Gauge::default(),
                raft_log_wal_open_chunk_size: Gauge::default(),
                raft_log_wal_offset: Gauge::default(),
                raft_log_wal_closed_chunk_count: Gauge::default(),
                raft_log_wal_closed_chunk_total_size: Gauge::default(),
                raft_log_size: Gauge::default(),
                last_log_index: Gauge::default(),
                last_seq: Gauge::default(),
                current_term: Gauge::default(),
                proposals_applied: Gauge::default(),
                proposals_pending: Gauge::default(),
                proposals_failed: Counter::default(),
                read_failed: Counter::default(),
                watchers: Gauge::default(),
                version: Family::default(),
            };

            let mut registry = load_global_registry();
            registry.register(
                key!("current_leader_id"),
                "current leader",
                metrics.current_leader_id.clone(),
            );
            registry.register(key!("is_leader"), "is leader", metrics.is_leader.clone());
            registry.register(
                key!("node_is_health"),
                "node is health",
                metrics.node_is_health.clone(),
            );
            registry.register(
                key!("leader_changes"),
                "leader changes",
                metrics.leader_changes.clone(),
            );
            registry.register(
                key!("applying_snapshot"),
                "if this node is applying snapshot",
                metrics.applying_snapshot.clone(),
            );
            registry.register(
                key!("snapshot_key_count"),
                "number of keys in the last snapshot",
                metrics.snapshot_key_count.clone(),
            );
            registry.register(
                key!("snapshot_primary_index_count"),
                "number of primary keys in the last snapshot",
                metrics.snapshot_primary_index_count.clone(),
            );
            registry.register(
                key!("snapshot_expire_index_count"),
                "number of expire index keys in the last snapshot",
                metrics.snapshot_expire_index_count.clone(),
            );
            registry.register(
                key!("snapshot_block_count"),
                "number of blocks in the last snapshot",
                metrics.snapshot_stat.block_count.clone(),
            );
            registry.register(
                key!("snapshot_data_size"),
                "size of data section in the last snapshot",
                metrics.snapshot_stat.data_size.clone(),
            );
            registry.register(
                key!("snapshot_index_size"),
                "size of index section in the last snapshot",
                metrics.snapshot_stat.index_size.clone(),
            );
            registry.register(
                key!("snapshot_avg_block_size"),
                "average size of a block in the last snapshot",
                metrics.snapshot_stat.avg_block_size.clone(),
            );
            registry.register(
                key!("snapshot_avg_keys_per_block"),
                "average number of keys per block in the last snapshot",
                metrics.snapshot_stat.avg_keys_per_block.clone(),
            );
            registry.register(
                key!("snapshot_read_block"),
                "total number of read block from cache or from disk",
                metrics.snapshot_stat.read_block.clone(),
            );
            registry.register(
                key!("snapshot_read_block_from_cache"),
                "total number of read block from cache",
                metrics.snapshot_stat.read_block_from_cache.clone(),
            );
            registry.register(
                key!("snapshot_read_block_from_disk"),
                "total number of read block from disk",
                metrics.snapshot_stat.read_block_from_disk.clone(),
            );

            registry.register(
                key!("raft_log_cache_items"),
                "number of items in raft log cache",
                metrics.raft_log_cache_items.clone(),
            );
            registry.register(
                key!("raft_log_cache_used_size"),
                "size of used space in raft log cache",
                metrics.raft_log_cache_used_size.clone(),
            );
            registry.register(
                key!("raft_log_wal_open_chunk_size"),
                "size of open chunk in raft log wal",
                metrics.raft_log_wal_open_chunk_size.clone(),
            );
            registry.register(
                key!("raft_log_wal_offset"),
                "global offset of raft log WAL",
                metrics.raft_log_wal_offset.clone(),
            );
            registry.register(
                key!("raft_log_wal_closed_chunk_count"),
                "number of closed chunks in raft log WAL",
                metrics.raft_log_wal_closed_chunk_count.clone(),
            );
            registry.register(
                key!("raft_log_wal_closed_chunk_total_size"),
                "total size of closed chunks in raft log WAL",
                metrics.raft_log_wal_closed_chunk_total_size.clone(),
            );

            registry.register(
                key!("raft_log_size"),
                "the size in bytes of the on disk data of raft log",
                metrics.raft_log_size.clone(),
            );
            registry.register(
                key!("proposals_applied"),
                "proposals applied",
                metrics.proposals_applied.clone(),
            );
            registry.register(
                key!("last_log_index"),
                "last log index",
                metrics.last_log_index.clone(),
            );
            registry.register(key!("last_seq"), "last seq", metrics.last_seq.clone());
            registry.register(
                key!("current_term"),
                "current term",
                metrics.current_term.clone(),
            );
            registry.register(
                key!("proposals_pending"),
                "proposals pending, raft-log is proposed, not yet applied",
                metrics.proposals_pending.clone(),
            );
            registry.register(
                key!("proposals_failed"),
                "number of failed proposals(raft-log), due to leader change or storage error",
                metrics.proposals_failed.clone(),
            );
            registry.register(
                key!("read_failed"),
                "read failed",
                metrics.read_failed.clone(),
            );
            registry.register(key!("watchers"), "watchers", metrics.watchers.clone());
            registry.register(key!("version"), "version", metrics.version.clone());
            metrics
        }
    }

    static SERVER_METRICS: LazyLock<ServerMetrics> = LazyLock::new(ServerMetrics::init);

    pub fn set_current_leader(current_leader: NodeId) {
        SERVER_METRICS.current_leader_id.set(current_leader as i64);
    }

    pub fn set_is_leader(is_leader: bool) {
        SERVER_METRICS.is_leader.set(is_leader as i64);
    }

    pub fn set_node_is_health(is_health: bool) {
        SERVER_METRICS.node_is_health.set(is_health as i64);
    }

    pub fn incr_leader_change() {
        SERVER_METRICS.leader_changes.inc();
    }

    /// Whether or not state-machine is applying snapshot.
    pub fn incr_applying_snapshot(cnt: i64) {
        SERVER_METRICS.applying_snapshot.inc_by(cnt);
    }

    pub fn set_snapshot_key_count(n: u64) {
        SERVER_METRICS.snapshot_key_count.set(n as i64);
    }
    pub fn set_snapshot_primary_index_count(n: u64) {
        SERVER_METRICS.snapshot_primary_index_count.set(n as i64);
    }
    pub fn set_snapshot_expire_index_count(n: u64) {
        SERVER_METRICS.snapshot_expire_index_count.set(n as i64);
    }

    pub fn snapshot() -> &'static SnapshotStat {
        &SERVER_METRICS.snapshot_stat
    }

    pub fn set_raft_log_stat(st: RaftLogStat) {
        SERVER_METRICS
            .raft_log_cache_items
            .set(st.payload_cache_item_count as i64);
        SERVER_METRICS
            .raft_log_cache_used_size
            .set(st.payload_cache_size as i64);
        SERVER_METRICS
            .raft_log_wal_open_chunk_size
            .set(st.open_chunk.size as i64);
        SERVER_METRICS
            .raft_log_wal_offset
            .set(st.open_chunk.global_end as i64);
        SERVER_METRICS
            .raft_log_wal_closed_chunk_count
            .set(st.closed_chunks.len() as i64);
        SERVER_METRICS
            .raft_log_wal_closed_chunk_total_size
            .set(st.closed_chunks.iter().map(|v| v.size).sum::<u64>() as i64);
    }

    pub fn set_raft_log_size(raft_log_size: u64) {
        SERVER_METRICS.raft_log_size.set(raft_log_size as i64);
    }

    pub fn set_proposals_applied(proposals_applied: u64) {
        SERVER_METRICS
            .proposals_applied
            .set(proposals_applied as i64);
    }

    pub fn set_last_log_index(last_log_index: u64) {
        SERVER_METRICS.last_log_index.set(last_log_index as i64);
    }

    pub fn set_last_seq(last_seq: u64) {
        SERVER_METRICS.last_seq.set(last_seq as i64);
    }

    pub fn set_current_term(current_term: u64) {
        SERVER_METRICS.current_term.set(current_term as i64);
    }

    pub fn incr_proposals_pending(cnt: i64) {
        SERVER_METRICS.proposals_pending.inc_by(cnt);
    }

    pub fn incr_proposals_failed() {
        SERVER_METRICS.proposals_failed.inc();
    }

    /// Accumulate the number of succeeded and failed read requests.
    pub fn incr_read_result<T, E>(r: &Result<T, E>) {
        if r.is_ok() {
            // TODO: success is not collected.
        } else {
            incr_read_failed();
        }
    }

    pub fn incr_read_failed() {
        SERVER_METRICS.read_failed.inc();
    }

    pub fn incr_watchers(cnt: i64) {
        SERVER_METRICS.watchers.inc_by(cnt);
    }

    pub fn set_version(semver: String, sha: String) {
        let labels = &vec![
            ("component".to_string(), "metasrv".to_string()),
            ("semver".to_string(), semver),
            ("sha".to_string(), sha),
        ];
        SERVER_METRICS.version.get_or_create(labels).set(1);
    }
}

pub mod raft_metrics {
    pub mod network {
        use std::sync::LazyLock;

        use databend_common_meta_types::raft_types::NodeId;
        use prometheus_client;
        use prometheus_client::encoding::EncodeLabelSet;
        use prometheus_client::metrics::counter::Counter;
        use prometheus_client::metrics::family::Family;
        use prometheus_client::metrics::gauge::Gauge;
        use prometheus_client::metrics::histogram::Histogram;
        use prometheus_client::metrics::histogram::exponential_buckets;

        macro_rules! key {
            ($key: literal) => {
                concat!("metasrv_raft_network_", $key)
            };
        }

        #[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
        pub struct ToLabels {
            pub to: String,
        }

        #[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
        pub struct FromLabels {
            pub from: String,
        }

        #[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
        pub struct PeerLabels {
            pub id: String,
            pub addr: String,
        }

        struct RaftMetrics {
            active_peers: Family<PeerLabels, Gauge>,
            fail_connect_to_peer: Family<PeerLabels, Gauge>,
            sent_bytes: Family<ToLabels, Counter>,
            recv_bytes: Family<FromLabels, Counter>,
            sent_failures: Family<ToLabels, Counter>,

            append_sent_seconds: Family<ToLabels, Histogram>,
            snapshot_send_success: Family<ToLabels, Counter>,
            snapshot_send_failure: Family<ToLabels, Counter>,
            snapshot_send_inflights: Family<ToLabels, Gauge>,
            snapshot_recv_inflights: Family<FromLabels, Gauge>,
            snapshot_sent_seconds: Family<ToLabels, Histogram>,
            snapshot_recv_seconds: Family<FromLabels, Histogram>,
            snapshot_recv_success: Family<FromLabels, Counter>,
            snapshot_recv_failures: Family<FromLabels, Counter>,
        }

        impl RaftMetrics {
            fn init() -> Self {
                let metrics = Self {
                    active_peers: Family::default(),
                    fail_connect_to_peer: Family::default(),
                    sent_bytes: Family::default(),
                    recv_bytes: Family::default(),
                    sent_failures: Family::default(),
                    append_sent_seconds: Family::new_with_constructor(|| {
                        Histogram::new(exponential_buckets(0.001f64, 2f64, 20))
                    }), // 0.001s ~ 1024s
                    snapshot_send_success: Family::default(),
                    snapshot_send_failure: Family::default(),
                    snapshot_send_inflights: Family::default(),
                    snapshot_recv_inflights: Family::default(),
                    snapshot_sent_seconds: Family::new_with_constructor(|| {
                        Histogram::new(exponential_buckets(1f64, 2f64, 10))
                    }), // 1s ~ 1024s
                    snapshot_recv_seconds: Family::new_with_constructor(|| {
                        Histogram::new(exponential_buckets(1f64, 2f64, 10))
                    }), // 1s ~ 1024s
                    snapshot_recv_success: Family::default(),
                    snapshot_recv_failures: Family::default(),
                };

                let mut registry = crate::metrics::registry::load_global_registry();
                registry.register(
                    key!("active_peers"),
                    "active peers",
                    metrics.active_peers.clone(),
                );
                registry.register(
                    key!("fail_connect_to_peer"),
                    "fail connect to peer",
                    metrics.fail_connect_to_peer.clone(),
                );
                registry.register(key!("sent_bytes"), "sent bytes", metrics.sent_bytes.clone());
                registry.register(key!("recv_bytes"), "recv bytes", metrics.recv_bytes.clone());
                registry.register(
                    key!("sent_failures"),
                    "sent failures",
                    metrics.sent_failures.clone(),
                );
                registry.register(
                    key!("append_sent_seconds"),
                    "append entries sent seconds",
                    metrics.append_sent_seconds.clone(),
                );
                registry.register(
                    key!("snapshot_send_success"),
                    "snapshot send success",
                    metrics.snapshot_send_success.clone(),
                );
                registry.register(
                    key!("snapshot_send_failure"),
                    "snapshot send failure",
                    metrics.snapshot_send_failure.clone(),
                );
                registry.register(
                    key!("snapshot_send_inflights"),
                    "snapshot send inflights",
                    metrics.snapshot_send_inflights.clone(),
                );
                registry.register(
                    key!("snapshot_recv_inflights"),
                    "snapshot recv inflights",
                    metrics.snapshot_recv_inflights.clone(),
                );
                registry.register(
                    key!("snapshot_sent_seconds"),
                    "snapshot sent seconds",
                    metrics.snapshot_sent_seconds.clone(),
                );
                registry.register(
                    key!("snapshot_recv_seconds"),
                    "snapshot recv seconds",
                    metrics.snapshot_recv_seconds.clone(),
                );
                registry.register(
                    key!("snapshot_recv_success"),
                    "snapshot recv success",
                    metrics.snapshot_recv_success.clone(),
                );
                registry.register(
                    key!("snapshot_recv_failures"),
                    "snapshot recv failures",
                    metrics.snapshot_recv_failures.clone(),
                );
                metrics
            }
        }

        static RAFT_METRICS: LazyLock<RaftMetrics> = LazyLock::new(RaftMetrics::init);

        pub fn incr_active_peers(id: &NodeId, addr: &str, cnt: i64) {
            let id = id.to_string();
            RAFT_METRICS
                .active_peers
                .get_or_create(&PeerLabels {
                    id,
                    addr: addr.to_string(),
                })
                .inc_by(cnt);
        }

        pub fn incr_connect_failure(id: &NodeId, addr: &str) {
            let id = id.to_string();
            RAFT_METRICS
                .fail_connect_to_peer
                .get_or_create(&PeerLabels {
                    id,
                    addr: addr.to_string(),
                })
                .inc();
        }

        pub fn incr_sendto_bytes(id: &NodeId, bytes: u64) {
            let to = id.to_string();
            RAFT_METRICS
                .sent_bytes
                .get_or_create(&ToLabels { to })
                .inc_by(bytes);
        }

        pub fn incr_recvfrom_bytes(addr: String, bytes: u64) {
            RAFT_METRICS
                .recv_bytes
                .get_or_create(&FromLabels { from: addr })
                .inc_by(bytes);
        }

        pub fn incr_sendto_result(id: &NodeId, success: bool) {
            if success {
                // success is not collected.
            } else {
                incr_sendto_failure(id);
            }
        }

        pub fn incr_sendto_failure(id: &NodeId) {
            let to = id.to_string();
            RAFT_METRICS
                .sent_failures
                .get_or_create(&ToLabels { to })
                .inc();
        }

        pub fn observe_append_sendto_spent(id: &NodeId, v: f64) {
            let to = id.to_string();
            RAFT_METRICS
                .append_sent_seconds
                .get_or_create(&ToLabels { to })
                .observe(v);
        }

        pub fn incr_snapshot_sendto_result(id: &NodeId, success: bool) {
            let to = id.to_string();
            if success {
                &RAFT_METRICS.snapshot_send_success
            } else {
                &RAFT_METRICS.snapshot_send_failure
            }
            .get_or_create(&ToLabels { to })
            .inc();
        }

        pub fn incr_snapshot_sendto_inflight(id: &NodeId, cnt: i64) {
            let to = id.to_string();
            RAFT_METRICS
                .snapshot_send_inflights
                .get_or_create(&ToLabels { to })
                .inc_by(cnt);
        }

        pub fn incr_snapshot_recvfrom_inflight(addr: String, cnt: i64) {
            RAFT_METRICS
                .snapshot_recv_inflights
                .get_or_create(&FromLabels { from: addr })
                .inc_by(cnt);
        }

        pub fn observe_snapshot_sendto_spent(id: &NodeId, v: f64) {
            let to = id.to_string();
            RAFT_METRICS
                .snapshot_sent_seconds
                .get_or_create(&ToLabels { to })
                .observe(v);
        }

        pub fn observe_snapshot_recvfrom_spent(addr: String, v: f64) {
            RAFT_METRICS
                .snapshot_recv_seconds
                .get_or_create(&FromLabels { from: addr })
                .observe(v);
        }

        pub fn incr_snapshot_recvfrom_result(addr: String, success: bool) {
            if success {
                &RAFT_METRICS.snapshot_recv_success
            } else {
                &RAFT_METRICS.snapshot_recv_failures
            }
            .get_or_create(&FromLabels { from: addr })
            .inc();
        }
    }

    pub mod storage {
        use std::sync::LazyLock;

        use prometheus_client::encoding::EncodeLabelSet;
        use prometheus_client::metrics::counter::Counter;
        use prometheus_client::metrics::family::Family;
        use prometheus_client::metrics::gauge::Gauge;

        use crate::metrics::registry::load_global_registry;

        macro_rules! key {
            ($key: literal) => {
                concat!("metasrv_raft_storage_", $key)
            };
        }

        #[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
        pub struct FuncLabels {
            pub func: String,
        }

        struct StorageMetrics {
            raft_store_write_failed: Family<FuncLabels, Counter>,
            raft_store_read_failed: Family<FuncLabels, Counter>,

            /// The number of tasks that are building a snapshot.
            ///
            /// It should be 0 or 1.
            snapshot_building: Gauge,

            /// The number of entries written to the snapshot file.
            snapshot_written_entries: Counter,
        }

        impl StorageMetrics {
            fn init() -> Self {
                let metrics = Self {
                    raft_store_write_failed: Family::default(),
                    raft_store_read_failed: Family::default(),

                    snapshot_building: Gauge::default(),
                    snapshot_written_entries: Counter::default(),
                };

                let mut registry = load_global_registry();
                registry.register(
                    key!("raft_store_write_failed"),
                    "raft store write failed",
                    metrics.raft_store_write_failed.clone(),
                );
                registry.register(
                    key!("raft_store_read_failed"),
                    "raft store read failed",
                    metrics.raft_store_read_failed.clone(),
                );

                registry.register(
                    key!("snapshot_building"),
                    "The number of tasks that are building a snapshot. It should be 0 or 1.",
                    metrics.snapshot_building.clone(),
                );
                registry.register(
                    key!("snapshot_written_entries"),
                    "The number of entries written to the snapshot file.",
                    metrics.snapshot_written_entries.clone(),
                );

                metrics
            }
        }

        static STORAGE_METRICS: LazyLock<StorageMetrics> = LazyLock::new(StorageMetrics::init);

        pub fn incr_raft_storage_write_result<T, E>(func: &str, result: &Result<T, E>) {
            match result {
                Ok(_) => {
                    // Do not update metrics for success.
                }
                Err(_) => {
                    incr_raft_storage_fail(func, true);
                }
            }
        }

        pub fn incr_raft_storage_read_result<T, E>(func: &str, result: &Result<T, E>) {
            match result {
                Ok(_) => {
                    // Do not update metrics for success.
                }
                Err(_) => {
                    incr_raft_storage_fail(func, false);
                }
            }
        }

        pub fn incr_raft_storage_fail(func: &str, write: bool) {
            let labels = FuncLabels {
                func: func.to_string(),
            };
            if write {
                STORAGE_METRICS
                    .raft_store_write_failed
                    .get_or_create(&labels)
                    .inc();
            } else {
                STORAGE_METRICS
                    .raft_store_read_failed
                    .get_or_create(&labels)
                    .inc();
            }
        }

        pub fn incr_snapshot_building_by(cnt: i64) {
            STORAGE_METRICS.snapshot_building.inc_by(cnt);
        }

        pub fn incr_snapshot_written_entries() {
            STORAGE_METRICS.snapshot_written_entries.inc();
        }
    }
}

pub mod network_metrics {
    use std::sync::LazyLock;
    use std::time::Duration;

    use databend_common_meta_types::protobuf::WatchResponse;
    use log::error;
    use prometheus_client::metrics::counter::Counter;
    use prometheus_client::metrics::gauge::Gauge;
    use prometheus_client::metrics::histogram::Histogram;

    use crate::metrics::registry::load_global_registry;

    macro_rules! key {
        ($key: literal) => {
            concat!("metasrv_meta_network_", $key)
        };
    }

    #[derive(Debug)]
    struct NetworkMetrics {
        rpc_delay_ms: Histogram,
        rpc_delay_read_ms: Histogram,
        rpc_delay_write_ms: Histogram,
        sent_bytes: Counter,
        recv_bytes: Counter,
        req_inflights: Gauge,
        req_success: Counter,
        req_failed: Counter,

        /// Number of items sent during watch stream initialization.
        watch_initialization_item_sent: Counter,

        /// Number of items sent when data changes in a watch stream.
        watch_change_item_sent: Counter,

        /// Number of items sent in a stream get response.
        stream_get_item_sent: Counter,

        /// Number of items sent in a stream mget response.
        stream_mget_item_sent: Counter,

        /// Number of items sent in a stream list response.
        stream_list_item_sent: Counter,
    }

    impl NetworkMetrics {
        pub fn init() -> Self {
            let rpc_delay_buckets = vec![
                1.0, 2.0, 3.0, 4.0, 5.0, 7.0, //
                10.0, 20.0, 30.0, 40.0, 50.0, 70.0, //
                100.0, 200.0, 300.0, 400.0, 500.0, 700.0, //
                1000.0, 2000.0, 3000.0, 4000.0, 5000.0, 7000.0, //
                10000.0, 20000.0, 30000.0, 40000.0, 50000.0, 70000.0,
            ];

            let metrics = Self {
                rpc_delay_ms: Histogram::new(rpc_delay_buckets.clone().into_iter()),
                rpc_delay_read_ms: Histogram::new(rpc_delay_buckets.clone().into_iter()),
                rpc_delay_write_ms: Histogram::new(rpc_delay_buckets.into_iter()),
                sent_bytes: Counter::default(),
                recv_bytes: Counter::default(),
                req_inflights: Gauge::default(),
                req_success: Counter::default(),
                req_failed: Counter::default(),

                watch_initialization_item_sent: Counter::default(),
                watch_change_item_sent: Counter::default(),

                stream_get_item_sent: Counter::default(),
                stream_mget_item_sent: Counter::default(),
                stream_list_item_sent: Counter::default(),
            };

            let mut registry = load_global_registry();
            registry.register(
                key!("rpc_delay_ms"),
                "rpc delay milliseconds",
                metrics.rpc_delay_ms.clone(),
            );
            registry.register(
                key!("rpc_delay_read_ms"),
                "rpc delay milliseconds for read operations",
                metrics.rpc_delay_read_ms.clone(),
            );
            registry.register(
                key!("rpc_delay_write_ms"),
                "rpc delay milliseconds for write operations",
                metrics.rpc_delay_write_ms.clone(),
            );
            registry.register(key!("sent_bytes"), "sent bytes", metrics.sent_bytes.clone());
            registry.register(key!("recv_bytes"), "recv bytes", metrics.recv_bytes.clone());
            registry.register(
                key!("req_inflights"),
                "req inflights",
                metrics.req_inflights.clone(),
            );
            registry.register(
                key!("req_success"),
                "req success",
                metrics.req_success.clone(),
            );
            registry.register(key!("req_failed"), "req failed", metrics.req_failed.clone());

            registry.register(
                key!("watch_initialization"),
                "Number of items sent during watch stream initialization",
                metrics.watch_initialization_item_sent.clone(),
            );
            registry.register(
                key!("watch_change"),
                "Number of items sent when data changes in a watch stream",
                metrics.watch_change_item_sent.clone(),
            );

            registry.register(
                key!("stream_get_item_sent"),
                "Number of items sent in a stream get response",
                metrics.stream_get_item_sent.clone(),
            );
            registry.register(
                key!("stream_mget_item_sent"),
                "Number of items sent in a stream mget response",
                metrics.stream_mget_item_sent.clone(),
            );
            registry.register(
                key!("stream_list_item_sent"),
                "Number of items sent in a stream list response",
                metrics.stream_list_item_sent.clone(),
            );

            metrics
        }
    }

    static NETWORK_METRICS: LazyLock<NetworkMetrics> = LazyLock::new(NetworkMetrics::init);

    /// Sample RPC delay for operations where read/write type is unknown.
    /// Prefer using `sample_rpc_read_delay` or `sample_rpc_write_delay` when the operation type is known.
    #[deprecated(
        note = "Use sample_rpc_read_delay or sample_rpc_write_delay for better metrics granularity"
    )]
    pub fn sample_rpc_delay(d: Duration) {
        NETWORK_METRICS.rpc_delay_ms.observe(d.as_millis() as f64);
    }

    pub fn sample_rpc_read_delay(d: Duration) {
        let delay_ms = d.as_millis() as f64;
        NETWORK_METRICS.rpc_delay_ms.observe(delay_ms);
        NETWORK_METRICS.rpc_delay_read_ms.observe(delay_ms);
    }

    pub fn sample_rpc_write_delay(d: Duration) {
        let delay_ms = d.as_millis() as f64;
        NETWORK_METRICS.rpc_delay_ms.observe(delay_ms);
        NETWORK_METRICS.rpc_delay_write_ms.observe(delay_ms);
    }

    pub fn incr_sent_bytes(bytes: u64) {
        NETWORK_METRICS.sent_bytes.inc_by(bytes);
    }

    pub fn incr_recv_bytes(bytes: u64) {
        NETWORK_METRICS.recv_bytes.inc_by(bytes);
    }

    pub fn incr_request_inflights(cnt: i64) {
        NETWORK_METRICS.req_inflights.inc_by(cnt);
    }

    pub fn incr_request_result(success: bool) {
        if success {
            NETWORK_METRICS.req_success.inc();
        } else {
            NETWORK_METRICS.req_failed.inc();
        }
    }

    /// Increment the number of items sent in a watch response.
    ///
    /// It determines the type of item based on the response type.
    pub fn incr_watch_sent(resp: &WatchResponse) {
        if resp.is_initialization {
            incr_watch_sent_initialization_item();
        } else {
            incr_watch_sent_change_item();
        }
    }

    pub fn incr_watch_sent_initialization_item() {
        NETWORK_METRICS.watch_initialization_item_sent.inc();
    }

    pub fn incr_watch_sent_change_item() {
        NETWORK_METRICS.watch_change_item_sent.inc();
    }

    pub fn incr_stream_sent_item(typ: &'static str) {
        match typ {
            "get" => {
                NETWORK_METRICS.stream_get_item_sent.inc();
            }
            "mget" => {
                NETWORK_METRICS.stream_mget_item_sent.inc();
            }
            "list" => {
                NETWORK_METRICS.stream_list_item_sent.inc();
            }
            _ => {
                error!("Unknown stream item type: {}", typ);
            }
        }
    }
}

/// RAII metrics counter for in-flight requests with const generic to distinguish read/write operations
#[derive(Default)]
pub(crate) struct InFlightMetrics<const IS_READ: bool> {
    start: Option<Instant>,
}

impl<const IS_READ: bool> counter::Counter for InFlightMetrics<IS_READ> {
    fn incr(&mut self, n: i64) {
        network_metrics::incr_request_inflights(n);

        #[allow(clippy::comparison_chain)]
        if n > 0 {
            self.start = Some(Instant::now());
        } else if n < 0 {
            if let Some(s) = self.start {
                let elapsed = s.elapsed();
                if IS_READ {
                    network_metrics::sample_rpc_read_delay(elapsed);
                } else {
                    network_metrics::sample_rpc_write_delay(elapsed);
                }
            }
        }
    }
}

/// Type alias for read request metrics
pub(crate) type InFlightRead = InFlightMetrics<true>;

/// Type alias for write request metrics
pub(crate) type InFlightWrite = InFlightMetrics<false>;

/// RAII metrics counter of pending raft proposals
#[derive(Default)]
pub(crate) struct ProposalPending;

impl counter::Counter for ProposalPending {
    fn incr(&mut self, n: i64) {
        server_metrics::incr_proposals_pending(n);
    }
}

/// Encode metrics as prometheus format string
pub fn meta_metrics_to_prometheus_string() -> String {
    let registry = crate::metrics::registry::load_global_registry();

    let mut text = String::new();
    prometheus_encode(&mut text, &registry).unwrap();
    text
}

#[derive(Default)]
pub(crate) struct SnapshotBuilding;

impl counter::Counter for SnapshotBuilding {
    fn incr(&mut self, n: i64) {
        raft_metrics::storage::incr_snapshot_building_by(n);
    }
}
