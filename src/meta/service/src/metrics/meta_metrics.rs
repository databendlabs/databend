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
//! The `field` is arbitrary string.

use std::time::Instant;

use databend_common_metrics::count;
use prometheus_client::encoding::text::encode as prometheus_encode;

pub mod server_metrics {
    use std::sync::LazyLock;

    use databend_common_meta_types::NodeId;
    use prometheus_client::metrics::counter::Counter;
    use prometheus_client::metrics::family::Family;
    use prometheus_client::metrics::gauge::Gauge;

    use crate::metrics::registry::load_global_registry;

    macro_rules! key {
        ($key: literal) => {
            concat!("metasrv_server_", $key)
        };
    }

    struct ServerMetrics {
        current_leader_id: Gauge,
        is_leader: Gauge,
        node_is_health: Gauge,
        leader_changes: Counter,
        applying_snapshot: Gauge,
        snapshot_key_num: Gauge,
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
                snapshot_key_num: Gauge::default(),
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
                key!("snapshot_key_num"),
                "snapshot key numbers",
                metrics.snapshot_key_num.clone(),
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

    pub fn set_snapshot_key_num(snapshot_key_num: i64) {
        SERVER_METRICS.snapshot_key_num.set(snapshot_key_num);
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

        use databend_common_meta_types::NodeId;
        use prometheus_client;
        use prometheus_client::encoding::EncodeLabelSet;
        use prometheus_client::metrics::counter::Counter;
        use prometheus_client::metrics::family::Family;
        use prometheus_client::metrics::gauge::Gauge;
        use prometheus_client::metrics::histogram::exponential_buckets;
        use prometheus_client::metrics::histogram::Histogram;

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
        }

        impl StorageMetrics {
            fn init() -> Self {
                let metrics = Self {
                    raft_store_write_failed: Family::default(),
                    raft_store_read_failed: Family::default(),
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
    }
}

pub mod network_metrics {
    use std::sync::LazyLock;
    use std::time::Duration;

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
        rpc_delay_seconds: Histogram,
        rpc_delay_ms: Histogram,
        sent_bytes: Counter,
        recv_bytes: Counter,
        req_inflights: Gauge,
        req_success: Counter,
        req_failed: Counter,
    }

    impl NetworkMetrics {
        pub fn init() -> Self {
            let metrics = Self {
                rpc_delay_seconds: Histogram::new(
                    vec![
                        1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 20.0, 30.0, 60.0,
                    ]
                    .into_iter(),
                ),
                rpc_delay_ms: Histogram::new(
                    vec![
                        1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 2000.0,
                        5000.0, 10000.0, 30000.0, 60000.0,
                    ]
                    .into_iter(),
                ),
                sent_bytes: Counter::default(),
                recv_bytes: Counter::default(),
                req_inflights: Gauge::default(),
                req_success: Counter::default(),
                req_failed: Counter::default(),
            };

            let mut registry = load_global_registry();
            registry.register(
                key!("rpc_delay_seconds"),
                "rpc delay seconds",
                metrics.rpc_delay_seconds.clone(),
            );
            registry.register(
                key!("rpc_delay_ms"),
                "rpc delay milliseconds",
                metrics.rpc_delay_ms.clone(),
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

            metrics
        }
    }

    static NETWORK_METRICS: LazyLock<NetworkMetrics> = LazyLock::new(NetworkMetrics::init);

    pub fn sample_rpc_delay(d: Duration) {
        NETWORK_METRICS.rpc_delay_seconds.observe(d.as_secs_f64());
        NETWORK_METRICS.rpc_delay_ms.observe(d.as_millis() as f64);
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
}

/// RAII metrics counter of in-flight requests count and delay.
#[derive(Default)]
pub(crate) struct RequestInFlight {
    start: Option<Instant>,
}

impl count::Count for RequestInFlight {
    fn incr_count(&mut self, n: i64) {
        network_metrics::incr_request_inflights(n);

        #[allow(clippy::comparison_chain)]
        if n > 0 {
            self.start = Some(Instant::now());
        } else if n < 0 {
            if let Some(s) = self.start {
                network_metrics::sample_rpc_delay(s.elapsed())
            }
        }
    }
}

/// RAII metrics counter of pending raft proposals
#[derive(Default)]
pub(crate) struct ProposalPending;

impl count::Count for ProposalPending {
    fn incr_count(&mut self, n: i64) {
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
