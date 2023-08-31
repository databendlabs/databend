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

use common_metrics::counter;
use log::error;

fn f64_of(b: bool) -> f64 {
    if b { 1_f64 } else { 0_f64 }
}

pub mod server_metrics {
    use common_meta_types::NodeId;
    use lazy_static::lazy_static;
    use prometheus;
    use prometheus::register_gauge;
    use prometheus::register_int_counter;
    use prometheus::Gauge;
    use prometheus::IntCounter;

    use crate::metrics::meta_metrics::f64_of;

    macro_rules! key {
        ($key: literal) => {
            concat!("metasrv_server_", $key)
        };
    }

    struct ServerMetrics {
        current_leader: Gauge,
        is_leader: Gauge,
        node_is_health: Gauge,
        leader_changes: IntCounter,
        applying_snapshot: Gauge,
        proposal_applied: Gauge,
        last_log_index: Gauge,
        last_seq: Gauge,
        current_term: Gauge,
        proposals_pending: Gauge,
        proposals_failed: IntCounter,
        read_failed: IntCounter,
        watchers: Gauge,
    }

    impl ServerMetrics {
        fn init() -> Self {
            Self {
                current_leader: register_gauge!(key!("current_leader"), "current leader of raft")
                    .unwrap(),
                is_leader: register_gauge!(key!("is_leader"), "whether is leader of raft").unwrap(),
                node_is_health: register_gauge!(key!("node_is_health"), "whether node is health")
                    .unwrap(),
                leader_changes: register_int_counter!(key!("leader_changes"), "leader changes")
                    .unwrap(),
                applying_snapshot: register_gauge!(
                    key!("applying_snapshot"),
                    "whether applying snapshot"
                )
                .unwrap(),
                proposal_applied: register_gauge!(key!("proposal_applied"), "proposal applied")
                    .unwrap(),
                last_log_index: register_gauge!(key!("last_log_index"), "last log index").unwrap(),
                last_seq: register_gauge!(key!("last_seq"), "last seq").unwrap(),
                current_term: register_gauge!(key!("current_term"), "current term").unwrap(),
                proposals_pending: register_gauge!(key!("proposals_pending"), "proposals pending")
                    .unwrap(),
                proposals_failed: register_int_counter!(
                    key!("proposals_failed"),
                    "proposals failed"
                )
                .unwrap(),
                read_failed: register_int_counter!(key!("read_failed"), "read failed").unwrap(),
                watchers: register_gauge!(key!("watchers"), "number of watchers").unwrap(),
            }
        }
    }

    lazy_static! {
        static ref SERVER_METRICS: ServerMetrics = ServerMetrics::init();
    }

    pub fn set_current_leader(current_leader: NodeId) {
        SERVER_METRICS.current_leader.set(current_leader as f64);
    }

    pub fn set_is_leader(is_leader: bool) {
        SERVER_METRICS.is_leader.set(f64_of(is_leader));
    }

    pub fn set_node_is_health(is_health: bool) {
        SERVER_METRICS.node_is_health.set(f64_of(is_health));
    }

    pub fn incr_leader_change() {
        SERVER_METRICS.leader_changes.inc();
    }

    /// Whether or not state-machine is applying snapshot.
    pub fn incr_applying_snapshot(cnt: i64) {
        SERVER_METRICS.applying_snapshot.add(cnt as f64);
    }

    pub fn set_proposals_applied(proposals_applied: u64) {
        SERVER_METRICS
            .proposal_applied
            .set(proposals_applied as f64);
    }

    pub fn set_last_log_index(last_log_index: u64) {
        SERVER_METRICS.last_log_index.set(last_log_index as f64);
    }

    pub fn set_last_seq(last_seq: u64) {
        SERVER_METRICS.last_seq.set(last_seq as f64);
    }

    pub fn set_current_term(current_term: u64) {
        SERVER_METRICS.current_term.set(current_term as f64);
    }

    pub fn incr_proposals_pending(cnt: i64) {
        SERVER_METRICS.proposals_pending.add(cnt as f64);
    }

    pub fn incr_proposals_failed() {
        SERVER_METRICS.proposals_failed.inc();
    }

    pub fn incr_read_failed() {
        SERVER_METRICS.read_failed.inc();
    }

    pub fn incr_watchers(cnt: i64) {
        SERVER_METRICS.watchers.add(cnt as f64);
    }
}

pub mod raft_metrics {
    pub mod network {
        use common_meta_types::NodeId;
        use metrics::counter;
        use metrics::histogram;
        use metrics::increment_gauge;

        macro_rules! key {
            ($key: literal) => {
                concat!("metasrv_raft_network_", $key)
            };
        }

        pub fn incr_active_peers(id: &NodeId, addr: &str, cnt: i64) {
            let labels = [("id", id.to_string()), ("address", addr.to_owned())];
            increment_gauge!(key!("active_peers"), cnt as f64, &labels);
        }

        pub fn incr_fail_connections_to_peer(id: &NodeId, addr: &str) {
            let labels = [("id", id.to_string()), ("address", addr.to_owned())];
            counter!(key!("fail_connect_to_peer"), 1, &labels);
        }

        pub fn incr_sent_bytes_to_peer(id: &NodeId, bytes: u64) {
            let labels = [("to", id.to_string())];
            counter!(key!("sent_bytes"), bytes, &labels);
        }

        pub fn incr_recv_bytes_from_peer(addr: String, bytes: u64) {
            let labels = [("from", addr)];
            counter!(key!("recv_bytes"), bytes, &labels);
        }

        pub fn incr_sent_failure_to_peer(id: &NodeId) {
            let labels = [("to", id.to_string())];
            counter!(key!("sent_failures"), 1, &labels);
        }

        pub fn incr_snapshot_send_success_to_peer(id: &NodeId) {
            let labels = [("to", id.to_string())];
            counter!(key!("snapshot_send_success"), 1, &labels);
        }

        pub fn incr_snapshot_send_failures_to_peer(id: &NodeId) {
            let labels = [("to", id.to_string())];
            counter!(key!("snapshot_send_failures"), 1, &labels);
        }

        pub fn incr_snapshot_send_inflights_to_peer(id: &NodeId, cnt: i64) {
            let labels = [("to", id.to_string())];
            increment_gauge!(key!("snapshot_send_inflights"), cnt as f64, &labels);
        }

        pub fn incr_snapshot_recv_inflights_from_peer(addr: String, cnt: i64) {
            let labels = [("to", addr)];
            increment_gauge!(key!("snapshot_recv_inflights"), cnt as f64, &labels);
        }

        pub fn sample_snapshot_sent(id: &NodeId, v: f64) {
            let labels = [("to", id.to_string())];
            histogram!(key!("snapshot_sent_seconds"), v, &labels);
        }

        pub fn sample_snapshot_recv(addr: String, v: f64) {
            let labels = [("from", addr)];
            histogram!(key!("snapshot_recv_seconds"), v, &labels);
        }

        pub fn incr_snapshot_recv_failure_from_peer(addr: String) {
            let labels = [("from", addr)];
            counter!(key!("snapshot_recv_failures"), 1, &labels);
        }

        pub fn incr_snapshot_recv_success_from_peer(addr: String) {
            let labels = [("from", addr)];
            counter!(key!("snapshot_recv_success"), 1, &labels);
        }
    }

    pub mod storage {
        use metrics::counter;

        macro_rules! key {
            ($key: literal) => {
                concat!("metasrv_raft_storage_", $key)
            };
        }
        pub fn incr_raft_storage_fail(func: &str, write: bool) {
            let labels = [("func", func.to_string())];
            if write {
                counter!(key!("raft_store_write_failed"), 1, &labels);
            } else {
                counter!(key!("raft_store_read_failed"), 1, &labels);
            }
        }
    }
}

pub mod network_metrics {
    use std::time::Duration;

    use metrics::counter;
    use metrics::histogram;
    use metrics::increment_gauge;

    macro_rules! key {
        ($key: literal) => {
            concat!("metasrv_meta_network_", $key)
        };
    }

    pub fn sample_rpc_delay_seconds(d: Duration) {
        histogram!(key!("rpc_delay_seconds"), d);
    }

    pub fn incr_sent_bytes(bytes: u64) {
        counter!(key!("sent_bytes"), bytes);
    }

    pub fn incr_recv_bytes(bytes: u64) {
        counter!(key!("recv_bytes"), bytes);
    }

    pub fn incr_request_inflights(cnt: i64) {
        increment_gauge!(key!("req_inflights"), cnt as f64);
    }

    pub fn incr_request_result(success: bool) {
        if success {
            counter!(key!("req_success"), 1);
        } else {
            counter!(key!("req_failed"), 1);
        }
    }
}

/// RAII metrics counter of in-flight requests count and delay.
#[derive(Default)]
pub(crate) struct RequestInFlight {
    start: Option<Instant>,
}

impl counter::Count for RequestInFlight {
    fn incr_count(&mut self, n: i64) {
        network_metrics::incr_request_inflights(n);

        #[allow(clippy::comparison_chain)]
        if n > 0 {
            self.start = Some(Instant::now());
        } else if n < 0 {
            if let Some(s) = self.start {
                network_metrics::sample_rpc_delay_seconds(s.elapsed())
            }
        }
    }
}

/// RAII metrics counter of pending raft proposals
#[derive(Default)]
pub(crate) struct ProposalPending;

impl counter::Count for ProposalPending {
    fn incr_count(&mut self, n: i64) {
        server_metrics::incr_proposals_pending(n);
    }
}

/// Encode metrics as prometheus format string
pub fn meta_metrics_to_prometheus_string() -> String {
    let prometheus_handle = common_metrics::try_handle()
        .ok_or_else(|| {
            error!("could not get prometheus_handle");
        })
        .unwrap();

    prometheus_handle.render()
}
