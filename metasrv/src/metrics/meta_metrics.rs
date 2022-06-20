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

use std::sync::Once;

use common_meta_types::NodeId;
use lazy_static::lazy_static;
use prometheus::exponential_buckets;
use prometheus::CounterVec;
use prometheus::Gauge;
use prometheus::GaugeVec;
use prometheus::HistogramOpts;
use prometheus::HistogramVec;
use prometheus::IntCounter;
use prometheus::IntCounterVec;
use prometheus::IntGauge;
use prometheus::IntGaugeVec;
use prometheus::Opts;
use prometheus::Registry;

pub const META_NAMESPACE: &str = "metasrv";
pub const SERVER_SUBSYSTEM: &str = "server";
pub const RAFT_NETWORK_SUBSYSTEM: &str = "raft_network";
pub const META_NETWORK_SUBSYSTEM: &str = "meta_network";

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    // server metrics
    pub static ref CURRENT_LEADER: IntGauge = IntGauge::with_opts(
        Opts::new("current_leader_id", "Current leader id of cluster, 0 means no leader.")
            .namespace(META_NAMESPACE)
            .subsystem(SERVER_SUBSYSTEM)
    )
    .expect("meta metric cannot be created");

    pub static ref IS_LEADER: IntGauge = IntGauge::with_opts(
        Opts::new("is_leader", "Whether or not this node is current leader.")
            .namespace(META_NAMESPACE)
            .subsystem(SERVER_SUBSYSTEM)
    )
    .expect("meta metric cannot be created");

    pub static ref NODE_IS_HEALTH: IntGauge = IntGauge::with_opts(
        Opts::new("node_is_health", "Whether or not this node is health(in leader/follower state).")
            .namespace(META_NAMESPACE)
            .subsystem(SERVER_SUBSYSTEM)
    )
    .expect("meta metric cannot be created");

    pub static ref LEADER_CHANGES: IntCounter = IntCounter::with_opts(
        Opts::new("leader_changes", "Number of leader changes seen.")
            .namespace(META_NAMESPACE)
            .subsystem(SERVER_SUBSYSTEM)
    )
    .expect("meta metric cannot be created");

    pub static ref APPLYING_SNAPSHOT: IntGauge = IntGauge::with_opts(
        Opts::new(
            "applying_snapshot",
            "Whether or not statemachine is applying snapshot."
        )
        .namespace(META_NAMESPACE)
        .subsystem(SERVER_SUBSYSTEM)
    )
    .expect("meta metric cannot be created");

    pub static ref PROPOSALS_APPLIED: Gauge = Gauge::with_opts(
        Opts::new(
            "proposals_applied",
            "Total number of consensus proposals applied."
        )
        .namespace(META_NAMESPACE)
        .subsystem(SERVER_SUBSYSTEM)
    )
    .expect("meta metric cannot be created");

    pub static ref PROPOSALS_PENDING: IntGauge = IntGauge::with_opts(
        Opts::new("proposals_pending", "Total number of pending proposals.")
            .namespace(META_NAMESPACE)
            .subsystem(SERVER_SUBSYSTEM)
    )
    .expect("meta metric cannot be created");

    pub static ref PROPOSALS_FAILED: IntCounter = IntCounter::with_opts(
        Opts::new("proposals_failed", "Total number of failed proposals.")
            .namespace(META_NAMESPACE)
            .subsystem(SERVER_SUBSYSTEM)
    )
    .expect("meta metric cannot be created");

    pub static ref READ_FAILED: IntCounter = IntCounter::with_opts(
        Opts::new("read_failed", "Total number of failed read request.")
            .namespace(META_NAMESPACE)
            .subsystem(SERVER_SUBSYSTEM)
    )
    .expect("meta metric cannot be created");

    pub static ref WATCHERS: IntGauge = IntGauge::with_opts(
        Opts::new("watchers", "Total number of active watchers.")
            .namespace(META_NAMESPACE)
            .subsystem(SERVER_SUBSYSTEM)
    )
    .expect("meta metric cannot be created");

    // network metrics
    pub static ref ACTIVE_PEERS: GaugeVec = GaugeVec::new(
        Opts::new("active_peers", "Current number of active connections to peers.")
            .namespace(META_NAMESPACE)
            .subsystem(RAFT_NETWORK_SUBSYSTEM),
        &["id", "address"],
    )
    .expect("meta metric cannot be created");

    pub static ref CONNECT_TO_PEER_FAIL: CounterVec = CounterVec::new(
        Opts::new("fail_connect_to_peer", "Total number of fail connections to peers.")
            .namespace(META_NAMESPACE)
            .subsystem(RAFT_NETWORK_SUBSYSTEM),
        &["id", "address"],
    )
    .expect("meta metric cannot be created");

    pub static ref SENT_BYTES: CounterVec = CounterVec::new(
        Opts::new("sent_bytes", "Total number of sent bytes to peers.")
            .namespace(META_NAMESPACE)
            .subsystem(RAFT_NETWORK_SUBSYSTEM),
        &["to"],
    )
    .expect("meta metric cannot be created");

    pub static ref RECV_BYTES: CounterVec = CounterVec::new(
        Opts::new("recv_bytes", "Total number of received bytes from peers.")
            .namespace(META_NAMESPACE)
            .subsystem(RAFT_NETWORK_SUBSYSTEM),
        &["from"],
    )
    .expect("meta metric cannot be created");

    pub static ref SENT_FAILURES: CounterVec = CounterVec::new(
        Opts::new("sent_failures", "Total number of send failures to peers.")
            .namespace(META_NAMESPACE)
            .subsystem(RAFT_NETWORK_SUBSYSTEM),
        &["to"],
    )
    .expect("meta metric cannot be created");

    pub static ref SNAPSHOT_SEND_SUCCESS: IntCounterVec = IntCounterVec::new(
        Opts::new("snapshot_send_success", "Total number of successful snapshot sends.")
            .namespace(META_NAMESPACE)
            .subsystem(RAFT_NETWORK_SUBSYSTEM),
        &["to"],
    )
    .expect("meta metric cannot be created");

    pub static ref SNAPSHOT_SEND_FAILURES: IntCounterVec = IntCounterVec::new(
        Opts::new("snapshot_send_failures", "Total number of snapshot send failures.")
            .namespace(META_NAMESPACE)
            .subsystem(RAFT_NETWORK_SUBSYSTEM),
        &["to"],
    )
    .expect("meta metric cannot be created");

    pub static ref SNAPSHOT_SEND_INFLIGHTS: IntGaugeVec = IntGaugeVec::new(
        Opts::new("snapshot_send_inflights", "Total number of inflight snapshot sends.")
            .namespace(META_NAMESPACE)
            .subsystem(RAFT_NETWORK_SUBSYSTEM),
        &["to"],
    )
    .expect("meta metric cannot be created");

    pub static ref SNAPSHOT_SENT_SECONDS: HistogramVec = HistogramVec::new(
        HistogramOpts::new("snapshot_sent_seconds", "Total latency distributions of snapshot sends.")
            .namespace(META_NAMESPACE)
            .subsystem(RAFT_NETWORK_SUBSYSTEM)
            // lowest bucket start of upper bound 0.1 sec (100 ms) with factor 2
            // highest bucket start of 0.1 sec * 2^9 == 51.2 sec
            .buckets(exponential_buckets(0.1, 2.0, 10).unwrap()),
        &["to"],
    )
    .expect("meta metric cannot be created");

    pub static ref SNAPSHOT_RECV_INFLIGHTS: IntGaugeVec = IntGaugeVec::new(
        Opts::new("snapshot_recv_inflights", "Total number of inflight snapshot receives.")
            .namespace(META_NAMESPACE)
            .subsystem(RAFT_NETWORK_SUBSYSTEM),
        &["from"],
    )
    .expect("meta metric cannot be created");

    pub static ref SNAPSHOT_RECV_FAILURES: IntCounterVec = IntCounterVec::new(
        Opts::new("snapshot_recv_failures", "Total number of snapshot receive failures.")
            .namespace(META_NAMESPACE)
            .subsystem(RAFT_NETWORK_SUBSYSTEM),
        &["from"],
    )
    .expect("meta metric cannot be created");

    pub static ref SNAPSHOT_RECV_SUCCESS: IntCounterVec = IntCounterVec::new(
        Opts::new("snapshot_recv_success", "Total number of successful receive snapshot.")
            .namespace(META_NAMESPACE)
            .subsystem(RAFT_NETWORK_SUBSYSTEM),
        &["from"],
    )
    .expect("meta metric cannot be created");

    pub static ref SNAPSHOT_RECV_SECONDS: HistogramVec = HistogramVec::new(
        HistogramOpts::new("snapshot_recv_seconds", "Total latency distributions of snapshot receives.")
            .namespace(META_NAMESPACE)
            .subsystem(RAFT_NETWORK_SUBSYSTEM)
            // lowest bucket start of upper bound 0.1 sec (100 ms) with factor 2
            // highest bucket start of 0.1 sec * 2^9 == 51.2 sec
            .buckets(exponential_buckets(0.1, 2.0, 10).unwrap()),
        &["from"],
    )
    .expect("meta metric cannot be created");

    pub static ref META_SERVICE_SENT_BYTES: IntCounter = IntCounter::with_opts(
        Opts::new("sent_bytes", "Total number of sent bytes to meta grpc client.")
            .namespace(META_NAMESPACE)
            .subsystem(META_NETWORK_SUBSYSTEM)
    )
    .expect("meta metric cannot be created");

    pub static ref META_SERVICE_RECV_BYTES: IntCounter = IntCounter::with_opts(
        Opts::new("recv_bytes", "Total number of recv bytes from meta grpc client.")
            .namespace(META_NAMESPACE)
            .subsystem(META_NETWORK_SUBSYSTEM)
    )
    .expect("meta metric cannot be created");

    pub static ref META_REQUEST_INFLIGHTS: IntGauge = IntGauge::with_opts(
        Opts::new("req_inflights", "Total number of inflight meta grpc requests.")
            .namespace(META_NAMESPACE)
            .subsystem(META_NETWORK_SUBSYSTEM)
    )
    .expect("meta metric cannot be created");

    pub static ref META_SERVICE_SUCCESS: IntCounter = IntCounter::with_opts(
        Opts::new("req_success", "Total number of success request from meta grpc client.")
            .namespace(META_NAMESPACE)
            .subsystem(META_NETWORK_SUBSYSTEM)
    )
    .expect("meta metric cannot be created");

    pub static ref META_SERVICE_FAILED: IntCounter = IntCounter::with_opts(
        Opts::new("req_failed", "Total number of fail request from meta grpc client.")
            .namespace(META_NAMESPACE)
            .subsystem(META_NETWORK_SUBSYSTEM)
    )
    .expect("meta metric cannot be created");
}

pub fn init_meta_metrics_recorder() {
    static START: Once = Once::new();
    START.call_once(init_meta_recorder)
}

/// Init meta metrics recorder.
fn init_meta_recorder() {
    REGISTRY
        .register(Box::new(CURRENT_LEADER.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(IS_LEADER.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(NODE_IS_HEALTH.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(LEADER_CHANGES.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(APPLYING_SNAPSHOT.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(PROPOSALS_APPLIED.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(PROPOSALS_PENDING.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(PROPOSALS_FAILED.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(READ_FAILED.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(WATCHERS.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(ACTIVE_PEERS.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(CONNECT_TO_PEER_FAIL.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(SENT_BYTES.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(RECV_BYTES.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(SENT_FAILURES.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(SNAPSHOT_SEND_SUCCESS.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(SNAPSHOT_SEND_FAILURES.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(SNAPSHOT_SEND_INFLIGHTS.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(SNAPSHOT_SENT_SECONDS.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(SNAPSHOT_RECV_INFLIGHTS.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(SNAPSHOT_RECV_FAILURES.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(SNAPSHOT_RECV_SUCCESS.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(SNAPSHOT_RECV_SECONDS.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(META_SERVICE_SENT_BYTES.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(META_SERVICE_RECV_BYTES.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(META_REQUEST_INFLIGHTS.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(META_SERVICE_SUCCESS.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(META_SERVICE_FAILED.clone()))
        .expect("collector can be registered");
}

pub fn set_meta_metrics_current_leader(current_leader: NodeId) {
    CURRENT_LEADER.set(current_leader as i64);
}

pub fn set_meta_metrics_is_leader(is_leader: bool) {
    IS_LEADER.set(if is_leader { 1 } else { 0 });
}

pub fn set_meta_metrics_node_is_health(is_health: bool) {
    NODE_IS_HEALTH.set(if is_health { 1 } else { 0 });
}

pub fn get_meta_metrics_node_is_health() -> bool {
    NODE_IS_HEALTH.get() > 0
}

pub fn incr_meta_metrics_leader_change() {
    LEADER_CHANGES.inc();
}

pub fn incr_meta_metrics_applying_snapshot(cnt: i64) {
    APPLYING_SNAPSHOT.add(cnt);
}

pub fn set_meta_metrics_proposals_applied(proposals_applied: u64) {
    PROPOSALS_APPLIED.set(proposals_applied as f64);
}

pub fn incr_meta_metrics_proposals_pending(cnt: i64) {
    PROPOSALS_PENDING.add(cnt);
}

pub fn incr_meta_metrics_proposals_failed() {
    PROPOSALS_FAILED.inc();
}

pub fn incr_meta_metrics_read_failed() {
    READ_FAILED.inc();
}

pub fn incr_meta_metrics_watchers(cnt: i64) {
    WATCHERS.add(cnt);
}

pub fn incr_meta_metrics_active_peers(id: &NodeId, addr: &String, cnt: i64) {
    ACTIVE_PEERS
        .with_label_values(&[&id.to_string(), addr])
        .add(cnt as f64);
}

pub fn incr_meta_metrics_fail_connections_to_peer(id: &NodeId, addr: &String) {
    CONNECT_TO_PEER_FAIL
        .with_label_values(&[&id.to_string(), addr])
        .inc();
}

pub fn incr_meta_metrics_sent_bytes_to_peer(id: &NodeId, bytes: u64) {
    SENT_BYTES
        .with_label_values(&[&id.to_string()])
        .inc_by(bytes as f64);
}

pub fn incr_meta_metrics_recv_bytes_from_peer(addr: String, bytes: u64) {
    RECV_BYTES.with_label_values(&[&addr]).inc_by(bytes as f64);
}

pub fn incr_meta_metrics_meta_sent_bytes(bytes: u64) {
    META_SERVICE_SENT_BYTES.inc_by(bytes);
}

pub fn incr_meta_metrics_meta_recv_bytes(bytes: u64) {
    META_SERVICE_RECV_BYTES.inc_by(bytes);
}

pub fn incr_meta_metrics_sent_failure_to_peer(id: &NodeId) {
    SENT_FAILURES.with_label_values(&[&id.to_string()]).inc();
}

pub fn incr_meta_metrics_snapshot_send_inflights_to_peer(id: &NodeId, cnt: i64) {
    SNAPSHOT_SEND_INFLIGHTS
        .with_label_values(&[&id.to_string()])
        .add(cnt);
}

pub fn incr_meta_metrics_snapshot_send_success_to_peer(id: &NodeId) {
    SNAPSHOT_SEND_SUCCESS
        .with_label_values(&[&id.to_string()])
        .inc();
}

pub fn incr_meta_metrics_snapshot_send_failures_to_peer(id: &NodeId) {
    SNAPSHOT_SEND_FAILURES
        .with_label_values(&[&id.to_string()])
        .inc();
}

pub fn sample_meta_metrics_snapshot_sent(id: &NodeId, v: f64) {
    SNAPSHOT_SENT_SECONDS
        .with_label_values(&[&id.to_string()])
        .observe(v);
}

pub fn incr_meta_metrics_snapshot_recv_inflights_from_peer(addr: String, cnt: i64) {
    SNAPSHOT_SEND_INFLIGHTS.with_label_values(&[&addr]).add(cnt);
}

pub fn incr_meta_metrics_snapshot_recv_failure_from_peer(addr: String) {
    SNAPSHOT_RECV_FAILURES.with_label_values(&[&addr]).inc();
}

pub fn incr_meta_metrics_snapshot_recv_success_from_peer(addr: String) {
    SNAPSHOT_RECV_SUCCESS.with_label_values(&[&addr]).inc();
}

pub fn sample_meta_metrics_snapshot_recv(addr: String, v: f64) {
    SNAPSHOT_SENT_SECONDS.with_label_values(&[&addr]).observe(v);
}

pub fn add_meta_metrics_meta_request_inflights(cnt: i64) {
    META_REQUEST_INFLIGHTS.add(cnt);
}

pub fn incr_meta_metrics_meta_request_result(success: bool) {
    if success {
        META_SERVICE_SUCCESS.inc();
    } else {
        META_SERVICE_FAILED.inc();
    }
}

/// Encode metrics as prometheus format string
pub fn meta_metrics_to_prometheus_string() -> String {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&REGISTRY.gather(), &mut buffer) {
        eprintln!("could not encode custom metrics: {}", e);
    };
    let res = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("custom metrics could not be from_utf8'd: {}", e);
            String::default()
        }
    };

    res
}
