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

use lazy_static::lazy_static;
use prometheus::Gauge;
use prometheus::IntCounter;
use prometheus::IntGauge;
use prometheus::Opts;
use prometheus::Registry;
use serde_json;

pub const META_NAMESPACE: &str = "metasrv";
pub const SERVER_SUBSYSTEM: &str = "server";

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
    pub static ref HAS_LEADER: IntGauge = IntGauge::with_opts(
        Opts::new("has_leader", "Whether or not a leader exists.")
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
    pub static ref LEADER_CHANGES: IntCounter = IntCounter::with_opts(
        Opts::new("leader_changes", "Number of leader changes seen.")
            .namespace(META_NAMESPACE)
            .subsystem(SERVER_SUBSYSTEM)
    )
    .expect("meta metric cannot be created");
    pub static ref APPLYING_SNAPSHOT: IntGauge = IntGauge::with_opts(
        Opts::new("applying_snapshot", "Whether or not applying snapshot.")
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
}

pub fn init_meta_metrics_recorder() {
    static START: Once = Once::new();
    START.call_once(init_meta_recorder)
}

/// Init meta metrics recorder.
fn init_meta_recorder() {
    REGISTRY
        .register(Box::new(HAS_LEADER.clone()))
        .expect("collector can be registered");

    REGISTRY
        .register(Box::new(IS_LEADER.clone()))
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
}

pub fn set_meta_metrics_has_leader(has_leader: bool) {
    HAS_LEADER.set(if has_leader { 1 } else { 0 });
}

pub fn set_meta_metrics_is_leader(is_leader: bool) {
    IS_LEADER.set(if is_leader { 1 } else { 0 });
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

/// Encode metrics as json value
pub fn meta_metrics_to_json() -> serde_json::Value {
    serde_json::json!({
        "has_leader": HAS_LEADER.get(),
        "is_leader": IS_LEADER.get(),
        "leader_changes": LEADER_CHANGES.get(),
        "applying_snapshot": APPLYING_SNAPSHOT.get(),
        "proposals_applied": PROPOSALS_APPLIED.get() as u64,
        "proposals_pending": PROPOSALS_PENDING.get(),
        "proposals_failed": PROPOSALS_FAILED.get(),
        "read_failed": READ_FAILED.get(),
        "watchers": WATCHERS.get(),
    })
}
