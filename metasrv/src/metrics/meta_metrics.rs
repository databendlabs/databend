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

use common_metrics::Counter;
use common_metrics::Gauge;
use common_metrics::MetricOption;
use once_cell::sync::OnceCell;

pub const META_NAMESPACE: &str = "metasrv";
pub const SERVER_SUBSYSTEM: &str = "server";

pub struct MetaMetrics {
    has_leader: Option<Gauge>,
    is_leader: Option<Gauge>,
    leader_changes: Option<Counter>,
    applying_snapshot: Option<Gauge>,
    proposals_applied: Option<Gauge>,
    proposals_pending: Option<Gauge>,
    proposals_failed: Option<Gauge>,
    read_failed: Option<Gauge>,
}

static INSTANCE: OnceCell<MetaMetrics> = OnceCell::new();

impl Default for MetaMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl MetaMetrics {
    pub fn new() -> MetaMetrics {
        MetaMetrics {
            has_leader: None,
            is_leader: None,
            leader_changes: None,
            applying_snapshot: None,
            proposals_applied: None,
            proposals_pending: None,
            proposals_failed: None,
            read_failed: None,
        }
    }

    pub fn instance() -> &'static MetaMetrics {
        INSTANCE.get().expect("metametrics is not initialized")
    }

    /// Init all meta metrics.
    pub fn register(self: &mut MetaMetrics) {
        self.has_leader = Some(common_metrics::register_gauge(MetricOption::new(
            "has_leader".to_string(),
            META_NAMESPACE.to_string(),
            SERVER_SUBSYSTEM.to_string(),
            "Whether or not a leader exists.".to_string(),
        )));

        self.is_leader = Some(common_metrics::register_gauge(MetricOption::new(
            "is_leader".to_string(),
            META_NAMESPACE.to_string(),
            SERVER_SUBSYSTEM.to_string(),
            "Whether or not this node is current leader.".to_string(),
        )));

        self.leader_changes = Some(common_metrics::register_counter(MetricOption::new(
            "leader_changes_seen_total".to_string(),
            META_NAMESPACE.to_string(),
            SERVER_SUBSYSTEM.to_string(),
            "Number of leader changes seen.".to_string(),
        )));

        self.applying_snapshot = Some(common_metrics::register_gauge(MetricOption::new(
            "applying_snapshot".to_string(),
            META_NAMESPACE.to_string(),
            SERVER_SUBSYSTEM.to_string(),
            "Whether or not applying snapshot.".to_string(),
        )));

        self.proposals_applied = Some(common_metrics::register_gauge(MetricOption::new(
            "proposals_applied".to_string(),
            META_NAMESPACE.to_string(),
            SERVER_SUBSYSTEM.to_string(),
            "Total number of consensus proposals applied.".to_string(),
        )));

        self.proposals_pending = Some(common_metrics::register_gauge(MetricOption::new(
            "proposals_pending".to_string(),
            META_NAMESPACE.to_string(),
            SERVER_SUBSYSTEM.to_string(),
            "Current number of pending proposals.".to_string(),
        )));

        self.proposals_failed = Some(common_metrics::register_gauge(MetricOption::new(
            "proposals_failed".to_string(),
            META_NAMESPACE.to_string(),
            SERVER_SUBSYSTEM.to_string(),
            "Current number of failed proposals seen.".to_string(),
        )));

        self.read_failed = Some(common_metrics::register_gauge(MetricOption::new(
            "read_failed".to_string(),
            META_NAMESPACE.to_string(),
            SERVER_SUBSYSTEM.to_string(),
            "Current number of failed read seen.".to_string(),
        )));
    }

    pub fn has_leader(has_leader: bool) {
        if let Some(instance) = INSTANCE.get() {
            let a = instance.has_leader.as_ref().unwrap();
            a.set(if has_leader { 1.0 } else { 0.0 });
        }
    }

    pub fn is_leader(is_leader: bool) {
        if let Some(instance) = INSTANCE.get() {
            let a = instance.is_leader.as_ref().unwrap();
            a.set(if is_leader { 1.0 } else { 0.0 });
        }
    }

    pub fn incr_leader_change() {
        if let Some(instance) = INSTANCE.get() {
            let a = instance.leader_changes.as_ref().unwrap();
            a.increment(1);
        }
    }

    pub fn incr_applying_snapshot(cnt: i32) {
        if let Some(instance) = INSTANCE.get() {
            let a = instance.applying_snapshot.as_ref().unwrap();
            if cnt > 0 {
                a.increment(cnt as f64);
            } else {
                a.decrement(cnt as f64);
            }
        }
    }

    pub fn set_proposals_applied(proposals_applied: u64) {
        if let Some(instance) = INSTANCE.get() {
            let a = instance.proposals_applied.as_ref().unwrap();
            a.set(proposals_applied as f64);
        }
    }

    pub fn incr_proposals_pending(cnt: i32) {
        if let Some(instance) = INSTANCE.get() {
            let a = instance.proposals_pending.as_ref().unwrap();
            if cnt > 0 {
                a.increment(cnt as f64);
            } else {
                a.decrement(cnt as f64);
            }
        }
    }

    pub fn incr_proposals_failed() {
        if let Some(instance) = INSTANCE.get() {
            let a = instance.proposals_failed.as_ref().unwrap();
            a.increment(1_f64);
        }
    }

    pub fn incr_read_failed() {
        if let Some(instance) = INSTANCE.get() {
            let a = instance.read_failed.as_ref().unwrap();
            a.increment(1_f64);
        }
    }
}

pub fn init_meta_metrics_recorder() {
    static START: Once = Once::new();
    START.call_once(init_meta_recorder)
}

/// Init meta metrics recorder.
fn init_meta_recorder() {
    let mut meta_metrics = MetaMetrics::new();
    meta_metrics.register();

    let _ = INSTANCE.set(meta_metrics);
}

pub fn set_meta_metrics_has_leader(has_leader: bool) {
    MetaMetrics::has_leader(has_leader);
}

pub fn set_meta_metrics_is_leader(is_leader: bool) {
    MetaMetrics::is_leader(is_leader);
}

pub fn incr_meta_metrics_leader_change() {
    MetaMetrics::incr_leader_change();
}

pub fn incr_meta_metrics_applying_snapshot(cnt: i32) {
    MetaMetrics::incr_applying_snapshot(cnt);
}

pub fn set_meta_metrics_proposals_applied(proposals_applied: u64) {
    MetaMetrics::set_proposals_applied(proposals_applied);
}

pub fn incr_meta_metrics_proposals_pending(cnt: i32) {
    MetaMetrics::incr_proposals_pending(cnt);
}

pub fn incr_meta_metrics_proposals_failed() {
    MetaMetrics::incr_proposals_failed();
}

pub fn incr_meta_metrics_read_failed() {
    MetaMetrics::incr_read_failed();
}
