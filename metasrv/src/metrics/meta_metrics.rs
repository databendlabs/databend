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

use common_metrics::Gauge;
use common_metrics::MetricOption;
use once_cell::sync::OnceCell;

pub const META_NAMESPACE: &str = "metasrv";
pub const SERVER_SUBSYSTEM: &str = "server";

pub struct MetaMetrics {
    has_leader: Option<Gauge>,
}

static INSTANCE: OnceCell<MetaMetrics> = OnceCell::new();

impl Default for MetaMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl MetaMetrics {
    pub fn new() -> MetaMetrics {
        MetaMetrics { has_leader: None }
    }

    pub fn instance() -> &'static MetaMetrics {
        INSTANCE.get().expect("metametrics is not initialized")
    }

    /// Init all meta metrics.
    pub fn register(self: &mut MetaMetrics) {
        // add has_leader metric
        let has_leader = MetricOption::new(
            "has_leader".to_string(),
            META_NAMESPACE.to_string(),
            SERVER_SUBSYSTEM.to_string(),
            "Whether or not a leader exists.".to_string(),
        );
        self.has_leader = Some(common_metrics::register_gauge(has_leader));
    }

    pub fn has_leader(has_leader: bool) {
        if let Some(instance) = INSTANCE.get() {
            let a = instance.has_leader.as_ref().unwrap();
            a.set(if has_leader { 1.0 } else { 0.0 });
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
