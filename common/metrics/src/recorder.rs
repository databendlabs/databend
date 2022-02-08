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

use std::sync::Arc;
use std::sync::Once;

use common_infallible::RwLock;
use common_tracing::tracing;
use metrics::counter;
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_exporter_prometheus::PrometheusHandle;
use once_cell::sync::Lazy;

static PROMETHEUS_HANDLE: Lazy<Arc<RwLock<Option<PrometheusHandle>>>> =
    Lazy::new(|| Arc::new(RwLock::new(None)));

pub const LABEL_KEY_TENANT: &str = "tenant";
pub const LABEL_KEY_CLUSTER: &str = "cluster_name";

#[inline]
pub fn label_counter(name: &'static str, tenant_id: &str, cluster_id: &str) {
    label_counter_with_val(name, 1, tenant_id, cluster_id)
}

#[inline]
pub fn label_counter_with_val(name: &'static str, val: u64, tenant_id: &str, cluster_id: &str) {
    let labels = [
        (LABEL_KEY_TENANT, tenant_id.to_string()),
        (LABEL_KEY_CLUSTER, cluster_id.to_string()),
    ];
    counter!(name, val, &labels);
}

pub fn init_default_metrics_recorder() {
    static START: Once = Once::new();
    START.call_once(init_prometheus_recorder)
}

/// Init prometheus recorder.
fn init_prometheus_recorder() {
    let recorder = PrometheusBuilder::new()
        .build()
        .expect("failed to build Prometheus recorder");
    let mut h = PROMETHEUS_HANDLE.as_ref().write();
    *h = Some(recorder.0.handle());
    metrics::clear_recorder();
    match metrics::set_boxed_recorder(Box::new(recorder.0)) {
        Ok(_) => (),
        Err(err) => tracing::warn!("Install prometheus recorder failed, cause: {}", err),
    };
}

pub fn try_handle() -> Option<PrometheusHandle> {
    PROMETHEUS_HANDLE.as_ref().read().clone()
}
