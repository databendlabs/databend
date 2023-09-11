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

use common_metrics::register_counter_family;
use common_metrics::register_gauge_family;
use common_metrics::Counter;
use common_metrics::Gauge;
use common_metrics::Family;
use common_metrics::label_counter_with_val_and_labels;
use metrics::gauge;
use lazy_static::lazy_static;

pub static METRIC_CLUSTER_HEARTBEAT_COUNT: &str = "cluster.heartbeat.count";
pub static METRIC_CLUSTER_ERROR_COUNT: &str = "cluster.error.count";
pub static METRIC_CLUSTER_DISCOVERED_NODE_GAUGE: &str = "cluster.discovered_node.gauge";

lazy_static! {
    static ref CLUSTER_CLUSTER_HEARTBEAT_COUNT: Family<Vec<(&'static str, String)>, Counter> =
        register_counter_family("cluster_heartbeat_count");
    static ref CLUSTER_CLUSTER_ERROR_COUNT: Family<Vec<(&'static str, String)>, Counter> =
        register_counter_family("cluster_error_count");
    static ref CLUSTER_DISCOVERED_NODE_GAUGE: Family<Vec<(&'static str, String)>, Gauge> = register_gauge_family("cluster_discovered_node");
}

pub(crate) fn metric_incr_cluster_heartbeat_count(
    local_id: &str,
    flight_address: &str,
    cluster_id: &str,
    tenant_id: &str,
    result: &str,
) {
    let labels = &vec![
    (
        "local_id",
        String::from(local_id),
    ),
    (
        "flight_address",
        String::from(flight_address),
    ),
    ("cluster_id", cluster_id.to_string()),
    ("tenant_id", tenant_id.to_string()),
    (
        "result", result.to_string(),
    )];

    label_counter_with_val_and_labels(
        METRIC_CLUSTER_HEARTBEAT_COUNT,
        labels,
        1,
    );
    CLUSTER_CLUSTER_HEARTBEAT_COUNT.get_or_create(labels).inc();
}

pub(crate) fn metric_incr_cluster_error_count(
    local_id: &str,
    function: &str,
    cluster_id: &str,
    tenant_id: &str,
    flight_address: &str,
) {
    let labels = &vec![
    (
        "local_id",
        local_id.to_string(),
    ),
    (
        "function",
        function.to_string(),
    ),
    ("cluster_id", cluster_id.to_string()),
    ("tenant_id", tenant_id.to_string()),
    (
        "flight_address", flight_address.to_string(),
    )];

    label_counter_with_val_and_labels(
        METRIC_CLUSTER_ERROR_COUNT,
        labels,
        1
    );

    CLUSTER_CLUSTER_ERROR_COUNT.get_or_create(labels).inc();
}

pub(crate) fn metrics_gauge_discovered_nodes(local_id: &str, cluster_id: &str, tenant_id: &str, flight_address: &str, val: f64) {
    let labels = &vec![
    (
        "local_id",
        local_id.to_string(),
    ),
    ("cluster_id", cluster_id.to_string()),
    ("tenant_id", tenant_id.to_string()),
    (
        "flight_address", flight_address.to_string(),
    )];

    gauge!(METRIC_CLUSTER_DISCOVERED_NODE_GAUGE, val, labels);
    CLUSTER_DISCOVERED_NODE_GAUGE.get_or_create(labels).set(val);
}