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

use lazy_static::lazy_static;

use crate::register_counter_family;
use crate::register_gauge_family;
use crate::Counter;
use crate::Family;
use crate::Gauge;

lazy_static! {
    static ref CLUSTER_CLUSTER_HEARTBEAT_COUNT: Family<Vec<(&'static str, String)>, Counter> =
        register_counter_family("cluster_heartbeat_count");
    static ref CLUSTER_CLUSTER_ERROR_COUNT: Family<Vec<(&'static str, String)>, Counter> =
        register_counter_family("cluster_error_count");
    static ref CLUSTER_DISCOVERED_NODE_GAUGE: Family<Vec<(&'static str, String)>, Gauge> =
        register_gauge_family("cluster_discovered_node");
}

pub fn metric_incr_cluster_heartbeat_count(
    local_id: &str,
    flight_address: &str,
    cluster_id: &str,
    tenant_id: &str,
    result: &str,
) {
    let labels = &vec![
        ("local_id", String::from(local_id)),
        ("flight_address", String::from(flight_address)),
        ("cluster_id", cluster_id.to_string()),
        ("tenant_id", tenant_id.to_string()),
        ("result", result.to_string()),
    ];

    CLUSTER_CLUSTER_HEARTBEAT_COUNT.get_or_create(labels).inc();
}

pub fn metric_incr_cluster_error_count(
    local_id: &str,
    function: &str,
    cluster_id: &str,
    tenant_id: &str,
    flight_address: &str,
) {
    let labels = &vec![
        ("local_id", local_id.to_string()),
        ("function", function.to_string()),
        ("cluster_id", cluster_id.to_string()),
        ("tenant_id", tenant_id.to_string()),
        ("flight_address", flight_address.to_string()),
    ];

    CLUSTER_CLUSTER_ERROR_COUNT.get_or_create(labels).inc();
}

pub fn metrics_gauge_discovered_nodes(
    local_id: &str,
    cluster_id: &str,
    tenant_id: &str,
    flight_address: &str,
    val: f64,
) {
    let labels = &vec![
        ("local_id", local_id.to_string()),
        ("cluster_id", cluster_id.to_string()),
        ("tenant_id", tenant_id.to_string()),
        ("flight_address", flight_address.to_string()),
    ];

    CLUSTER_DISCOVERED_NODE_GAUGE
        .get_or_create(labels)
        .set(val as i64);
}
