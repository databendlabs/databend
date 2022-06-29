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

pub static METRIC_CLUSTER_HEARTBEAT_COUNT: &str = "cluster.heartbeat.count";
pub static METRIC_CLUSTER_ERROR_COUNT: &str = "cluster.error.count";
pub static METRIC_CLUSTER_DISCOVERED_NODE_GAUGE: &str = "cluster.discovered_node.gauge";

pub static METRIC_LABEL_LOCAL_ID: &str = "local_id";
pub static METRIC_LABEL_FLIGHT_ADDRESS: &str = "flight_address";
pub static METRIC_LABEL_CLUSTER_ID: &str = "cluster_id";
pub static METRIC_LABEL_TENANT_ID: &str = "tenant_id";
