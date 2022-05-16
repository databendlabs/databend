// Copyright 2022 Datafuse Labs.
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

use poem::web::Data;
use poem::web::Json;
use serde_json;

use crate::meta_service::MetaNode;

/// GET /v1/metrics
///
/// return the metrics.
/// The response content is the same as `MetaMetrics` in metrics/meta_metrics.rs
#[poem::handler]
pub async fn metrics_handler(
    meta_node: Data<&Arc<MetaNode>>,
) -> poem::Result<Json<serde_json::Value>> {
    let metrics = meta_node.raft.metrics().borrow().clone();

    let has_leader = matches!(metrics.current_leader, Some(_));

    Ok(Json(serde_json::json!({
        "has_leader": has_leader,
    })))
}
