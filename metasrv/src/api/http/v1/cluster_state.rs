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

use poem::http::StatusCode;
use poem::web::Data;
use poem::web::IntoResponse;
use poem::web::Json;
use serde_json;

use crate::meta_service::MetaNode;

// GET /v1/cluster/nodes
// list all nodes in current databend-metasrv cluster
// request: None
// return: return a list of cluster node information
#[poem::handler]
pub async fn nodes_handler(meta_node: Data<&Arc<MetaNode>>) -> poem::Result<impl IntoResponse> {
    let nodes = meta_node.get_nodes().await.map_err(|e| {
        poem::Error::from_string(
            format!("failed to get nodes: {}", e),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
    })?;
    Ok(Json(nodes))
}

#[poem::handler]
pub async fn state_handler(
    meta_node: Data<&Arc<MetaNode>>,
) -> poem::Result<Json<serde_json::Value>> {
    let voters = meta_node.get_voters().await.map_err(|e| {
        poem::Error::from_string(
            format!("failed to get voters: {}", e),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
    })?;
    let non_voters = meta_node.get_non_voters().await.map_err(|e| {
        poem::Error::from_string(
            format!("failed to get non-voters: {}", e),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
    })?;
    Ok(Json(serde_json::json!({
        "voters": voters,
        "non_voters": non_voters,
    })))
}
