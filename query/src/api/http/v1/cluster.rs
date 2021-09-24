// Copyright 2020 Datafuse Labs.
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

use std::convert::Infallible;
use std::fmt::Debug;

use axum::body::Bytes;
use axum::body::Full;
use axum::extract::Extension;
use axum::extract::Json;
use axum::http::Response;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Html};
use serde_json::json;
use serde_json::Value;

use crate::clusters::{ClusterRef, ClusterDiscoveryRef};
use crate::sessions::SessionManagerRef;
use common_management::NodeInfo;
use common_exception::{Result, ErrorCode};
use std::sync::Arc;

pub struct ClusterTemplate {
    result: Result<String>,
}

impl IntoResponse for ClusterTemplate {
    type Body = Full<Bytes>;
    type BodyError = Infallible;

    fn into_response(self) -> Response<Self::Body> {
        match self.result {
            Ok(nodes) => Html(nodes).into_response(),
            Err(cause) => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Full::from(format!("Failed to fetch cluster nodes list. cause: {}", cause)))
                .unwrap(),
        }
    }
}

// GET /v1/cluster/list
// list all nodes in current databend-query cluster
// request: None
// cluster_state: the shared in memory state which store all nodes known to current node
// return: return a list of cluster node information
pub async fn cluster_list_handler(sessions: Extension<SessionManagerRef>) -> ClusterTemplate {
    let sessions = sessions.0;
    ClusterTemplate { result: list_nodes(sessions).await }
}

async fn list_nodes(sessions: SessionManagerRef) -> Result<String> {
    let watch_cluster_session = sessions.create_session("WatchCluster")?;
    let watch_cluster_context = watch_cluster_session.create_context().await?;

    let nodes_list = watch_cluster_context.get_cluster().get_nodes();
    Ok(serde_json::to_string(&nodes_list)?)
}
