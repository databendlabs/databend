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

use std::sync::Arc;

use common_exception::Result;
use poem::http::StatusCode;
use poem::web::Data;
use poem::web::Html;
use poem::web::IntoResponse;
use poem::Response;

use crate::meta_service::MetaNode;

pub struct ClusterTemplate {
    result: Result<String>,
}

impl IntoResponse for ClusterTemplate {
    fn into_response(self) -> Response {
        match self.result {
            Ok(nodes) => Html(nodes).into_response(),
            Err(cause) => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(format!(
                    "Failed to fetch cluster nodes list. cause: {}",
                    cause
                )),
        }
    }
}

// GET /v1/cluster/nodes
// list all nodes in current databend-query cluster
// request: None
// cluster_state: the shared in memory state which store all nodes known to current node
// return: return a list of cluster node information
#[poem::handler]
pub async fn nodes_handler(meta_node: Data<&Arc<MetaNode>>) -> ClusterTemplate {
    let nodes = meta_node.get_nodes().await.unwrap();
    // let nodes = meta_node.get_nodes().await.expect("Failed to fetch cluster nodes list");
    let nodes_list = serde_json::to_string(&nodes).unwrap();
    ClusterTemplate {
        result: Ok(nodes_list),
    }
}
