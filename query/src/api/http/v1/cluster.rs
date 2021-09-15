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
use axum::response::IntoResponse;
use serde_json::json;
use serde_json::Value;

use crate::clusters::ClusterRef;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct ClusterNodeRequest {
    pub name: String, // unique for each node
    // Priority is in [0, 10]
    // Larger value means higher
    // priority
    pub priority: u8,
    pub address: String,
}

#[derive(Debug)]
pub enum ClusterError {
    #[allow(dead_code)]
    Parse,
    #[allow(dead_code)]
    Add,
    #[allow(dead_code)]
    Remove,
    #[allow(dead_code)]
    List,
}

// error handling for cluster create http calls
// https://github.com/tokio-rs/axum/blob/main/examples/error-handling-and-dependency-injection/src/main.rs
impl IntoResponse for ClusterError {
    type Body = Full<Bytes>;
    type BodyError = Infallible;

    fn into_response(self) -> Response<Self::Body> {
        let (status, error_message) = match self {
            ClusterError::Parse => (StatusCode::EXPECTATION_FAILED, "cannot parse json"),
            ClusterError::Add => (
                StatusCode::SERVICE_UNAVAILABLE,
                "cannot add node to current cluster, please retry",
            ),
            ClusterError::Remove => (
                StatusCode::SERVICE_UNAVAILABLE,
                "cannot delete node in current cluster, please retry",
            ),
            ClusterError::List => (
                StatusCode::SERVICE_UNAVAILABLE,
                "cannot list nodes in current cluster, please retry",
            ),
        };

        let body = Json(json!({
            "cluster create error": error_message,
        }));

        (status, body).into_response()
    }
}
// // POST /v1/cluster/list
// // create node depends on json context in http body
// // request: the request body contains node message(name, ip address, priority)
// // cluster_state: the shared in memory state which store all nodes known to current node
// // return: return node information when add success
// pub async fn cluster_add_handler(
//     request: Json<ClusterNodeRequest>,
//     cluster_state: Extension<ClusterRef>,
// ) -> Result<Json<Value>, ClusterError> {
//     let req: ClusterNodeRequest = request.0;
//     let cluster: ClusterRef = cluster_state.0;
//     log::info!("Cluster add node: {:?}", req);
//     return match cluster
//         .add_node(&req.name.clone(), req.priority, &req.address)
//         .await
//     {
//         Ok(_) => match cluster.get_node_by_name(req.clone().name) {
//             Ok(node) => {
//                 log::info!("Successfully added node: {:?}", req);
//                 Ok(Json(json!(node)))
//             }
//             Err(_) => {
//                 log::error!("Cannot find {:?} in current cluster configuration", req);
//                 Err(ClusterError::Add)
//             }
//         },
//         Err(_) => {
//             log::error!("Cannot add {:?} in current cluster", req);
//             Err(ClusterError::Add)
//         }
//     };
// }

// GET /v1/cluster/list
// list all nodes in current datafuse-query cluster
// request: None
// cluster_state: the shared in memory state which store all nodes known to current node
// return: return a list of cluster node information
// pub async fn cluster_list_handler(
//     cluster_state: Extension<ClusterRef>,
// ) -> Result<Json<Value>, ClusterError> {
//     let cluster: ClusterRef = cluster_state.0;
//     return match cluster.get_nodes() {
//         Ok(nodes) => {
//             log::info!("Successfully listed nodes ");
//             Ok(Json(json!(nodes)))
//         }
//         Err(_) => {
//             log::error!("Unable to list nodes ");
//             Err(ClusterError::List)
//         }
//     };
// }

// // POST /v1/cluster/remove
// // remove a node based on name in current datafuse-query cluster
// // request: Node to be deleted
// // cluster_state: the shared in memory state which store all nodes known to current node
// // return: return Ok status code when delete success
// pub async fn cluster_remove_handler(
//     request: Json<ClusterNodeRequest>,
//     cluster_state: Extension<ClusterRef>,
// ) -> Result<String, ClusterError> {
//     let req: ClusterNodeRequest = request.0;
//     let cluster: ClusterRef = cluster_state.0;
//     log::info!("Cluster remove node: {:?}", req);
//     return match cluster.remove_node(req.clone().name) {
//         Ok(_) => {
//             log::error!("removed node {:?}", req.name);
//             Ok(format!("removed node {:?}", req.name))
//         }
//         Err(_) => {
//             log::error!("cannot remove node {:?}", req.name);
//             Err(ClusterError::Remove)
//         }
//     };
// }
