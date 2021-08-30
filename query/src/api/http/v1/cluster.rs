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
use std::sync::Arc;

use axum::body::Bytes;
use axum::body::Full;
use axum::extract::Extension;
use axum::extract::Json;
use axum::handler::post;
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
    ParseError,
    #[allow(dead_code)]
    AddError,
    #[allow(dead_code)]
    RemoveError,
    #[allow(dead_code)]
    ListError,
}

// error handling for cluster create http calls
// https://github.com/tokio-rs/axum/blob/main/examples/error-handling-and-dependency-injection/src/main.rs
impl IntoResponse for ClusterError {
    type Body = Full<Bytes>;
    type BodyError = Infallible;

    fn into_response(self) -> Response<Self::Body> {
        let (status, error_message) = match self {
            ClusterError::ParseError => (StatusCode::EXPECTATION_FAILED, "cannot parse json"),
            ClusterError::AddError => (
                StatusCode::SERVICE_UNAVAILABLE,
                "cannot add node to current cluster, please retry",
            ),
            ClusterError::RemoveError => (
                StatusCode::SERVICE_UNAVAILABLE,
                "cannot delete node in current cluster, please retry",
            ),
            ClusterError::ListError => (
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
// POST /v1/cluster/list
// create node depends on json context in http body
// request: the request body contains node message(name, ip address, priority)
// cluster_state: the shared in memory state which store all nodes known to current node
// return: return node information when add success
pub async fn cluster_add_handler(
    request: Json<ClusterNodeRequest>,
    cluster_state: Extension<ClusterRef>,
) -> Result<Json<Value>, ClusterError> {
    let req: ClusterNodeRequest = request.0;
    let cluster: ClusterRef = cluster_state.0;
    log::info!("Cluster add node: {:?}", req);
    return match cluster
        .add_node(&req.name.clone(), req.priority, &req.address)
        .await
    {
        Ok(_) => match cluster.get_node_by_name(req.clone().name) {
            Ok(node) => {
                log::info!("Successfully added node: {:?}", req);
                Ok(Json(json!(node)))
            }
            Err(_) => {
                log::error!("Cannot find {:?} in current cluster configuration", req);
                Err(ClusterError::AddError)
            }
        },
        Err(_) => {
            log::error!("Cannot add {:?} in current cluster", req);
            Err(ClusterError::AddError)
        }
    };
}

// GET /v1/cluster/list
// list all nodes in current datafuse-query cluster
// request: None
// cluster_state: the shared in memory state which store all nodes known to current node
// return: return a list of cluster node information
pub async fn cluster_list_handler(
    cluster_state: Extension<ClusterRef>,
) -> Result<Json<Value>, ClusterError> {
    let cluster: ClusterRef = cluster_state.0;
    return match cluster.get_nodes() {
        Ok(nodes) => {
            log::info!("Successfully listed nodes ");
            Ok(Json(json!(nodes)))
        }
        Err(_) => {
            log::error!("Unable to list nodes ");
            Err(ClusterError::ListError)
        }
    };
}

// POST /v1/cluster/remove
// remove a node based on name in current datafuse-query cluster
// request: Node to be deleted
// cluster_state: the shared in memory state which store all nodes known to current node
// return: return Ok status code when delete success
pub async fn cluster_remove_handler(
    request: Json<ClusterNodeRequest>,
    cluster_state: Extension<ClusterRef>,
) -> Result<String, ClusterError> {
    let req: ClusterNodeRequest = request.0;
    let cluster: ClusterRef = cluster_state.0;
    log::info!("Cluster remove node: {:?}", req);
    return match cluster.remove_node(req.clone().name) {
        Ok(_) => {
            log::error!("removed node {:?}", req.name);
            Ok(format!("removed node {:?}", req.name))
        }
        Err(_) => {
            log::error!("cannot remove node {:?}", req.name);
            Err(ClusterError::RemoveError)
        }
    };
}

// pub fn cluster_handler(
//     cluster: ClusterRef,
// ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
//     cluster_list_node(cluster.clone())
//         .or(cluster_add_node(cluster.clone()))
//         .or(cluster_remove_node(cluster))
// }
//
// /// GET /v1/cluster/list
// fn cluster_list_node(
//     cluster: ClusterRef,
// ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
//     warp::path!("v1" / "cluster" / "list")
//         .and(warp::get())
//         .and(with_cluster(cluster))
//         .and_then(handlers::list_node)
// }
//
// fn cluster_add_node(
//     cluster: ClusterRef,
// ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
//     warp::path!("v1" / "cluster" / "add")
//         .and(warp::post())
//         .and(json_body())
//         .and(with_cluster(cluster))
//         .and_then(handlers::add_node)
// }
//
// fn cluster_remove_node(
//     cluster: ClusterRef,
// ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
//     warp::path!("v1" / "cluster" / "remove")
//         .and(warp::post())
//         .and(json_body())
//         .and(with_cluster(cluster))
//         .and_then(handlers::remove_node)
// }
//
// fn with_cluster(
//     cluster: ClusterRef,
// ) -> impl Filter<Extract = (ClusterRef,), Error = std::convert::Infallible> + Clone {
//     warp::any().map(move || cluster.clone())
// }
//
// fn json_body() -> impl Filter<Extract = (ClusterNodeRequest,), Error = warp::Rejection> + Clone {
//     // When accepting a body, we want a JSON body
//     // (and to reject huge payloads)...
//     warp::body::content_length_limit(1024 * 16).and(warp::body::json())
// }
//
// mod handlers {
//     use log::info;
//
//     use crate::api::http::v1::cluster::ClusterNodeRequest;
//     use crate::api::http::v1::cluster::NoBacktraceErrorCode;
//     use crate::clusters::ClusterRef;
//
//     pub async fn list_node(
//         cluster: ClusterRef,
//     ) -> Result<impl warp::Reply, std::convert::Infallible> {
//         // TODO(BohuTANG): error handler
//         let nodes = cluster.get_nodes().unwrap();
//         Ok(warp::reply::json(&nodes))
//     }
//
//     pub async fn add_node(
//         req: ClusterNodeRequest,
//         cluster: ClusterRef,
//     ) -> Result<impl warp::Reply, warp::Rejection> {
//         info!("Cluster add node: {:?}", req);
//         match cluster
//             .add_node(&req.name, req.priority, &req.address)
//             .await
//         {
//             Ok(_) => Ok(warp::reply::with_status(
//                 "".to_string(),
//                 warp::http::StatusCode::OK,
//             )),
//             Err(error_codes) => Err(warp::reject::custom(NoBacktraceErrorCode(error_codes))),
//         }
//     }
//
//     pub async fn remove_node(
//         req: ClusterNodeRequest,
//         cluster: ClusterRef,
//     ) -> Result<impl warp::Reply, std::convert::Infallible> {
//         info!("Cluster remove node: {:?}", req);
//         // TODO(BohuTANG): error handler
//         cluster.remove_node(req.name).unwrap();
//         Ok(warp::http::StatusCode::OK)
//     }
// }
//
// struct NoBacktraceErrorCode(ErrorCode);
//
// impl Debug for NoBacktraceErrorCode {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         write!(f, "{}", self.0)
//     }
// }
//
// impl Reject for NoBacktraceErrorCode {}
