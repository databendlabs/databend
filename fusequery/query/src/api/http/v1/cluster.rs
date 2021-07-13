// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;
use std::fmt::Formatter;

use common_exception::ErrorCode;
use common_management::cluster::ClusterClientRef;
use warp::reject::Reject;
use warp::Filter;

use crate::configs::Config;

#[derive(Clone)]
pub struct ClusterExtra {
    pub cfg: Config,
    pub client: ClusterClientRef,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct ClusterNodeRequest {}

pub fn cluster_handler(
    cfg: Config,
    client: ClusterClientRef,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let extra = ClusterExtra { cfg, client };
    cluster_list_node(extra.clone())
        .or(cluster_add_node(extra.clone()))
        .or(cluster_remove_node(extra))
}

/// GET /v1/cluster/list
fn cluster_list_node(
    extra: ClusterExtra,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("v1" / "cluster" / "list")
        .and(warp::get())
        .and(with_cluster_extra(extra))
        .and_then(handlers::list_node)
}

fn cluster_add_node(
    extra: ClusterExtra,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("v1" / "cluster" / "add")
        .and(warp::post())
        .and(json_body())
        .and(with_cluster_extra(extra))
        .and_then(handlers::add_node)
}

fn cluster_remove_node(
    extra: ClusterExtra,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("v1" / "cluster" / "remove")
        .and(warp::post())
        .and(json_body())
        .and(with_cluster_extra(extra))
        .and_then(handlers::remove_node)
}

fn with_cluster_extra(
    extra: ClusterExtra,
) -> impl Filter<Extract = (ClusterExtra,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || extra.clone())
}

fn json_body() -> impl Filter<Extract = (ClusterNodeRequest,), Error = warp::Rejection> + Clone {
    // When accepting a body, we want a JSON body
    // (and to reject huge payloads)...
    warp::body::content_length_limit(1024 * 16).and(warp::body::json())
}

mod handlers {

    use crate::api::http::v1::cluster::ClusterExtra;
    use crate::api::http::v1::cluster::ClusterNodeRequest;

    pub async fn list_node(
        extra: ClusterExtra,
    ) -> Result<impl warp::Reply, std::convert::Infallible> {
        let results = extra
            .client
            .get_executors_by_namespace(extra.cfg.cluster_namespace)
            .await
            .unwrap();
        Ok(warp::reply::json(&results))
    }

    pub async fn add_node(
        _req: ClusterNodeRequest,
        _extra: ClusterExtra,
    ) -> Result<impl warp::Reply, warp::Rejection> {
        Ok(warp::reply::json(&vec![""]))
    }

    pub async fn remove_node(
        _req: ClusterNodeRequest,
        _extra: ClusterExtra,
    ) -> Result<impl warp::Reply, std::convert::Infallible> {
        Ok(warp::reply::json(&vec![""]))
    }
}

struct NoBacktraceErrorCode(ErrorCode);

impl Debug for NoBacktraceErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Reject for NoBacktraceErrorCode {}
