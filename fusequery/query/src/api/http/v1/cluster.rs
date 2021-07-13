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
        .or(cluster_register_node(extra.clone()))
        .or(cluster_unregister_node(extra))
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

fn cluster_register_node(
    extra: ClusterExtra,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("v1" / "cluster" / "register")
        .and(warp::post())
        .and(with_cluster_extra(extra))
        .and_then(handlers::register_node)
}

fn cluster_unregister_node(
    extra: ClusterExtra,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("v1" / "cluster" / "unregister")
        .and(warp::post())
        .and(with_cluster_extra(extra))
        .and_then(handlers::unregister_node)
}

fn with_cluster_extra(
    extra: ClusterExtra,
) -> impl Filter<Extract = (ClusterExtra,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || extra.clone())
}

mod handlers {
    use crate::api::http::v1::cluster::ClusterExtra;

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

    pub async fn register_node(extra: ClusterExtra) -> Result<impl warp::Reply, warp::Rejection> {
        let conf = extra.cfg.clone();
        let executor = conf.executor_from_config().unwrap();
        extra
            .client
            .register(conf.cluster_namespace, &executor)
            .await
            .unwrap();
        Ok(warp::http::StatusCode::OK)
    }

    pub async fn unregister_node(
        extra: ClusterExtra,
    ) -> Result<impl warp::Reply, std::convert::Infallible> {
        let conf = extra.cfg.clone();
        let executor = conf.executor_from_config().unwrap();
        extra
            .client
            .unregister(conf.cluster_namespace, &executor)
            .await
            .unwrap();
        Ok(warp::http::StatusCode::OK)
    }
}

struct NoBacktraceErrorCode(ErrorCode);

impl Debug for NoBacktraceErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Reject for NoBacktraceErrorCode {}
