// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;
use std::fmt::Formatter;

use common_exception::ErrorCode;
use common_exception::Result;
use common_management::cluster::ClusterClientRef;
use warp::reject::{Reject};
use warp::{Filter, Reply, Rejection};

use crate::configs::Config;
use crate::sessions::{SessionManagerRef, SessionManager};
use futures::{Future, TryFuture};
use std::result::Result as StdResult;
use warp::reply::Response;
use crate::api::http::v1::cluster::action_register::RegisterAction;
use crate::api::http::v1::cluster::action_list::ListAction;

#[derive(Clone)]
pub struct ClusterExtra {
    pub cfg: Config,
    pub client: ClusterClientRef,
}

pub struct ClusterRouter {
    sessions: SessionManagerRef,
}

impl ClusterRouter {
    pub fn create(sessions: SessionManagerRef) -> Self {
        ClusterRouter { sessions }
    }

    // async fn add_node(sessions: &SessionManager, _: NodeInfo) -> StdResult<impl Reply, Rejection> {
    //     // sessions.try_get_cluster()?;
    //     Ok(warp::http::StatusCode::OK)
    // }

    // fn add_node<RouterFuture>(&self) -> RouterFuture
    //     where RouterFuture: TryFuture, RouterFuture::Ok: Reply, RouterFuture::Error: IsReject
    // {
    //     let sessions = self.sessions.clone();
    //
    //     async move {
    //         // TODO: 处理
    //     }
    //     // let conf = extra.cfg.clone();
    //     // let executor = conf.executor_from_config().unwrap();
    //     // extra
    //     //     .client
    //     //     .register(conf.cluster_namespace, &executor)
    //     //     .await
    //     //     .unwrap();
    //     // Ok(warp::http::StatusCode::OK)
    // }

    /// GET /v1/cluster/list
    fn cluster_list_node(&self) -> impl Filter<Extract=impl Reply, Error=Rejection> + Clone {
        let sessions = self.sessions.clone();
        warp::path!("v1" / "cluster" / "list")
            .and(warp::get())
            .map(move || ListAction::create(sessions.clone()))
    }

    /// POST /v1/cluster/register
    fn cluster_register_node(&self) -> impl Filter<Extract=impl Reply, Error=Rejection> + Clone {
        let sessions = self.sessions.clone();
        warp::path!("v1" / "cluster" / "register")
            .and(warp::post())
            .and(warp::body::content_length_limit(1024 * 16))
            .and(warp::body::json())
            .map(move |info| RegisterAction::create(info, sessions.clone()))
    }

    fn cluster_unregister_node(&self) -> impl Filter<Extract=impl Reply, Error=Rejection> + Clone {
        warp::path!("v1" / "cluster" / "unregister")
            .and(warp::post())
            .and_then(handlers::unregister_node)
    }

    pub fn build(&self) -> Result<impl Filter<Extract=impl Reply, Error=Rejection> + Clone> {
        Ok(self.cluster_list_node()
            .or(self.cluster_register_node())
            .or(self.cluster_unregister_node())
        )
    }
}

mod handlers {
    //
    // pub async fn list_node(
    //     extra: ClusterExtra,
    // ) -> Result<impl warp::Reply, std::convert::Infallible> {
    //     let results = extra
    //         .client
    //         .get_executors_by_namespace(extra.cfg.cluster_namespace)
    //         .await
    //         .unwrap();
    //     Ok(warp::reply::json(&results))
    // }

    pub async fn unregister_node() -> Result<impl warp::Reply, std::convert::Infallible> {
        // let conf = extra.cfg.clone();
        // let executor = conf.executor_from_config().unwrap();
        // extra
        //     .client
        //     .unregister(conf.cluster_namespace, &executor)
        //     .await
        //     .unwrap();
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
