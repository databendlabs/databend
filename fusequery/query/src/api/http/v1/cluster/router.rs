// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;
use std::fmt::Formatter;

use common_exception::ErrorCode;
use common_exception::Result;
use common_management::cluster::ClusterManagerRef;
use warp::reject::{Reject};
use warp::{Filter, Reply, Rejection};

use crate::configs::Config;
use crate::sessions::{SessionManagerRef, SessionManager};
use futures::{Future, TryFuture};
use std::result::Result as StdResult;
use warp::reply::Response;
use crate::api::http::v1::cluster::action_create::CreateAction;
use crate::api::http::v1::cluster::action_list::ListAction;
use crate::api::http::v1::cluster::action_remove::{RemoveAction, NodeIdentifier};
use crate::api::http::v1::cluster::action_get::GetAction;

pub struct ClusterRouter {
    sessions: SessionManagerRef,
}

/// Restful API for cluster management
impl ClusterRouter {
    pub fn create(sessions: SessionManagerRef) -> Self {
        ClusterRouter { sessions }
    }

    /// GET /v1/cluster/nodes
    fn cluster_list_nodes(&self) -> impl Filter<Extract=impl Reply, Error=Rejection> + Clone {
        let sessions = self.sessions.clone();
        warp::path!("v1" / "cluster" / "nodes")
            .and(warp::get())
            .map(move || ListAction::create(sessions.clone()))
    }

    /// GET /v1/cluster/node/${name}
    fn cluster_get_node(&self) -> impl Filter<Extract=impl Reply, Error=Rejection> + Clone {
        let sessions = self.sessions.clone();
        warp::path!("v1" / "cluster" / "node")
            .and(warp::path::param())
            .and(warp::get())
            .map(move |name| GetAction::create(name, sessions.clone()))
    }

    /// POST /v1/cluster/nodes
    fn cluster_create_node(&self) -> impl Filter<Extract=impl Reply, Error=Rejection> + Clone {
        let sessions = self.sessions.clone();
        warp::path!("v1" / "cluster" / "nodes")
            .and(warp::post())
            .and(warp::body::content_length_limit(1024 * 16))
            .and(warp::body::json())
            .map(move |info| CreateAction::create(info, sessions.clone()))
    }

    /// DELETE /v1/cluster/node/${name}
    fn cluster_remove_node(&self) -> impl Filter<Extract=impl Reply, Error=Rejection> + Clone {
        let sessions = self.sessions.clone();
        warp::path!("v1" / "cluster" / "node")
            .and(warp::path::param())
            .and(warp::delete())
            .map(move |name| RemoveAction::create(name, sessions.clone()))
    }

    pub fn build(&self) -> Result<impl Filter<Extract=impl Reply, Error=Rejection> + Clone> {
        Ok(self.cluster_list_nodes()
            .or(self.cluster_get_node())
            .or(self.cluster_create_node())
            .or(self.cluster_remove_node())
        )
    }
}
