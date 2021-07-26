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
use crate::api::http::v1::cluster::action_unregister::{UnregisterAction, NodeIdentifier};

pub struct ClusterRouter {
    sessions: SessionManagerRef,
}

impl ClusterRouter {
    pub fn create(sessions: SessionManagerRef) -> Self {
        ClusterRouter { sessions }
    }

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
        let sessions = self.sessions.clone();
        // We use DELETE HTTP method, see: RFC 2616
        warp::path!("v1" / "cluster" / "unregister")
            .and(warp::delete())
            .and(warp::query::<NodeIdentifier>())
            .map(move |name| UnregisterAction::create(name, sessions.clone()))
    }

    pub fn build(&self) -> Result<impl Filter<Extract=impl Reply, Error=Rejection> + Clone> {
        Ok(self.cluster_list_node()
            .or(self.cluster_register_node())
            .or(self.cluster_unregister_node())
        )
    }
}
