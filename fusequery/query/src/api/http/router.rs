// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use common_management::cluster::ClusterManager;
use common_management::cluster::ClusterManagerRef;
use warp::{Filter, Reply, Rejection};

use crate::api::http::v1::kv::KvStore;
use crate::api::http::v1::kv::KvStoreRef;
use crate::configs::Config;
use crate::sessions::SessionManagerRef;
use crate::api::http::v1::hello::HelloRouter;
use crate::api::http::v1::config::ConfigRouter;
use crate::api::http::debug::home::DebugRouter;
use crate::api::http::v1::ClusterRouter;

pub struct Router {
    hello_apis: HelloRouter,
    debug_apis: DebugRouter,
    config_apis: ConfigRouter,
    cluster_apis: ClusterRouter,
}

impl Router {
    pub fn create(sessions: SessionManagerRef) -> Self {
        Router {
            hello_apis: HelloRouter::create(sessions.clone()),
            debug_apis: DebugRouter::create(sessions.clone()),
            config_apis: ConfigRouter::create(sessions.clone()),
            cluster_apis: ClusterRouter::create(sessions.clone()),
        }
    }

    pub fn build(&self) -> Result<impl Filter<Extract=impl Reply, Error=Rejection> + Clone> {
        // .or(super::v1::kv::kv_handler(self.kv.clone()))

        Ok(self.hello_apis.build()?
            .or(self.debug_apis.build()?)
            .or(self.config_apis.build()?)
            .or(self.cluster_apis.build()?)
            .with(warp::log("v1"))
        )
    }
}
