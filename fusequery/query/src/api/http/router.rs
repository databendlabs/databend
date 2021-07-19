// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use warp::Filter;

use crate::clusters::ClusterRef;
use crate::configs::Config;

pub struct Router {
    cfg: Config,
    cluster: ClusterRef,
}

impl Router {
    pub fn create(cfg: Config, cluster: ClusterRef) -> Self {
        Router { cfg, cluster }
    }

    pub fn router(
        &self,
    ) -> Result<impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone> {
        let v1 = super::v1::hello::hello_handler(self.cfg.clone())
            .or(super::v1::config::config_handler(self.cfg.clone()))
            .or(super::v1::cluster::cluster_handler(self.cluster.clone()))
            .or(super::debug::backtrace::backtrace_handler(self.cfg.clone()))
            .or(super::debug::home::debug_handler(self.cfg.clone()));
        let routes = v1.with(warp::log("v1"));
        Ok(routes)
    }
}
