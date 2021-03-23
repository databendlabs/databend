// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use fuse_query_configs::Config;
use warp::Filter;

use crate::clusters::ClusterRef;
use crate::error::FuseQueryResult;

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
    ) -> FuseQueryResult<impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone>
    {
        let v1 = super::v1::hello::hello_handler(self.cfg.clone())
            .or(super::v1::config::config_handler(self.cfg.clone()))
            .or(super::v1::cluster::cluster_handler(self.cluster.clone()));
        let routes = v1.with(warp::log("v1"));
        Ok(routes)
    }
}
