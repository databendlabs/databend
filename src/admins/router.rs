// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use warp::Filter;

use crate::clusters::ClusterRef;
use crate::configs::Config;
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
        let config_handler = super::v1::config::config_handler(self.cfg.clone())?;
        let hello_handler = super::v1::hello::hello_handler(self.cfg.clone())?;
        let cluster_nodes_handler =
            super::v1::cluster::cluster_nodes_handler(self.cluster.clone())?;
        let v1 = config_handler.or(hello_handler).or(cluster_nodes_handler);
        Ok(v1)
    }
}
