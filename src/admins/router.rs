// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use crate::clusters::ClusterRef;
use crate::configs::Config;
use warp::Filter;

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
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        super::v1::config::config_handler(self.cfg.clone())
            .or(super::v1::hello::hello_handler(self.cfg.clone()))
            .or(super::v1::cluster::cluster_nodes_handler(
                self.cluster.clone(),
            ))
    }
}
