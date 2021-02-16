// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use crate::admins::Router;
use crate::clusters::ClusterRef;
use crate::configs::Config;

pub struct Admin {
    cfg: Config,
    cluster: ClusterRef,
}

impl Admin {
    pub fn create(cfg: Config, cluster: ClusterRef) -> Self {
        Admin { cfg, cluster }
    }

    pub async fn start(&self) {
        let router = Router::create(self.cfg.clone(), self.cluster.clone());
        warp::serve(router.router())
            .run(
                self.cfg
                    .admin_api_address
                    .parse::<std::net::SocketAddr>()
                    .expect("Failed to parse admin address"),
            )
            .await;
    }
}
