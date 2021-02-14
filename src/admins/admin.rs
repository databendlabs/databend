// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::admins::Router;
use crate::configs::Config;

pub struct Admin {
    cfg: Config,
}

impl Admin {
    pub fn create(cfg: Config) -> Self {
        Admin { cfg }
    }

    pub async fn start(&self) {
        let router = Router::create(self.cfg.clone());
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
