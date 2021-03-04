// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::admins::Router;
use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::error::FuseQueryResult;

pub struct AdminService {
    cfg: Config,
    cluster: ClusterRef,
}

impl AdminService {
    pub fn create(cfg: Config, cluster: ClusterRef) -> Self {
        AdminService { cfg, cluster }
    }

    pub async fn make_server(&self) -> FuseQueryResult<()> {
        let address = self.cfg.admin_api_address.parse::<std::net::SocketAddr>()?;
        let router = Router::create(self.cfg.clone(), self.cluster.clone());
        warp::serve(router.router()?).run(address).await;
        Ok(())
    }
}
