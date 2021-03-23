// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use fuse_query_configs::Config;

use crate::clusters::ClusterRef;
use crate::error::FuseQueryResult;
use crate::rpcs::http::router::Router;

pub struct HttpService {
    cfg: Config,
    cluster: ClusterRef,
}

impl HttpService {
    pub fn create(cfg: Config, cluster: ClusterRef) -> Self {
        HttpService { cfg, cluster }
    }

    pub async fn make_server(&self) -> FuseQueryResult<()> {
        let address = self.cfg.http_api_address.parse::<std::net::SocketAddr>()?;
        let router = Router::create(self.cfg.clone(), self.cluster.clone());
        warp::serve(router.router()?).run(address).await;
        Ok(())
    }
}
