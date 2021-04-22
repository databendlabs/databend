// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use anyhow::Result;

use crate::api::http::router::Router;
use crate::clusters::ClusterRef;
use crate::configs::Config;

pub struct HttpService {
    cfg: Config,
    cluster: ClusterRef
}

impl HttpService {
    pub fn create(cfg: Config, cluster: ClusterRef) -> Self {
        HttpService { cfg, cluster }
    }

    pub async fn make_server(&self) -> Result<()> {
        let address = self.cfg.http_api_address.parse::<std::net::SocketAddr>()?;
        let router = Router::create(self.cfg.clone(), self.cluster.clone());
        warp::serve(router.router()?).run(address).await;
        Ok(())
    }
}
