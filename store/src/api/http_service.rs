// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::api::http::router::Router;
use crate::configs::Config;

pub struct HttpService {
    cfg: Config,
}

impl HttpService {
    pub fn create(cfg: Config) -> Self {
        HttpService { cfg }
    }

    pub async fn start(&mut self) -> Result<()> {
        let router = Router::create(self.cfg.clone());
        let server = warp::serve(router.router()?);

        let conf = self.cfg.clone();
        let tls_cert = conf.tls_server_cert;
        let tls_key = conf.tls_server_key;

        let address = conf.http_api_address.parse::<std::net::SocketAddr>()?;

        if !tls_cert.is_empty() && !tls_key.is_empty() {
            log::info!("Http API TLS enabled");
            server
                .tls()
                .cert_path(tls_cert)
                .key_path(tls_key)
                .run(address)
                .await;
        } else {
            log::warn!("Http API TLS not set");
            server.run(address).await;
        }
        Ok(())
    }
}
