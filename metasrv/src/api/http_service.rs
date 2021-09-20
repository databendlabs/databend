// Copyright 2020 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use axum::handler::get;
use axum::AddExtensionLayer;
use axum::Router;
use common_exception::Result;

use crate::configs::Config;

pub struct HttpService {
    cfg: Config,
}

// build axum router
macro_rules! build_router {
    ($cfg: expr) => {
        Router::new()
            .route("/v1/health", get(super::http::v1::health::health_handler))
            .route("/v1/config", get(super::http::v1::config::config_handler))
            .route(
                "/debug/home",
                get(super::http::debug::home::debug_home_handler),
            )
            .route(
                "/debug/pprof/profile",
                get(super::http::debug::pprof::debug_pprof_handler),
            )
            .layer(AddExtensionLayer::new($cfg.clone()))
    };
}

impl HttpService {
    pub fn create(cfg: Config) -> Box<Self> {
        Box::new(HttpService { cfg })
    }

    pub async fn start(&mut self) -> Result<()> {
        let app = build_router!(self.cfg.clone());

        let conf = self.cfg.clone();
        let tls_cert = conf.admin_tls_server_cert;
        let tls_key = conf.admin_tls_server_key;

        let address = conf.admin_api_address;

        if !tls_cert.is_empty() && !tls_key.is_empty() {
            log::info!("Http API TLS enabled");
            axum_server::bind_rustls(address)
                .private_key_file(tls_key)
                .certificate_file(tls_cert)
                .serve(app)
                .await?;
        } else {
            log::warn!("Http API TLS not set");
            axum_server::bind(address).serve(app).await?;
        }
        Ok(())
    }
}
