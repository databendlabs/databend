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

use common_exception::Result;
use poem::get;
use poem::listener::Listener;
use poem::listener::TcpListener;
use poem::listener::TlsConfig;
use poem::Endpoint;
use poem::EndpointExt;
use poem::Route;

use crate::configs::Config;

pub struct HttpService {
    cfg: Config,
}

impl HttpService {
    pub fn create(cfg: Config) -> Box<Self> {
        Box::new(HttpService { cfg })
    }

    fn build_router(&self) -> impl Endpoint {
        Route::new()
            .at("/v1/health", get(super::http::v1::health::health_handler))
            .at("/v1/config", get(super::http::v1::config::config_handler))
            .at(
                "/debug/home",
                get(super::http::debug::home::debug_home_handler),
            )
            .at(
                "/debug/pprof/profile",
                get(super::http::debug::pprof::debug_pprof_handler),
            )
            .data(self.cfg.clone())
    }

    pub async fn start(&mut self) -> Result<()> {
        let app = self.build_router();

        let conf = self.cfg.clone();
        let tls_cert = conf.admin_tls_server_cert;
        let tls_key = conf.admin_tls_server_key;

        let address = conf.admin_api_address;

        if !tls_cert.is_empty() && !tls_key.is_empty() {
            log::info!("Http API TLS enabled");
            poem::Server::new(
                TcpListener::bind(address).tls(TlsConfig::new().key(tls_key).cert(tls_cert)),
            )
            .await?
            .run(app)
            .await?;
        } else {
            log::warn!("Http API TLS not set");
            poem::Server::new(TcpListener::bind(address))
                .await?
                .run(app)
                .await?;
        }
        Ok(())
    }
}
