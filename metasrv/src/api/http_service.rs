// Copyright 2021 Datafuse Labs.
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

use std::sync::Arc;

use common_base::tokio::sync::broadcast;
use common_base::HttpShutdownHandler;
use common_base::Stoppable;
use common_exception::Result;
use common_tracing::tracing;
use poem::get;
use poem::listener::RustlsConfig;
use poem::Endpoint;
use poem::EndpointExt;
use poem::Route;

use crate::configs::Config;
use crate::meta_service::MetaNode;

pub struct HttpService {
    cfg: Config,
    shutdown_handler: HttpShutdownHandler,
    meta_node: Arc<MetaNode>,
}

impl HttpService {
    pub fn create(cfg: Config, meta_node: Arc<MetaNode>) -> Box<Self> {
        Box::new(HttpService {
            cfg,
            shutdown_handler: HttpShutdownHandler::create("http api".to_string()),
            meta_node,
        })
    }

    fn build_router(&self) -> impl Endpoint {
        Route::new()
            .at("/v1/health", get(super::http::v1::health::health_handler))
            .at("/v1/config", get(super::http::v1::config::config_handler))
            .at(
                "/v1/cluster/nodes",
                get(super::http::v1::cluster_state::nodes_handler),
            )
            .at(
                "/v1/cluster/state",
                get(super::http::v1::cluster_state::state_handler),
            )
            .at(
                "/debug/home",
                get(super::http::debug::home::debug_home_handler),
            )
            .at(
                "/debug/pprof/profile",
                get(super::http::debug::pprof::debug_pprof_handler),
            )
            .data(self.meta_node.clone())
            .data(self.cfg.clone())
    }

    fn build_tls(config: &Config) -> Result<RustlsConfig> {
        let conf = config.clone();
        let tls_cert = conf.admin_tls_server_cert;
        let tls_key = conf.admin_tls_server_key;

        let cfg = RustlsConfig::new()
            .cert(std::fs::read(tls_cert.as_str())?)
            .key(std::fs::read(tls_key.as_str())?);
        Ok(cfg)
    }

    async fn start_with_tls(&mut self, listening: String) -> Result<()> {
        tracing::info!("Http API TLS enabled");

        let tls_config = Self::build_tls(&self.cfg.clone())?;
        self.shutdown_handler
            .start_service(listening, Some(tls_config), self.build_router())
            .await?;
        Ok(())
    }

    async fn start_without_tls(&mut self, listening: String) -> Result<()> {
        tracing::warn!("Http API TLS not set");

        self.shutdown_handler
            .start_service(listening, None, self.build_router())
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Stoppable for HttpService {
    async fn start(&mut self) -> Result<()> {
        let conf = self.cfg.clone();
        match conf.admin_tls_server_key.is_empty() || conf.admin_tls_server_cert.is_empty() {
            true => self.start_without_tls(conf.admin_api_address).await,
            false => self.start_with_tls(conf.admin_api_address).await,
        }
    }

    async fn stop(&mut self, force: Option<broadcast::Receiver<()>>) -> Result<()> {
        self.shutdown_handler.stop(force).await
    }
}
