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

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use common_exception::Result;
use common_tracing::tracing;
use poem::get;
use poem::listener::RustlsConfig;
use poem::Endpoint;
use poem::EndpointExt;
use poem::Route;

use crate::common::service::HttpShutdownHandler;
use crate::configs::Config;
use crate::servers::Server;
use crate::sessions::SessionManager;

pub struct HttpService {
    sessions: Arc<SessionManager>,
    shutdown_handler: HttpShutdownHandler,
}

impl HttpService {
    pub fn create(sessions: Arc<SessionManager>) -> Box<HttpService> {
        Box::new(HttpService {
            sessions,
            shutdown_handler: HttpShutdownHandler::create("http api".to_string()),
        })
    }

    fn build_router(&self) -> impl Endpoint {
        Route::new()
            .at("/v1/health", get(super::http::v1::health::health_handler))
            .at("/v1/config", get(super::http::v1::config::config_handler))
            .at("/v1/logs", get(super::http::v1::logs::logs_handler))
            .at(
                "/v1/cluster/list",
                get(super::http::v1::cluster::cluster_list_handler),
            )
            .at(
                "/debug/home",
                get(super::http::debug::home::debug_home_handler),
            )
            .at(
                "/debug/pprof/profile",
                get(super::http::debug::pprof::debug_pprof_handler),
            )
            .data(self.sessions.clone())
            .data(self.sessions.get_conf().clone())
    }

    fn build_tls(config: &Config) -> Result<RustlsConfig> {
        let mut cfg = RustlsConfig::new()
            .cert(std::fs::read(&config.query.api_tls_server_cert.as_str())?)
            .key(std::fs::read(&config.query.api_tls_server_key.as_str())?);
        if Path::new(&config.query.api_tls_server_root_ca_cert).exists() {
            cfg = cfg.client_auth_required(std::fs::read(
                &config.query.api_tls_server_root_ca_cert.as_str(),
            )?);
        }
        Ok(cfg)
    }

    async fn start_with_tls(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        tracing::info!("Http API TLS enabled");

        let tls_config = Self::build_tls(self.sessions.get_conf())?;
        let addr = self
            .shutdown_handler
            .start_service(listening, Some(tls_config), self.build_router())
            .await?;
        Ok(addr)
    }

    async fn start_without_tls(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        tracing::warn!("Http API TLS not set");

        let addr = self
            .shutdown_handler
            .start_service(listening, None, self.build_router())
            .await?;
        Ok(addr)
    }
}

#[async_trait::async_trait]
impl Server for HttpService {
    async fn shutdown(&mut self, graceful: bool) {
        self.shutdown_handler.shutdown(graceful).await;
    }

    async fn start(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        let config = &self.sessions.get_conf().query;
        match config.api_tls_server_key.is_empty() || config.api_tls_server_cert.is_empty() {
            true => self.start_without_tls(listening).await,
            false => self.start_with_tls(listening).await,
        }
    }
}
