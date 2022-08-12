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

use common_exception::Result;
use common_http::health_handler;
use common_http::home::debug_home_handler;
#[cfg(feature = "memory-profiling")]
use common_http::jeprof::debug_jeprof_dump_handler;
use common_http::pprof::debug_pprof_handler;
use common_http::HttpShutdownHandler;
use poem::get;
use poem::listener::RustlsCertificate;
use poem::listener::RustlsConfig;
use poem::Endpoint;
use poem::Route;
use tracing::info;
use tracing::warn;

use crate::servers::Server;
use crate::Config;

pub struct HttpService {
    config: Config,
    shutdown_handler: HttpShutdownHandler,
}

impl HttpService {
    pub fn create(config: &Config) -> Result<Box<HttpService>> {
        Ok(Box::new(HttpService {
            config: config.clone(),
            shutdown_handler: HttpShutdownHandler::create("http api".to_string()),
        }))
    }

    fn build_router(&self) -> impl Endpoint {
        #[cfg_attr(not(feature = "memory-profiling"), allow(unused_mut))]
        let mut route = Route::new()
            .at("/v1/health", get(health_handler))
            .at("/v1/config", get(super::http::v1::config::config_handler))
            .at("/v1/logs", get(super::http::v1::logs::logs_handler))
            .at(
                "/v1/status",
                get(super::http::v1::instance_status::instance_status_handler),
            )
            .at(
                "/v1/tables",
                get(super::http::v1::tenant_tables::list_tables_handler),
            )
            .at(
                "/v1/cluster/list",
                get(super::http::v1::cluster::cluster_list_handler),
            )
            .at("/debug/home", get(debug_home_handler))
            .at("/debug/pprof/profile", get(debug_pprof_handler));

        if self.config.query.management_mode {
            route = route.at(
                "/v1/tenants/:tenant/tables",
                get(super::http::v1::tenant_tables::list_tenant_tables_handler),
            )
        }

        #[cfg(feature = "memory-profiling")]
        {
            route = route.at(
                // to follow the conversions of jepref, we arrange the path in
                // this way, so that jeprof could be invoked like:
                //   `jeprof ./target/debug/databend-query http://localhost:8080/debug/mem`
                // and jeprof will translate the above url into sth like:
                //    "http://localhost:8080/debug/mem/pprof/profile?seconds=30"
                "/debug/mem/pprof/profile",
                get(debug_jeprof_dump_handler),
            );
        };

        route
    }

    fn build_tls(config: &Config) -> Result<RustlsConfig> {
        let certificate = RustlsCertificate::new()
            .cert(std::fs::read(&config.query.api_tls_server_cert.as_str())?)
            .key(std::fs::read(&config.query.api_tls_server_key.as_str())?);
        let mut cfg = RustlsConfig::new().fallback(certificate);
        if Path::new(&config.query.api_tls_server_root_ca_cert).exists() {
            cfg = cfg.client_auth_required(std::fs::read(
                &config.query.api_tls_server_root_ca_cert.as_str(),
            )?);
        }
        Ok(cfg)
    }

    async fn start_with_tls(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        info!("Http API TLS enabled");

        let tls_config = Self::build_tls(&self.config)?;
        let addr = self
            .shutdown_handler
            .start_service(listening, Some(tls_config), self.build_router())
            .await?;
        Ok(addr)
    }

    async fn start_without_tls(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        warn!("Http API TLS not set");

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
        let config = &self.config.query;
        match config.api_tls_server_key.is_empty() || config.api_tls_server_cert.is_empty() {
            true => self.start_without_tls(listening).await,
            false => self.start_with_tls(listening).await,
        }
    }
}
