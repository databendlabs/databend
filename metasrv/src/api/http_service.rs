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

use common_base::tokio::sync::broadcast;
use common_base::HttpShutdownHandler;
use common_base::Stoppable;
use common_exception::Result;
use poem::get;
use poem::listener::RustlsConfig;
use poem::Endpoint;
use poem::EndpointExt;
use poem::Route;

use crate::configs::Config;

pub struct HttpService {
    cfg: Config,
    shutdown_handler: HttpShutdownHandler,
}

impl HttpService {
    pub fn create(cfg: Config) -> Box<Self> {
        Box::new(HttpService {
            cfg,
            shutdown_handler: HttpShutdownHandler::create("http api".to_string()),
        })
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
        log::info!("Http API TLS enabled");

        let tls_config = Self::build_tls(&self.cfg.clone())?;
        self.shutdown_handler
            .start_service(listening, Some(tls_config), self.build_router())
            .await?;
        Ok(())
    }

    async fn start_without_tls(&mut self, listening: String) -> Result<()> {
        log::warn!("Http API TLS not set");

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
        self.shutdown_handler.shutdown(true).await;
        if let Some(mut force) = force {
            log::info!("waiting for force");
            let _ = force
                .recv()
                .await
                .expect("Failed to recv the shutdown signal");
            log::info!("shutdown the service force");
            self.shutdown_handler.shutdown(false).await;
        }
        Ok(())
    }
}
