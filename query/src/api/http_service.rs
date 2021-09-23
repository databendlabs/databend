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

use std::borrow::BorrowMut;
use std::fs::File;
use std::io::BufReader;
use std::io::{self};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use axum::handler::get;
use axum::handler::post;
use axum::AddExtensionLayer;
use axum::Router;
use axum_server;
use axum_server::tls::TlsLoader;
use common_exception::{Result, ErrorCode};
use common_runtime::tokio;
use common_runtime::tokio::task::JoinHandle;
use tokio_rustls::rustls::internal::pemfile::certs;
use tokio_rustls::rustls::internal::pemfile::pkcs8_private_keys;
use tokio_rustls::rustls::AllowAnyAuthenticatedClient;
use tokio_rustls::rustls::Certificate;
use tokio_rustls::rustls::NoClientAuth;
use tokio_rustls::rustls::PrivateKey;
use tokio_rustls::rustls::RootCertStore;
use tokio_rustls::rustls::ServerConfig;

use crate::clusters::{ClusterRef, ClusterDiscoveryRef};
use crate::configs::{Config, QueryConfig};
use crate::servers::Server;
use axum::routing::BoxRoute;
use crate::sessions::SessionManagerRef;
use axum_server::Handle;

pub struct HttpService {
    sessions: SessionManagerRef,
    join_handle: Option<JoinHandle<std::io::Result<()>>>,
    abort_handler: Handle,
}

impl HttpService {
    pub fn create(sessions: SessionManagerRef) -> Box<HttpService> {
        Box::new(HttpService {
            sessions,
            join_handle: None,
            abort_handler: axum_server::Handle::new(),
        })
    }

    fn build_tls(config: &Config) -> Result<ServerConfig> {
        let tls_key = Path::new(config.query.api_tls_server_key.as_str());
        let tls_cert = Path::new(config.query.api_tls_server_cert.as_str());

        let key = HttpService::load_keys(tls_key)?;
        let certs = HttpService::load_certs(tls_cert)?;

        let mut tls_config = ServerConfig::new(NoClientAuth::new());
        if let Err(cause) = tls_config.set_single_cert(certs, key[0].clone()) {
            return Err(ErrorCode::TLSConfigurationFailure(
                format!(
                    "Cannot build TLS config for http service, cause {}", cause
                )
            ))
        }

        HttpService::add_tls_pem_files(config, tls_config)
    }

    fn add_tls_pem_files(config: &Config, mut tls_config: ServerConfig) -> Result<ServerConfig> {
        let pem_path = &config.query.api_tls_server_root_ca_cert;
        if let Some(pem_path) = HttpService::load_ca(pem_path) {
            log::info!("Client Authentication for http service.");

            let pem_file = File::open(pem_path.as_str())?;
            let mut root_cert_store = RootCertStore::empty();

            if let Err(_) = root_cert_store.add_pem_file(BufReader::new(pem_file).borrow_mut()) {
                return Err(ErrorCode::TLSConfigurationFailure(
                    "Cannot add client ca in for http service"
                ));
            }

            let authenticated_client = AllowAnyAuthenticatedClient::new(root_cert_store);
            tls_config.set_client_certificate_verifier(authenticated_client);
        }

        Ok(tls_config)
    }

    fn load_certs(path: &Path) -> Result<Vec<Certificate>> {
        match certs(&mut BufReader::new(File::open(path)?)) {
            Ok(certs) => Ok(certs),
            Err(_) => Err(ErrorCode::TLSConfigurationFailure("invalid cert")),
        }
    }

    // currently only PKCS8 key supports for TLS setup
    fn load_keys(path: &Path) -> Result<Vec<PrivateKey>> {
        match pkcs8_private_keys(&mut BufReader::new(File::open(path)?)) {
            Ok(keys) => Ok(keys),
            Err(_) => Err(ErrorCode::TLSConfigurationFailure("invalid key")),
        }
    }

    // Client Auth(mTLS) CA certificate configuration
    fn load_ca(ca_path: &str) -> Option<String> {
        match Path::new(ca_path).exists() {
            false => None,
            true => Some(ca_path.to_string()),
        }
    }

    fn build_router(&self) -> Router<BoxRoute> {
        Router::new()
            .layer(AddExtensionLayer::new(self.sessions.clone()))
            .route("/v1/health", get(super::http::v1::health::health_handler))
            .route("/v1/config", get(super::http::v1::config::config_handler))
            .route("/v1/logs", get(super::http::v1::logs::logs_handler))
            // .route(
            //     "/v1/cluster/list",
            //     get(super::http::v1::cluster::cluster_list_handler),
            // )
            .route("/debug/home", get(super::http::debug::home::debug_home_handler))
            .route("/debug/pprof/profile", get(super::http::debug::pprof::debug_pprof_handler))
            .boxed()
    }

    async fn start_with_tls(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        log::info!("Http API TLS enabled");

        let loader = Self::tls_loader(self.sessions.get_conf());

        let server = axum_server::bind(listening.to_string())
            .loader(loader.await?)
            .handle(self.abort_handler.clone())
            .serve(self.build_router());

        self.join_handle = Some(tokio::spawn(server));
        self.abort_handler.listening().await;

        match self.abort_handler.listening_addrs() {
            None => Err(ErrorCode::CannotListenerPort("")),
            Some(addresses) if addresses.is_empty() => Err(ErrorCode::CannotListenerPort("")),
            Some(addresses) => {
                // 0.0.0.0, for multiple network interface, we may listen to multiple address
                let first_address = addresses[0].clone();
                for address in addresses {
                    if address.port() != first_address.port() {
                        return Err(ErrorCode::CannotListenerPort(""));
                    }
                }

                Ok(first_address)
            }
        }
    }

    async fn tls_loader(config: &Config) -> Result<TlsLoader> {
        let mut tls_loader = TlsLoader::new();
        tls_loader.config(Arc::new(Self::build_tls(config)?));

        match tls_loader.load().await {
            Ok(_) => Ok(tls_loader),
            Err(cause) => Err(ErrorCode::TLSConfigurationFailure(
                format!("Cannot load tls config, cause {}", cause)
            ))
        }
    }

    async fn start_without_tls(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        log::warn!("Http API TLS not set");

        let server = axum_server::bind(listening.to_string())
            .handle(self.abort_handler.clone())
            .serve(self.build_router());

        self.join_handle = Some(tokio::spawn(server));
        self.abort_handler.listening().await;

        match self.abort_handler.listening_addrs() {
            None => Err(ErrorCode::CannotListenerPort("")),
            Some(addresses) if addresses.is_empty() => Err(ErrorCode::CannotListenerPort("")),
            Some(addresses) => {
                // 0.0.0.0, for multiple network interface, we may listen to multiple address
                let first_address = addresses[0].clone();
                for address in addresses {
                    if address.port() != first_address.port() {
                        return Err(ErrorCode::CannotListenerPort(""));
                    }
                }

                Ok(first_address)
            }
        }
    }
}

#[async_trait::async_trait]
impl Server for HttpService {
    async fn shutdown(&mut self) {
        self.abort_handler.graceful_shutdown();

        if let Some(join_handle) = self.join_handle.take() {
            if let Err(error) = join_handle.await {
                log::error!(
                    "Unexpected error during shutdown Http API handler. cause {}",
                    error
                );
            }
        }
    }

    async fn start(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        let config = &self.sessions.get_conf().query;
        match config.api_tls_server_key.is_empty() || config.api_tls_server_cert.is_empty() {
            true => self.start_without_tls(listening).await,
            false => self.start_with_tls(listening).await,
        }
    }
}
