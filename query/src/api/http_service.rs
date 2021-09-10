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
use common_exception::Result;
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

// use crate::api::http::router::Router;
use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::servers::Server;

pub struct HttpService {
    cfg: Config,
    cluster: ClusterRef,
    join_handle: Option<JoinHandle<std::result::Result<(), std::io::Error>>>,
    abort_handler: axum_server::Handle,
    tls_config: Option<ServerConfig>,
}

// build axum router
macro_rules! build_router {
    ($cfg: expr, $cluster: expr) => {
        Router::new()
            .route("/v1/health", get(super::http::v1::health::health_handler))
            .route("/v1/config", get(super::http::v1::config::config_handler))
            .route("/v1/logs", get(super::http::v1::logs::logs_handler))
            .route(
                "/v1/cluster/add",
                post(super::http::v1::cluster::cluster_add_handler),
            )
            .route(
                "/v1/cluster/list",
                get(super::http::v1::cluster::cluster_list_handler),
            )
            .route(
                "/v1/cluster/remove",
                post(super::http::v1::cluster::cluster_remove_handler),
            )
            .route(
                "/debug/home",
                get(super::http::debug::home::debug_home_handler),
            )
            .route(
                "/debug/pprof/profile",
                get(super::http::debug::pprof::debug_pprof_handler),
            )
            .layer(AddExtensionLayer::new($cluster.clone()))
            .layer(AddExtensionLayer::new($cfg.clone()))
    };
}

impl HttpService {
    pub fn create(cfg: Config, cluster: ClusterRef) -> Box<Self> {
        let tls_config = HttpService::build_tls(cfg.clone());
        let handler = axum_server::Handle::new();
        Box::new(HttpService {
            cfg,
            cluster,
            join_handle: None,
            abort_handler: handler,
            tls_config,
        })
    }

    fn build_tls(cfg: Config) -> Option<ServerConfig> {
        if cfg.query.api_tls_server_key.is_empty() || cfg.query.api_tls_server_cert.is_empty() {
            return None;
        }
        let certs = HttpService::load_certs(Path::new(cfg.query.api_tls_server_cert.as_str()))
            .expect("cannot load TLS cert for http service");
        let key = HttpService::load_keys(Path::new(cfg.query.api_tls_server_key.as_str()))
            .expect("cannot load TLS key for http service")
            .remove(0);
        let ca = HttpService::load_ca(cfg.query.api_tls_server_root_ca_cert);
        let config = HttpService::build_tls_config(certs, key, ca);
        Some(config)
    }

    fn build_tls_config(
        certs: Vec<Certificate>,
        key: PrivateKey,
        ca_path: Option<String>,
    ) -> ServerConfig {
        let mut config = ServerConfig::new(NoClientAuth::new());
        config
            .set_single_cert(certs, key)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))
            .expect("cannot build TLS config for http service");
        match ca_path {
            Some(path) => {
                let mut root_castore = RootCertStore::empty();
                root_castore
                    .add_pem_file(
                        BufReader::new(
                            File::open(path.as_str()).expect("cannot read ca certificate"),
                        )
                        .borrow_mut(),
                    )
                    .expect("cannot add client ca in for http service");
                config.set_client_certificate_verifier(AllowAnyAuthenticatedClient::new(
                    root_castore,
                ));
            }
            None => {
                log::info!("No Client Authentication for http service");
            }
        }
        config
    }
    fn load_certs(path: &Path) -> io::Result<Vec<Certificate>> {
        certs(&mut BufReader::new(File::open(path)?))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid cert"))
    }

    // currently only PKCS8 key supports for TLS setup
    fn load_keys(path: &Path) -> io::Result<Vec<PrivateKey>> {
        pkcs8_private_keys(&mut BufReader::new(File::open(path)?))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid key"))
    }

    // Client Auth(mTLS) CA certificate configuration
    fn load_ca(ca_path: String) -> Option<String> {
        if Path::new(ca_path.as_str()).exists() {
            return Some(ca_path);
        }
        None
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
        let app = build_router!(self.cfg.clone(), self.cluster.clone());
        let handler = self.abort_handler.clone();
        match self.tls_config.clone() {
            None => {
                log::warn!("Http API TLS not set");

                self.join_handle = Some(tokio::spawn(
                    axum_server::bind(listening.to_string())
                        .handle(handler.clone())
                        .serve(app),
                ));
                Ok(listening)
            }
            Some(config) => {
                log::info!("Http API TLS enabled");
                let mut loader = TlsLoader::new();
                loader.config(Arc::new(config));
                loader.load().await.expect("cannot load tls config");
                self.join_handle = Some(tokio::spawn(
                    axum_server::bind_rustls(listening.to_string())
                        .handle(handler.clone())
                        .loader(loader)
                        .serve(app),
                ));
                Ok(listening)
            }
        }
    }
}
