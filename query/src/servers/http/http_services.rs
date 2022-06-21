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
use poem::listener::RustlsCertificate;
use poem::listener::RustlsConfig;
use poem::middleware::NormalizePath;
use poem::middleware::TrailingSlash;
use poem::put;
use poem::Endpoint;
use poem::EndpointExt;
use poem::Route;

use super::v1::upload_to_stage;
use crate::common::service::HttpShutdownHandler;
use crate::servers::http::middleware::HTTPSessionMiddleware;
use crate::servers::http::v1::clickhouse_router;
use crate::servers::http::v1::query_route;
use crate::servers::http::v1::streaming_load;
use crate::servers::Server;
use crate::sessions::SessionManager;
use crate::Config;

#[derive(Copy, Clone)]
pub enum HttpHandlerKind {
    Query,
    Clickhouse,
}

impl HttpHandlerKind {
    pub fn usage(&self, sock: SocketAddr) -> String {
        match self {
            HttpHandlerKind::Query => {
                format!(
                    r#" curl -u root: --request POST '{:?}/v1/query/' --header 'Content-Type: application/json' --data-raw '{{"sql": "SELECT avg(number) FROM numbers(100000000)"}}'
"#,
                    sock,
                )
            }
            HttpHandlerKind::Clickhouse => {
                let json = r#"{"foo": "bar"}"#;
                format!(
                    r#" echo 'create table test(foo string)' | curl -u root: '{:?}' --data-binary  @-
echo '{}' | curl -u root: '{:?}/?query=INSERT%20INTO%20test%20FORMAT%20JSONEachRow' --data-binary @-"#,
                    sock, json, sock,
                )
            }
        }
    }
}

pub struct HttpHandler {
    session_manager: Arc<SessionManager>,
    shutdown_handler: HttpShutdownHandler,
    kind: HttpHandlerKind,
}

impl HttpHandler {
    pub fn create(session_manager: Arc<SessionManager>, kind: HttpHandlerKind) -> Box<dyn Server> {
        Box::new(HttpHandler {
            session_manager,
            shutdown_handler: HttpShutdownHandler::create("http handler".to_string()),
            kind,
        })
    }

    fn build_router(&self, sock: SocketAddr) -> impl Endpoint {
        let ep = match self.kind {
            HttpHandlerKind::Query => Route::new()
                .at(
                    "/",
                    get(poem::endpoint::make_sync(move |_| {
                        HttpHandlerKind::Query.usage(sock)
                    })),
                )
                .nest("/clickhouse", clickhouse_router())
                .nest("/v1/query", query_route())
                .at("/v1/streaming_load", put(streaming_load))
                .at("/v1/upload_to_stage", put(upload_to_stage)),
            HttpHandlerKind::Clickhouse => Route::new().nest("/", clickhouse_router()),
        };
        ep.with(HTTPSessionMiddleware {
            kind: self.kind,
            session_manager: self.session_manager.clone(),
        })
        .with(NormalizePath::new(TrailingSlash::Trim))
        .boxed()
    }

    fn build_tls(config: &Config) -> Result<RustlsConfig> {
        let certificate = RustlsCertificate::new()
            .cert(std::fs::read(
                &config.query.http_handler_tls_server_cert.as_str(),
            )?)
            .key(std::fs::read(
                &config.query.http_handler_tls_server_key.as_str(),
            )?);
        let mut cfg = RustlsConfig::new().fallback(certificate);
        if Path::new(&config.query.http_handler_tls_server_root_ca_cert).exists() {
            cfg = cfg.client_auth_required(std::fs::read(
                &config.query.http_handler_tls_server_root_ca_cert.as_str(),
            )?);
        }
        Ok(cfg)
    }

    async fn start_with_tls(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        tracing::info!("Http Handler TLS enabled");

        let config = self.session_manager.get_conf();
        let tls_config = Self::build_tls(&config)?;
        self.shutdown_handler
            .start_service(listening, Some(tls_config), self.build_router(listening))
            .await
    }

    async fn start_without_tls(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        self.shutdown_handler
            .start_service(listening, None, self.build_router(listening))
            .await
    }
}

#[async_trait::async_trait]
impl Server for HttpHandler {
    async fn shutdown(&mut self, graceful: bool) {
        self.shutdown_handler.shutdown(graceful).await;
    }

    async fn start(&mut self, listening: SocketAddr) -> common_exception::Result<SocketAddr> {
        let config = &self.session_manager.get_conf().query;
        match config.http_handler_tls_server_key.is_empty()
            || config.http_handler_tls_server_cert.is_empty()
        {
            true => self.start_without_tls(listening).await,
            false => self.start_with_tls(listening).await,
        }
    }
}
