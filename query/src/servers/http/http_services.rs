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
use poem::put;
use poem::Endpoint;
use poem::EndpointExt;
use poem::Route;

use crate::common::service::HttpShutdownHandler;
use crate::configs::Config;
use crate::servers::http::v1::middleware::HTTPSessionMiddleware;
use crate::servers::http::v1::query_route;
use crate::servers::http::v1::statement_router;
use crate::servers::http::v1::streaming_load;
use crate::servers::Server;
use crate::sessions::SessionManager;

pub struct HttpHandler {
    session_manager: Arc<SessionManager>,
    shutdown_handler: HttpShutdownHandler,
}

impl HttpHandler {
    pub fn create(session_manager: Arc<SessionManager>) -> Box<dyn Server> {
        Box::new(HttpHandler {
            session_manager,
            shutdown_handler: HttpShutdownHandler::create("http handler".to_string()),
        })
    }

    // TODO(younsofun): add doc url after it`s ready
    pub fn usage(sock: SocketAddr) -> String {
        format!(
            r#" examples:
curl --request POST '{:?}/v1/statement/' --header 'Content-Type: text/plain' --data-raw 'SELECT avg(number) FROM numbers(100000000)'
curl --request POST '{:?}/v1/query/' --header 'Content-Type: application/json' --data-raw '{{"sql": "SELECT avg(number) FROM numbers(100000000)"}}'"#,
            sock, sock
        )
    }

    fn build_router(&self, sock: SocketAddr) -> impl Endpoint {
        Route::new()
            .at(
                "/",
                get(poem::endpoint::make_sync(move |_| Self::usage(sock))),
            )
            .nest("/v1/statement", statement_router())
            .nest("/v1/query", query_route())
            .at("/v1/streaming_load", put(streaming_load))
            .with(HTTPSessionMiddleware {
                session_manager: self.session_manager.clone(),
            })
            .boxed()
    }

    fn build_tls(config: &Config) -> Result<RustlsConfig> {
        let mut cfg = RustlsConfig::new()
            .cert(std::fs::read(
                &config.query.http_handler_tls_server_cert.as_str(),
            )?)
            .key(std::fs::read(
                &config.query.http_handler_tls_server_key.as_str(),
            )?);
        if Path::new(&config.query.http_handler_tls_server_root_ca_cert).exists() {
            cfg = cfg.client_auth_required(std::fs::read(
                &config.query.http_handler_tls_server_root_ca_cert.as_str(),
            )?);
        }
        Ok(cfg)
    }

    async fn start_with_tls(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        tracing::info!("Http Handler TLS enabled");

        let tls_config = Self::build_tls(self.session_manager.get_conf())?;
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
