// Copyright 2021 Datafuse Labs
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
use std::time::Duration;

use databend_common_config::GlobalConfig;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_http::HttpError;
use databend_common_http::HttpShutdownHandler;
use databend_meta_types::anyerror::AnyError;
use http::StatusCode;
use log::info;
use poem::Endpoint;
use poem::EndpointExt;
use poem::IntoResponse;
use poem::Route;
use poem::get;
use poem::listener::OpensslTlsConfig;
use poem::middleware::CatchPanic;
use poem::middleware::CookieJarManager;
use poem::middleware::NormalizePath;
use poem::middleware::TrailingSlash;

use super::v1::HttpQueryContext;
use crate::servers::Server;
use crate::servers::http::middleware::EndpointKind;
use crate::servers::http::middleware::HTTPSessionMiddleware;
use crate::servers::http::middleware::PanicHandler;
use crate::servers::http::middleware::json_response;
use crate::servers::http::v1::clickhouse_router;
use crate::servers::http::v1::query_route;

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
                    r#" curl -u${{USER}} -p${{PASSWORD}}: --request POST '{:?}/v1/query/' --header 'Content-Type: application/json' --data-raw '{{"sql": "SELECT avg(number) FROM numbers(100000000)"}}'
"#,
                    sock,
                )
            }
            HttpHandlerKind::Clickhouse => {
                let json = r#"{"foo": "bar"}"#;
                format!(
                    r#" echo 'create table test(foo string)' | curl -u${{USER}} -p${{PASSWORD}}: '{:?}' --data-binary  @-
echo '{}' | curl -u${{USER}} -p${{PASSWORD}}: '{:?}/?query=INSERT%20INTO%20test%20FORMAT%20JSONEachRow' --data-binary @-"#,
                    sock, json, sock,
                )
            }
        }
    }
}

pub struct HttpHandler {
    shutdown_handler: HttpShutdownHandler,
    kind: HttpHandlerKind,
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn verify_handler(_ctx: &HttpQueryContext) -> poem::Result<impl IntoResponse> {
    Ok(StatusCode::OK)
}

impl HttpHandler {
    pub fn create(kind: HttpHandlerKind) -> Box<dyn Server> {
        Box::new(HttpHandler {
            kind,
            shutdown_handler: HttpShutdownHandler::create("http handler".to_string()),
        })
    }

    #[allow(clippy::let_with_type_underscore)]
    #[async_backtrace::framed]
    async fn build_router(&self, sock: SocketAddr) -> impl Endpoint + use<> {
        let ep_clickhouse = Route::new()
            .nest("/", clickhouse_router())
            .with(HTTPSessionMiddleware::create(
                self.kind,
                EndpointKind::Clickhouse,
            ))
            .with(CookieJarManager::new());

        let ep_usage = Route::new().at(
            "/",
            get(poem::endpoint::make_sync(move |_| {
                HttpHandlerKind::Query.usage(sock)
            })),
        );
        let ep_health = Route::new().at("/", get(poem::endpoint::make_sync(move |_| "ok")));

        let ep = match self.kind {
            HttpHandlerKind::Query => Route::new()
                .at("/", ep_usage)
                .nest("/health", ep_health)
                .nest("/v1", query_route())
                .nest("/clickhouse", ep_clickhouse),
            HttpHandlerKind::Clickhouse => Route::new()
                .nest("/", ep_clickhouse)
                .nest("/health", ep_health),
        };
        ep.with(NormalizePath::new(TrailingSlash::Trim))
            .with(CatchPanic::new().with_handler(PanicHandler::new()))
            .around(json_response)
            .boxed()
    }

    fn build_tls(config: &InnerConfig) -> Result<OpensslTlsConfig, std::io::Error> {
        let cfg = OpensslTlsConfig::new()
            .cert_from_file(config.query.common.http_handler_tls_server_cert.as_str())
            .key_from_file(config.query.common.http_handler_tls_server_key.as_str());

        // if Path::new(&config.query.http_handler_tls_server_root_ca_cert).exists() {
        //     cfg = cfg.client_auth_required(std::fs::read(
        //         config.query.http_handler_tls_server_root_ca_cert.as_str(),
        //     )?);
        // }
        Ok(cfg)
    }

    #[async_backtrace::framed]
    async fn start_with_tls(&mut self, listening: SocketAddr) -> Result<SocketAddr, HttpError> {
        info!("Http Handler TLS enabled");

        let config = GlobalConfig::instance();

        let tls_config = Self::build_tls(config.as_ref())
            .map_err(|e: std::io::Error| HttpError::TlsConfigError(AnyError::new(&e)))?;

        let router = self.build_router(listening).await;
        self.shutdown_handler
            .start_service(
                listening,
                Some(tls_config),
                router,
                Some(Duration::from_millis(1000)),
            )
            .await
    }

    #[async_backtrace::framed]
    async fn start_without_tls(&mut self, listening: SocketAddr) -> Result<SocketAddr, HttpError> {
        let router = self.build_router(listening).await;
        self.shutdown_handler
            .start_service(listening, None, router, Some(Duration::from_millis(1000)))
            .await
    }
}

#[async_trait::async_trait]
impl Server for HttpHandler {
    #[async_backtrace::framed]
    async fn shutdown(&mut self, graceful: bool) {
        self.shutdown_handler.shutdown(graceful).await;
    }

    #[async_backtrace::framed]
    async fn start(&mut self, listening: SocketAddr) -> Result<SocketAddr, ErrorCode> {
        let config = GlobalConfig::instance();

        let res = match config.query.common.http_handler_tls_server_key.is_empty()
            || config.query.common.http_handler_tls_server_cert.is_empty()
        {
            true => self.start_without_tls(listening).await,
            false => self.start_with_tls(listening).await,
        };

        res.map_err(|e: HttpError| match e {
            HttpError::BadAddressFormat(any_err) => {
                ErrorCode::BadAddressFormat(any_err.to_string())
            }
            le @ HttpError::ListenError { .. } => ErrorCode::CannotListenerPort(le.to_string()),
            HttpError::TlsConfigError(any_err) => {
                ErrorCode::TLSConfigurationFailure(any_err.to_string())
            }
        })
    }
}
