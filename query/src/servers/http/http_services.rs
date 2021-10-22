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

use std::net::SocketAddr;

use axum::handler::get;
use axum::routing::BoxRoute;
use axum::AddExtensionLayer;
use axum::Router;
use common_base::tokio;
use common_exception::Result;

use crate::servers::http::v1::statement::statement_router;
use crate::servers::http_server::HttpServer;
use crate::servers::Server;
use crate::sessions::SessionManagerRef;

pub struct HttpHandler {
    session_manager: SessionManagerRef,
    http_server: HttpServer,
}

impl HttpHandler {
    pub fn create(session_manager: SessionManagerRef) -> Box<dyn Server> {
        Box::new(HttpHandler {
            session_manager,
            http_server: HttpServer::create("HttpHandler".to_string()),
        })
    }
    fn build_router(&self) -> Router<BoxRoute> {
        Router::new()
            .route("/", get(|| async { "This is http handler." }))
            .nest("/v1/statement", statement_router())
            .layer(AddExtensionLayer::new(self.session_manager.clone()))
            .boxed()
    }
    async fn start_without_tls(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        let server = axum_server::bind(listening.to_string())
            .handle(self.http_server.abort_handler.clone())
            .serve(self.build_router());

        self.http_server.start_server(tokio::spawn(server)).await
    }
}

#[async_trait::async_trait]
impl Server for HttpHandler {
    async fn shutdown(&mut self) {
        self.http_server.shutdown().await;
    }

    async fn start(&mut self, listening: SocketAddr) -> common_exception::Result<SocketAddr> {
        self.start_without_tls(listening).await
    }
}
