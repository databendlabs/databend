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
use std::sync::Arc;

use common_exception::Result;
use poem::get;
use poem::Endpoint;
use poem::EndpointExt;
use poem::Route;

use crate::common::service::HttpShutdownHandler;
use crate::servers::http::v1::query_route;
use crate::servers::http::v1::statement_router;
use crate::servers::Server;
use crate::sessions::SessionManager;

// TODO(younsofun): add doc url after it`s ready
pub const HTTP_HANDLER_USAGE: &str = r#" examples:
curl --request POST '127.0.0.1:8001/v1/statement/' --header 'Content-Type: text/plain' --data-raw 'SELECT avg(number) FROM numbers(100000000)'
curl --request POST '127.0.0.1:8001/v1/query/' --header 'Content-Type: application/json' --data-raw '{"sql": "SELECT avg(number) FROM numbers(100000000)"}'"#;

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
    fn build_router(&self) -> impl Endpoint {
        Route::new()
            .at("/", get(poem::endpoint::make_sync(|_| HTTP_HANDLER_USAGE)))
            .nest("/v1/statement", statement_router())
            .nest("/v1/query", query_route())
            .data(self.session_manager.clone())
            .boxed()
    }
    async fn start_without_tls(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        let addr = self
            .shutdown_handler
            .start_service(listening, None, self.build_router())
            .await?;
        Ok(addr)
    }
}

#[async_trait::async_trait]
impl Server for HttpHandler {
    async fn shutdown(&mut self, graceful: bool) {
        self.shutdown_handler.shutdown(graceful).await;
    }

    async fn start(&mut self, listening: SocketAddr) -> common_exception::Result<SocketAddr> {
        self.start_without_tls(listening).await
    }
}
