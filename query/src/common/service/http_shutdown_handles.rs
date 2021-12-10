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

use common_base::tokio::sync::oneshot;
use common_base::tokio::task::JoinHandle;
use common_exception::ErrorCode;
use common_exception::Result;
use common_tracing::tracing;
use futures::FutureExt;
use poem::listener::Acceptor;
use poem::listener::AcceptorExt;
use poem::listener::IntoTlsConfigStream;
use poem::listener::Listener;
use poem::listener::RustlsConfig;
use poem::listener::TcpListener;
use poem::Endpoint;

pub struct HttpShutdownHandler {
    service_name: String,
    join_handle: Option<JoinHandle<std::io::Result<()>>>,
    abort_handle: Option<oneshot::Sender<()>>,
}

impl HttpShutdownHandler {
    pub(crate) fn create(service_name: String) -> HttpShutdownHandler {
        HttpShutdownHandler {
            service_name,
            join_handle: None,
            abort_handle: None,
        }
    }

    pub async fn start_service(
        &mut self,
        listening: SocketAddr,
        tls_config: Option<RustlsConfig>,
        ep: impl Endpoint + 'static,
    ) -> Result<SocketAddr> {
        assert!(self.join_handle.is_none());
        assert!(self.abort_handle.is_none());

        let mut acceptor = TcpListener::bind(listening)
            .into_acceptor()
            .await
            .map_err(|err| ErrorCode::CannotListenerPort(err.to_string()))?
            .boxed();

        let addr = acceptor
            .local_addr()
            .pop()
            .and_then(|addr| addr.0.as_socket_addr().cloned())
            .expect("socket addr");

        if let Some(tls_config) = tls_config {
            acceptor = acceptor
                .rustls(tls_config.into_stream().map_err(|err| {
                    ErrorCode::TLSConfigurationFailure(format!(
                        "Cannot build TLS config for http service, cause {}",
                        err
                    ))
                })?)
                .boxed();
        }

        let (tx, rx) = oneshot::channel();
        let join_handle = common_base::tokio::spawn(
            poem::Server::new_with_acceptor(acceptor).run_with_graceful_shutdown(
                ep,
                rx.map(|_| ()),
                None,
            ),
        );
        self.join_handle = Some(join_handle);
        self.abort_handle = Some(tx);
        Ok(addr)
    }

    pub async fn shutdown(&mut self, graceful: bool) {
        if graceful {
            if let Some(abort_handle) = self.abort_handle.take() {
                let _ = abort_handle.send(());
            }
            if let Some(join_handle) = self.join_handle.take() {
                if let Err(error) = join_handle.await {
                    tracing::error!(
                        "Unexpected error during shutdown Http Server {}. cause {}",
                        self.service_name,
                        error
                    );
                }
            }
        } else if let Some(join_handle) = self.join_handle.take() {
            join_handle.abort();
        }
    }
}
