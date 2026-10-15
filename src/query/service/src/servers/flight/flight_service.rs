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

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use log::info;
use socket2::Domain;
use socket2::Protocol;
use socket2::Socket;
use socket2::Type;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tonic::transport::Identity;
use tonic::transport::Server;
use tonic::transport::ServerTlsConfig;
use tonic::transport::server::TcpIncoming;

use super::v1::DatabendQueryFlightService;
use crate::servers::Server as DatabendQueryServer;

/// Backlog requested for the flight listener.
///
/// `TcpIncoming::bind` goes through `std::net::TcpListener::bind`, which
/// hard-codes a backlog of 128. Every distributed query opens new flight
/// connections to every node, so bursts can overflow such a small accept
/// queue; the kernel then drops SYNs and clients back off for seconds.
/// 4096 matches the default `net.core.somaxconn` of Linux >= 5.4; the kernel
/// clamps it to that sysctl if it is lower.
const FLIGHT_LISTEN_BACKLOG: i32 = 4096;

fn bind_flight_listener(addr: SocketAddr) -> std::io::Result<TcpIncoming> {
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    // Keep the SO_REUSEADDR behavior of `std::net::TcpListener::bind`.
    #[cfg(not(windows))]
    socket.set_reuse_address(true)?;
    socket.bind(&addr.into())?;
    socket.listen(FLIGHT_LISTEN_BACKLOG)?;
    socket.set_nonblocking(true)?;
    let listener = TcpListener::from_std(socket.into())?;
    Ok(TcpIncoming::from(listener))
}

pub struct FlightService {
    pub config: InnerConfig,
    pub abort_notify: Arc<Notify>,
}

impl FlightService {
    pub fn create(config: InnerConfig) -> Result<Box<dyn DatabendQueryServer>> {
        Ok(Box::new(Self {
            config,
            abort_notify: Arc::new(Notify::new()),
        }))
    }

    fn shutdown_notify(&self) -> impl Future<Output = ()> + 'static {
        let notified = self.abort_notify.clone();
        async move {
            notified.notified().await;
        }
    }

    #[async_backtrace::framed]
    async fn server_tls_config(conf: &InnerConfig) -> Result<ServerTlsConfig> {
        let cert = tokio::fs::read(conf.query.common.rpc_tls_server_cert.as_str()).await?;
        let key = tokio::fs::read(conf.query.common.rpc_tls_server_key.as_str()).await?;
        let server_identity = Identity::from_pem(cert, key);
        let tls_conf = ServerTlsConfig::new().identity(server_identity);
        Ok(tls_conf)
    }

    #[async_backtrace::framed]
    pub async fn start_with_incoming(&mut self, addr: SocketAddr) -> Result<()> {
        let flight_api_service = DatabendQueryFlightService::create();
        let builder = Server::builder();
        let mut builder = if self.config.tls_rpc_server_enabled() {
            info!("databend query tls rpc enabled");
            builder
                .tls_config(Self::server_tls_config(&self.config).await.map_err(|e| {
                    ErrorCode::TLSConfigurationFailure(format!(
                        "failed to load server tls config: {e}",
                    ))
                })?)
                .map_err(|e| {
                    ErrorCode::TLSConfigurationFailure(format!("failed to invoke tls_config: {e}",))
                })?
        } else {
            builder
        };

        let incoming = bind_flight_listener(addr)
            .map_err(|e| ErrorCode::CannotListenerPort(format!("{},{}", e, addr)))?
            .with_nodelay(Some(true))
            .with_keepalive(None);
        let server = builder
            .add_service(
                FlightServiceServer::new(flight_api_service)
                    .max_encoding_message_size(usize::MAX)
                    .max_decoding_message_size(usize::MAX),
            )
            .serve_with_incoming_shutdown(incoming, self.shutdown_notify());

        databend_common_base::runtime::spawn(server);
        Ok(())
    }
}

#[async_trait::async_trait]
impl DatabendQueryServer for FlightService {
    #[async_backtrace::framed]
    async fn shutdown(&mut self, _graceful: bool) {}

    #[async_backtrace::framed]
    async fn start(&mut self, addr: SocketAddr) -> Result<SocketAddr> {
        self.start_with_incoming(addr).await?;
        Ok(addr)
    }
}
