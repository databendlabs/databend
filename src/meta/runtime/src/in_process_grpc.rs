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

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use databend_common_grpc::IN_PROCESS_GRPC_SCHEME;
use databend_meta::runtime_api::BoxFuture;
use databend_meta::runtime_api::Channel;
use databend_meta::runtime_api::ChannelError;
use databend_meta::runtime_api::TlsConfig;
use hyper_util::rt::TokioIo;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::DuplexStream;
use tokio::io::ReadBuf;
use tokio::sync::mpsc;
use tonic::transport::Endpoint;
use tonic::transport::server::Connected;
use tonic::transport::server::TcpConnectInfo;
use tower::service_fn;

const IN_PROCESS_GRPC_BUFFER_SIZE: usize = 64 * 1024;

type InProcessGrpcSender = mpsc::UnboundedSender<InProcessGrpcStream>;

static NEXT_IN_PROCESS_GRPC_ENDPOINT_ID: AtomicU64 = AtomicU64::new(1);
static IN_PROCESS_GRPC_ENDPOINTS: LazyLock<Mutex<HashMap<String, InProcessGrpcSender>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// One server side of an in-process gRPC connection.
///
/// Tonic normally derives the peer address from a TCP stream. Embedded meta
/// clients still call `get_client_info()`, so this stream supplies a synthetic
/// loopback address to preserve that API contract without opening a socket.
pub struct InProcessGrpcStream {
    inner: DuplexStream,
}

impl AsyncRead for InProcessGrpcStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for InProcessGrpcStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl Connected for InProcessGrpcStream {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        let loopback = SocketAddr::from(([127, 0, 0, 1], 0));
        TcpConnectInfo {
            local_addr: Some(loopback),
            remote_addr: Some(loopback),
        }
    }
}

/// Registered endpoint for gRPC communication inside one process.
///
/// The endpoint owns the server-side receiver and unregisters its address on
/// drop. Every tonic connection gets a new duplex stream, so reconnects and
/// the meta client's connection TTL retain their normal behavior.
pub struct InProcessGrpcEndpoint {
    address: String,
    incoming: Option<mpsc::UnboundedReceiver<InProcessGrpcStream>>,
}

impl InProcessGrpcEndpoint {
    pub fn new() -> Self {
        let id = NEXT_IN_PROCESS_GRPC_ENDPOINT_ID.fetch_add(1, Ordering::Relaxed);
        let address = format!("{IN_PROCESS_GRPC_SCHEME}{id}");
        let (sender, incoming) = mpsc::unbounded_channel();

        IN_PROCESS_GRPC_ENDPOINTS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(address.clone(), sender);

        Self {
            address,
            incoming: Some(incoming),
        }
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn take_incoming(&mut self) -> mpsc::UnboundedReceiver<InProcessGrpcStream> {
        self.incoming
            .take()
            .expect("in-process gRPC incoming stream can only be taken once")
    }
}

impl Default for InProcessGrpcEndpoint {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for InProcessGrpcEndpoint {
    fn drop(&mut self) {
        IN_PROCESS_GRPC_ENDPOINTS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.address);
    }
}

pub(crate) fn connect_in_process_grpc(
    addr: String,
    timeout: Option<Duration>,
    tls_config: Option<TlsConfig>,
) -> BoxFuture<'static, Result<Channel, ChannelError>> {
    let sender = IN_PROCESS_GRPC_ENDPOINTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&addr)
        .cloned();

    Box::pin(async move {
        if tls_config.is_some() {
            return Err(ChannelError::TlsConfig {
                action: "connecting to an in-process gRPC channel with".to_string(),
                message: "TLS is not supported".to_string(),
            });
        }

        let sender = sender.ok_or_else(|| ChannelError::CannotConnect {
            uri: addr.clone(),
            message: "in-process gRPC endpoint is not registered".to_string(),
        })?;

        let mut endpoint = Endpoint::from_static("http://[::]:0");
        if let Some(timeout) = timeout {
            endpoint = endpoint.timeout(timeout);
        }

        let connector = service_fn(move |_| {
            let sender = sender.clone();
            async move {
                let (client, server) = tokio::io::duplex(IN_PROCESS_GRPC_BUFFER_SIZE);
                sender
                    .send(InProcessGrpcStream { inner: server })
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::ConnectionRefused,
                            "in-process gRPC endpoint closed",
                        )
                    })?;
                Ok::<_, io::Error>(TokioIo::new(client))
            }
        });

        endpoint
            .connect_with_connector(connector)
            .await
            .map_err(|e| ChannelError::CannotConnect {
                uri: addr,
                message: e.to_string(),
            })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_endpoint_unregisters_on_drop() {
        let endpoint = InProcessGrpcEndpoint::new();
        let address = endpoint.address().to_string();

        assert!(
            IN_PROCESS_GRPC_ENDPOINTS
                .lock()
                .unwrap()
                .contains_key(&address)
        );
        drop(endpoint);
        assert!(
            !IN_PROCESS_GRPC_ENDPOINTS
                .lock()
                .unwrap()
                .contains_key(&address)
        );
    }

    #[test]
    fn test_stream_supplies_loopback_connection_info() {
        let (_, server) = tokio::io::duplex(1);
        let stream = InProcessGrpcStream { inner: server };
        let info = stream.connect_info();

        assert_eq!(info.local_addr(), Some(([127, 0, 0, 1], 0).into()));
        assert_eq!(info.remote_addr(), Some(([127, 0, 0, 1], 0).into()));
    }
}
