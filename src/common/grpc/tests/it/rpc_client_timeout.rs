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

use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;

use databend_common_grpc::ConnectionFactory;
use http_body_util::Empty;
use hyper::body::Bytes;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tonic::transport::Endpoint;

/// Starts an HTTP/2 server that delays gRPC responses by `delay`.
/// Returns the server address and a join handle.
async fn start_delayed_grpc_server(
    delay: Duration,
) -> anyhow::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let handle = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let delay = delay;
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let _ = hyper::server::conn::http2::Builder::new(
                    hyper_util::rt::TokioExecutor::new(),
                )
                .serve_connection(
                    io,
                    service_fn(move |_req: hyper::Request<Incoming>| {
                        let delay = delay;
                        async move {
                            tokio::time::sleep(delay).await;
                            Ok::<_, Infallible>(
                                hyper::Response::builder()
                                    .header("content-type", "application/grpc")
                                    .header("grpc-status", "0")
                                    .body(Empty::<Bytes>::new())
                                    .unwrap(),
                            )
                        }
                    }),
                )
                .await;
            });
        }
    });

    Ok((addr, handle))
}

/// Helper: send a gRPC unary request through a tonic channel, mirroring
/// how Databend's `flight_client.rs` calls `do_action`.
///
/// Uses `tonic::client::Grpc` — the same client layer that all generated
/// tonic service clients (including `FlightServiceClient`) use internally.
/// The URI path is arbitrary since our test server handles all paths.
///
/// `request_timeout` sets the `grpc-timeout` header, same as
/// `flight_client.rs`'s `request.set_timeout(Duration::from_secs(timeout))`.
async fn send_grpc_request(
    channel: &tonic::transport::Channel,
    request_timeout: Duration,
) -> Result<(), tonic::Status> {
    let mut grpc = tonic::client::Grpc::new(channel.clone());
    let codec = tonic::codec::ProstCodec::<(), ()>::default();
    let path: http::uri::PathAndQuery = "/test.Service/Method".parse().unwrap();

    let mut request = tonic::Request::new(());
    request.set_timeout(request_timeout);
    grpc.ready().await.map_err(|e| tonic::Status::internal(e.to_string()))?;
    grpc.unary(request, path, codec).await?;
    Ok(())
}

/// Verifies that `rpc_client_timeout_secs` (the `timeout` param passed to
/// `create_rpc_channel`) does NOT limit per-request duration.
///
/// The server delays its response by 3s. The channel is created with
/// `timeout = 1s`. After the fix, `endpoint.timeout()` is not set, so
/// the request should succeed despite taking 3s.
#[tokio::test(flavor = "multi_thread")]
async fn test_rpc_client_timeout_is_connect_only() -> anyhow::Result<()> {
    let (addr, _server) = start_delayed_grpc_server(Duration::from_secs(3)).await?;

    let channel = ConnectionFactory::create_rpc_channel(
        addr.to_string(),
        // This corresponds to config.query.rpc_client_timeout_secs, which semantically
        // is a TCP connect timeout. The field name is misleading but kept for config
        // compatibility (not renamed in this PR).
        Some(Duration::from_secs(1)),
        None,
        None,
    )
    .await?;

    // Per-request timeout, same as flight_client.rs's request.set_timeout().
    // This sets the grpc-timeout header. After the fix, the effective timeout
    // should be this value (10s), not capped by rpc_client_timeout_secs (1s).
    let per_request_timeout = Duration::from_secs(10);

    let result = send_grpc_request(&channel, per_request_timeout).await;

    // After fix: no endpoint.timeout, so the request waits for the server's
    // 3s delay and succeeds. It must NOT be cancelled at 1s.
    match result {
        Ok(()) => {} // success — no per-request timeout enforced
        Err(status) => {
            assert_ne!(
                status.code(),
                tonic::Code::Cancelled,
                "should not get Cancelled from endpoint.timeout, but got: {}",
                status.message()
            );
        }
    }

    Ok(())
}

/// Reproduces the original bug: when `endpoint.timeout()` is explicitly set
/// to a small value, it caps per-request duration via tonic's client-side
/// `GrpcTimeout` layer (`min(grpc-timeout header, endpoint.timeout)`).
///
/// The server delays its response by 3s. The endpoint has `.timeout(1s)`.
/// The request should fail at ~1s.
#[tokio::test(flavor = "multi_thread")]
async fn test_endpoint_timeout_causes_early_cancellation() -> anyhow::Result<()> {
    let (addr, _server) = start_delayed_grpc_server(Duration::from_secs(3)).await?;

    // Build channel WITH endpoint.timeout(1s) — the old buggy behavior.
    let uri: http::Uri = format!("http://{}", addr).parse()?;
    let endpoint = Endpoint::from(uri).timeout(Duration::from_secs(1));

    let mut connector =
        hyper_util::client::legacy::connect::HttpConnector::new_with_resolver(
            databend_common_grpc::DNSService,
        );
    connector.set_nodelay(true);

    let channel = endpoint.connect_with_connector(connector).await?;

    // Per-request timeout via grpc-timeout header (10s), but endpoint.timeout(1s)
    // caps it to min(10s, 1s) = 1s.
    let per_request_timeout = Duration::from_secs(10);

    let result = send_grpc_request(&channel, per_request_timeout).await;

    let err = result.unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::Cancelled,
        "expected Cancelled from endpoint.timeout, got: {:?} {}",
        err.code(),
        err.message()
    );

    Ok(())
}
