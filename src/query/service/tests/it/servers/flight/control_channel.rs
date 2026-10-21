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

//! Behavior of the reused control-plane connection to a peer node, observed
//! only through `Cluster::do_action` results and the TCP connections that reach
//! the peer through a fault-injecting proxy.
//!
//! A cached channel that completed a request within the last second is used as
//! is; one that was quiet for longer is verified with a `PING` action first.
//! `QUIET` is long enough to make the next request take the ping path.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::net::TcpListener as StdTcpListener;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use databend_base::uniq_id::GlobalUniq;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_settings::FlightKeepAliveParams;
use databend_meta_client::types::NodeInfo;
use databend_query::clusters::Cluster;
use databend_query::clusters::ClusterHelper;
use databend_query::clusters::FlightParams;
use databend_query::servers::Server;
use databend_query::servers::flight::FlightService;
use databend_query::servers::flight::v1::actions::GET_RUNNING_QUERY_DUMP;
use databend_query::test_kits::TestFixture;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::watch;

/// Forwards TCP connections to `upstream` and can break them on demand.
struct FaultProxy {
    addr: SocketAddr,
    accepted: Arc<AtomicUsize>,
    close_all: watch::Sender<u64>,
    accept_loop: tokio::task::JoinHandle<()>,
}

impl FaultProxy {
    async fn start(upstream: SocketAddr) -> anyhow::Result<FaultProxy> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let accepted = Arc::new(AtomicUsize::new(0));
        let (close_all, close_rx) = watch::channel(0);

        let proxy_accepted = accepted.clone();
        let accept_loop = databend_common_base::runtime::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                proxy_accepted.fetch_add(1, Ordering::SeqCst);
                databend_common_base::runtime::spawn(forward(client, upstream, close_rx.clone()));
            }
        });

        Ok(FaultProxy {
            addr,
            accepted,
            close_all,
            accept_loop,
        })
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    /// Closes every open connection, as a peer restart or reset would.
    fn close_all_connections(&self) {
        self.close_all.send_modify(|generation| *generation += 1);
    }

    /// Stops listening and closes every open connection, as a peer that went
    /// down would. New connections to the proxy address are refused.
    fn shutdown(&self) {
        self.accept_loop.abort();
        self.close_all_connections();
    }
}

async fn forward(client: TcpStream, upstream: SocketAddr, mut close_all: watch::Receiver<u64>) {
    close_all.mark_unchanged();
    let Ok(server) = TcpStream::connect(upstream).await else {
        return;
    };
    let (client_read, client_write) = client.into_split();
    let (server_read, server_write) = server.into_split();

    tokio::select! {
        _ = pipe(client_read, server_write) => {}
        _ = pipe(server_read, client_write) => {}
        _ = close_all.changed() => {}
    }
    // Dropping both halves of both sockets closes the connection.
}

async fn pipe(mut from: OwnedReadHalf, mut to: OwnedWriteHalf) {
    let mut buf = vec![0; 64 * 1024];
    loop {
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}

struct Peer {
    _server: Box<dyn Server>,
    proxy: FaultProxy,
    cluster: Arc<Cluster>,
    node_id: String,
}

impl Peer {
    /// Starts a flight server and a cluster whose only remote node is that
    /// server, reached through a proxy. Each peer gets a unique node id so
    /// tests do not share cached connections.
    async fn start() -> anyhow::Result<Peer> {
        let config = GlobalConfig::instance();
        let server_addr = free_local_addr()?;
        let mut server = FlightService::create(config.as_ref().clone())?;
        server.start(server_addr).await?;

        let proxy = FaultProxy::start(server_addr).await?;
        let node_id = format!("control-channel-test-{}", GlobalUniq::unique());
        let node = NodeInfo::create(
            node_id.clone(),
            config.query.node_secret.clone(),
            String::new(),
            proxy.addr.to_string(),
            String::new(),
            String::new(),
            String::new(),
        );
        let cluster = Cluster::create(vec![Arc::new(node)], config.query.node_id.clone());

        Ok(Peer {
            _server: server,
            proxy,
            cluster,
            node_id,
        })
    }

    async fn request(&self, reuse_control_channel: bool) -> Result<String> {
        let params = FlightParams {
            timeout: 60,
            retry_times: 0,
            retry_interval: 0,
            keep_alive: FlightKeepAliveParams::default(),
            reuse_control_channel,
        };
        let message = HashMap::from([(self.node_id.clone(), "no-such-query".to_string())]);
        let mut response: HashMap<String, String> = self
            .cluster
            .do_action(GET_RUNNING_QUERY_DUMP, message, params)
            .await?;
        Ok(response.remove(&self.node_id).unwrap_or_default())
    }
}

/// Longer than the control channel's ping freshness window.
const QUIET: Duration = Duration::from_millis(1500);

fn free_local_addr() -> anyhow::Result<SocketAddr> {
    let listener = StdTcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?)
}

#[tokio::test(flavor = "current_thread")]
async fn test_requests_share_one_connection() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let peer = Peer::start().await?;

    for _ in 0..3 {
        peer.request(true).await?;
    }
    assert_eq!(peer.proxy.accepted(), 1);

    // Without reuse, every request opens its own connection.
    for _ in 0..3 {
        peer.request(false).await?;
    }
    assert_eq!(peer.proxy.accepted(), 4);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_reconnects_after_peer_closes_connection() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let peer = Peer::start().await?;

    peer.request(true).await?;
    assert_eq!(peer.proxy.accepted(), 1);

    peer.proxy.close_all_connections();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Still within the freshness window, so no ping: the request itself goes
    // over the closed connection and tonic reconnects underneath it.
    peer.request(true).await?;
    peer.request(true).await?;
    assert_eq!(peer.proxy.accepted(), 2);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_ping_verifies_quiet_channel_after_peer_closes_connection() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let peer = Peer::start().await?;

    peer.request(true).await?;
    assert_eq!(peer.proxy.accepted(), 1);

    peer.proxy.close_all_connections();
    tokio::time::sleep(QUIET).await;

    // The channel was quiet, so it is pinged first. The ping reconnects the
    // closed connection; the request then reuses it.
    peer.request(true).await?;
    assert_eq!(peer.proxy.accepted(), 2);

    // Verified a moment ago: no further ping or connection for the next one.
    peer.request(true).await?;
    assert_eq!(peer.proxy.accepted(), 2);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_peer_down_reports_cannot_connect() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let peer = Peer::start().await?;

    peer.request(true).await?;
    assert_eq!(peer.proxy.accepted(), 1);

    peer.proxy.shutdown();
    tokio::time::sleep(QUIET).await;

    // The ping on the cached channel is refused, the channel is dropped, and
    // the new connection is refused too. The error must carry the same code as
    // a failed connection on a fresh client, so callers that retry or report
    // on CANNOT_CONNECT_NODE behave the same with reuse.
    let started = std::time::Instant::now();
    let error = peer.request(true).await.unwrap_err();
    assert_eq!(error.code(), ErrorCode::CANNOT_CONNECT_NODE, "{error}");
    assert_eq!(peer.proxy.accepted(), 1);
    // Both attempts are refused immediately; nothing waits for a timeout.
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    Ok(())
}
