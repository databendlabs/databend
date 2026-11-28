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

//! Observes the TCP connections a gRPC channel dials.
//!
//! tonic dials a channel's connection inside its own connector stack and hides
//! the socket. [`ProbedConnector`] sits under that stack, so it sees every TCP
//! connect of the channel, including reconnects tonic performs on its own, and
//! keeps a handle to the live socket so its kernel statistics can be read.

use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use hyper::Uri;
use hyper_util::rt::TokioIo;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::net::TcpStream;
use tower_service::Service;

/// What the connector observed for one channel: the TCP connects it made and
/// the socket of the latest one while that socket is open.
#[derive(Clone, Default)]
pub struct SocketProbe {
    shared: Arc<ProbeShared>,
}

#[derive(Default)]
struct ProbeShared {
    connects: AtomicU64,
    connect_nanos: AtomicU64,
    /// Identifies the latest socket, so an older socket closing late does not
    /// unregister it.
    next_socket_id: AtomicU64,
    /// The latest socket while it is open. The socket's file descriptor is
    /// closed only after the socket removed itself here under this lock, so a
    /// descriptor read under the lock is never a reused one.
    socket: parking_lot::Mutex<Option<(u64, RawSocket)>>,
}

#[cfg(unix)]
type RawSocket = std::os::fd::RawFd;
#[cfg(not(unix))]
type RawSocket = ();

impl SocketProbe {
    /// TCP connects made so far, reconnects included.
    pub fn connects(&self) -> u64 {
        self.shared.connects.load(Ordering::Relaxed)
    }

    /// Time the latest TCP connect took, including name resolution.
    pub fn last_connect_time(&self) -> Duration {
        Duration::from_nanos(self.shared.connect_nanos.load(Ordering::Relaxed))
    }

    /// Kernel statistics of the open socket, `None` when it is closed or the
    /// platform does not provide them.
    pub fn tcp_info(&self) -> Option<TcpInfo> {
        let socket = self.shared.socket.lock();
        let (_, socket) = socket.as_ref()?;
        read_tcp_info(socket)
    }

    fn register(&self, socket: RawSocket, connect_time: Duration) -> u64 {
        let id = self.shared.next_socket_id.fetch_add(1, Ordering::Relaxed);
        self.shared.connects.fetch_add(1, Ordering::Relaxed);
        self.shared
            .connect_nanos
            .store(connect_time.as_nanos() as u64, Ordering::Relaxed);
        *self.shared.socket.lock() = Some((id, socket));
        id
    }

    fn unregister(&self, id: u64) {
        let mut socket = self.shared.socket.lock();
        if socket.as_ref().is_some_and(|(current, _)| *current == id) {
            *socket = None;
        }
    }
}

/// The sender-side state of a TCP connection that tells whether the sending
/// application, the receiver, or the network limited it. Times are in
/// microseconds since the connection was established.
#[derive(Clone, Copy, Debug, Default)]
pub struct TcpInfo {
    /// Smoothed round-trip time.
    pub rtt_us: u32,
    pub min_rtt_us: u32,
    /// Congestion window, in segments.
    pub snd_cwnd: u32,
    pub snd_mss: u32,
    pub total_retrans: u32,
    pub bytes_sent: u64,
    /// Bytes the peer acknowledged.
    pub bytes_acked: u64,
    pub bytes_retrans: u64,
    pub bytes_received: u64,
    /// Latest delivery rate estimate, bytes per second.
    pub delivery_rate: u64,
    /// The delivery rate sample was limited by the application not sending.
    pub delivery_rate_app_limited: bool,
    /// Time with data outstanding.
    pub busy_time_us: u64,
    /// Time stalled by the receive window of the peer.
    pub rwnd_limited_us: u64,
    /// Time stalled by the local send buffer.
    pub sndbuf_limited_us: u64,
    /// Bytes written but not yet sent.
    pub notsent_bytes: u32,
}

impl fmt::Display for TcpInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "rtt={}us min_rtt={}us cwnd={} mss={} retrans={} sent={} acked={} retrans_bytes={} received={} delivery_rate={}/s app_limited={} busy={}us rwnd_limited={}us sndbuf_limited={}us notsent={}",
            self.rtt_us,
            self.min_rtt_us,
            self.snd_cwnd,
            self.snd_mss,
            self.total_retrans,
            self.bytes_sent,
            self.bytes_acked,
            self.bytes_retrans,
            self.bytes_received,
            self.delivery_rate,
            self.delivery_rate_app_limited,
            self.busy_time_us,
            self.rwnd_limited_us,
            self.sndbuf_limited_us,
            self.notsent_bytes,
        )
    }
}

/// `struct tcp_info` of `include/uapi/linux/tcp.h` up to `tcpi_snd_wnd`
/// (Linux 5.4). The glibc definition in `libc` stops at `tcpi_total_retrans`.
/// An older kernel fills a prefix and leaves the rest zeroed.
#[cfg(target_os = "linux")]
#[repr(C)]
#[derive(Default)]
struct LinuxTcpInfo {
    tcpi_state: u8,
    tcpi_ca_state: u8,
    tcpi_retransmits: u8,
    tcpi_probes: u8,
    tcpi_backoff: u8,
    tcpi_options: u8,
    tcpi_snd_rcv_wscale: u8,
    /// Bit 0: `tcpi_delivery_rate_app_limited`.
    tcpi_flags: u8,
    tcpi_rto: u32, // typos:disable-line
    tcpi_ato: u32,
    tcpi_snd_mss: u32,
    tcpi_rcv_mss: u32,
    tcpi_unacked: u32,
    tcpi_sacked: u32,
    tcpi_lost: u32,
    tcpi_retrans: u32,
    tcpi_fackets: u32,
    tcpi_last_data_sent: u32,
    tcpi_last_ack_sent: u32,
    tcpi_last_data_recv: u32,
    tcpi_last_ack_recv: u32,
    tcpi_pmtu: u32,
    tcpi_rcv_ssthresh: u32,
    tcpi_rtt: u32,
    tcpi_rttvar: u32,
    tcpi_snd_ssthresh: u32,
    tcpi_snd_cwnd: u32,
    tcpi_advmss: u32,
    tcpi_reordering: u32,
    tcpi_rcv_rtt: u32,
    tcpi_rcv_space: u32,
    tcpi_total_retrans: u32,
    tcpi_pacing_rate: u64,
    tcpi_max_pacing_rate: u64,
    tcpi_bytes_acked: u64,
    tcpi_bytes_received: u64,
    tcpi_segs_out: u32,
    tcpi_segs_in: u32,
    tcpi_notsent_bytes: u32,
    tcpi_min_rtt: u32,
    tcpi_data_segs_in: u32,
    tcpi_data_segs_out: u32,
    tcpi_delivery_rate: u64,
    tcpi_busy_time: u64,
    tcpi_rwnd_limited: u64,
    tcpi_sndbuf_limited: u64,
    tcpi_delivered: u32,
    tcpi_delivered_ce: u32,
    tcpi_bytes_sent: u64,
    tcpi_bytes_retrans: u64,
    tcpi_dsack_dups: u32,
    tcpi_reord_seen: u32, // typos:disable-line
    tcpi_rcv_ooopack: u32,
    tcpi_snd_wnd: u32,
}

#[cfg(target_os = "linux")]
fn read_tcp_info(socket: &RawSocket) -> Option<TcpInfo> {
    let mut info = LinuxTcpInfo::default();
    let mut len = std::mem::size_of::<LinuxTcpInfo>() as libc::socklen_t;
    // SAFETY: `info` is a plain C struct of `len` bytes, and the descriptor is
    // open while the probe lock is held, see `ProbeShared::socket`.
    let ret = unsafe {
        libc::getsockopt(
            *socket,
            libc::IPPROTO_TCP,
            libc::TCP_INFO,
            &mut info as *mut LinuxTcpInfo as *mut libc::c_void,
            &mut len,
        )
    };
    if ret != 0 {
        return None;
    }
    Some(TcpInfo {
        rtt_us: info.tcpi_rtt,
        min_rtt_us: info.tcpi_min_rtt,
        snd_cwnd: info.tcpi_snd_cwnd,
        snd_mss: info.tcpi_snd_mss,
        total_retrans: info.tcpi_total_retrans,
        bytes_sent: info.tcpi_bytes_sent,
        bytes_acked: info.tcpi_bytes_acked,
        bytes_retrans: info.tcpi_bytes_retrans,
        bytes_received: info.tcpi_bytes_received,
        delivery_rate: info.tcpi_delivery_rate,
        delivery_rate_app_limited: info.tcpi_flags & 1 != 0,
        busy_time_us: info.tcpi_busy_time,
        rwnd_limited_us: info.tcpi_rwnd_limited,
        sndbuf_limited_us: info.tcpi_sndbuf_limited,
        notsent_bytes: info.tcpi_notsent_bytes,
    })
}

#[cfg(not(target_os = "linux"))]
fn read_tcp_info(_socket: &RawSocket) -> Option<TcpInfo> {
    None
}

#[cfg(unix)]
fn raw_socket(stream: &TcpStream) -> RawSocket {
    use std::os::fd::AsRawFd;
    stream.as_raw_fd()
}

#[cfg(not(unix))]
fn raw_socket(_stream: &TcpStream) -> RawSocket {}

/// Wraps the TCP connector of a channel and reports its connects to a [`SocketProbe`].
#[derive(Clone)]
pub struct ProbedConnector<C> {
    inner: C,
    probe: SocketProbe,
}

impl<C> ProbedConnector<C> {
    pub fn new(inner: C, probe: SocketProbe) -> Self {
        ProbedConnector { inner, probe }
    }
}

impl<C> Service<Uri> for ProbedConnector<C>
where
    C: Service<Uri, Response = TokioIo<TcpStream>>,
    C::Future: Send + 'static,
{
    type Response = TokioIo<ProbedStream>;
    type Error = C::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let probe = self.probe.clone();
        let started = Instant::now();
        let connecting = self.inner.call(uri);
        Box::pin(async move {
            let stream = connecting.await?.into_inner();
            let id = probe.register(raw_socket(&stream), started.elapsed());
            Ok(TokioIo::new(ProbedStream { stream, probe, id }))
        })
    }
}

/// A TCP stream registered with a [`SocketProbe`] while it is open.
pub struct ProbedStream {
    stream: TcpStream,
    probe: SocketProbe,
    id: u64,
}

impl Drop for ProbedStream {
    fn drop(&mut self) {
        // Runs before `stream` closes the descriptor.
        self.probe.unregister(self.id);
    }
}

impl AsyncRead for ProbedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for ProbedStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    use super::*;

    #[tokio::test]
    async fn test_probe_tracks_connects_and_socket_lifetime() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = databend_common_base::runtime::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 5];
            socket.read_exact(&mut buf).await.unwrap();
            buf
        });

        let probe = SocketProbe::default();
        let mut http = hyper_util::client::legacy::connect::HttpConnector::new();
        http.enforce_http(false);
        let mut connector = ProbedConnector::new(http, probe.clone());
        let uri: Uri = format!("http://{}", addr).parse().unwrap();
        let stream = connector.call(uri).await.unwrap();
        assert_eq!(probe.connects(), 1);

        let mut stream = stream.into_inner();
        stream.write_all(b"hello").await.unwrap();
        assert_eq!(&server.await.unwrap(), b"hello");

        #[cfg(target_os = "linux")]
        {
            let info = probe.tcp_info().expect("socket is open");
            assert!(info.bytes_sent >= 5);
        }

        drop(stream);
        assert!(probe.tcp_info().is_none());
        assert_eq!(probe.connects(), 1);
    }
}
