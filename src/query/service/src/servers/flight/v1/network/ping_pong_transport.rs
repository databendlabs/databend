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

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use arrow_flight::FlightData;
use async_channel::Sender;
use async_channel::TrySendError;
use databend_common_base::base::WatchNotify;
use databend_common_base::runtime::Runtime;
use databend_common_grpc::SocketProbe;
use futures::StreamExt;
use futures::future::Either;
use futures::future::select;
use futures::stream::BoxStream;
use parking_lot::Mutex;
use tokio::task::JoinHandle;
use tonic::Status;
use tonic::transport::Channel;

use super::wire_format::flight_data_size;

pub const REMOTE_FLIGHT_CHANNEL_CLOSED_MESSAGE: &str =
    "Aborted query, because the remote flight channel is closed.";

/// Frames a do_exchange connection may have awaiting their responses.
///
/// Every response is sent by a task on each side that only runs when the OS
/// schedules it. With one frame in flight the link idles for every such delay,
/// which grows to milliseconds when executor threads saturate the cores. Several
/// smaller frames in flight keep the link busy during those delays without
/// raising the bytes in flight.
pub const MAX_FRAMES_IN_FLIGHT: usize = 4;

/// The largest frame a do_exchange connection sends: `exchange_frame_bytes` above it is
/// clamped, so that the frames in flight fit in the receive window of the server.
pub const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;

/// Response from a ping-pong exchange.
pub struct PingPongResponse {
    pub data: Result<FlightData, Status>,
    pub rtt: Duration,
}

/// Callback trait for handling ping-pong responses.
pub trait PingPongCallback: Send + Sync + 'static {
    fn has_pending(&self) -> bool;
    fn on_response(&self, response: PingPongResponse);
    fn on_closed(&self);
}

/// How the connection of a transport was set up, kept for its statistics.
pub struct ConnectionTrace {
    /// Index of the connection among the connections of the exchange to the same node.
    pub lane: usize,
    /// Creating the channel: name resolution, TCP connect, TLS and HTTP/2 handshakes.
    pub channel_time: Duration,
    /// Opening the do_exchange stream, a round trip to the server.
    pub open_time: Duration,
    pub probe: SocketProbe,
    /// Keeps the connection open until its statistics are read.
    pub channel: Channel,
    /// The runtime polling the connection, the one it was created on.
    pub runtime: tokio::runtime::Handle,
}

/// Requests sent on a connection and their responses.
#[derive(Default)]
struct SendLog {
    /// Send times of the requests in flight; responses arrive in request order.
    send_times: VecDeque<Instant>,
    frames: u64,
    bytes: u64,
    rtt_total: Duration,
    rtt_max: Duration,
    first_send: Option<Instant>,
    last_response: Option<Instant>,
}

/// Snapshot of the requests a connection sent, see [`PingPongTransportInner::send_stats`].
#[derive(Clone, Copy, Debug, Default)]
pub struct SendStats {
    /// Frames answered by a response.
    pub frames: u64,
    /// Bytes of the frames sent.
    pub bytes: u64,
    pub rtt_total: Duration,
    pub rtt_max: Duration,
    /// From the first send to the latest response.
    pub active: Duration,
}

pub struct PingPongTransportInner {
    /// Requests awaiting their responses, at most `max_in_flight`.
    in_flight: AtomicUsize,
    /// The request channel's capacity, so a held slot always fits in it.
    max_in_flight: usize,
    send_log: Mutex<SendLog>,
    send_tx: Sender<FlightData>,
    local_node_id: String,
    remote_node_id: String,
    pub shutdown: WatchNotify,
}

/// Transport Flight frames with a bounded number of requests in flight per connection.
///
/// A request holds one of the connection's slots from sending until its response.
/// While all slots are held, subsequent sends return the data back to the caller.
/// The number of slots is the capacity of the request channel.
pub struct PingPongTransport {
    pub num_threads: usize,
    inner: Arc<PingPongTransportInner>,
    response_stream: Mutex<Option<BoxStream<'static, Result<FlightData, Status>>>>,
    trace: Option<ConnectionTrace>,
}

impl Drop for PingPongTransport {
    fn drop(&mut self) {
        self.inner.shutdown.notify_waiters();
    }
}

impl std::ops::Deref for PingPongTransport {
    type Target = PingPongTransportInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl PingPongTransportInner {
    pub fn get_rtt(&self) -> Duration {
        let mut log = self.send_log.lock();
        let Some(sent) = log.send_times.pop_front() else {
            return Duration::default();
        };
        let now = Instant::now();
        let rtt = now.saturating_duration_since(sent);
        log.frames += 1;
        log.rtt_total += rtt;
        log.rtt_max = log.rtt_max.max(rtt);
        log.last_response = Some(now);
        rtt
    }

    pub fn send_stats(&self) -> SendStats {
        let log = self.send_log.lock();
        let active = match (log.first_send, log.last_response) {
            (Some(first), Some(last)) => last.saturating_duration_since(first),
            _ => Duration::default(),
        };
        SendStats {
            frames: log.frames,
            bytes: log.bytes,
            rtt_total: log.rtt_total,
            rtt_max: log.rtt_max,
            active,
        }
    }
    /// Try to send data through the exchange.
    ///
    /// Returns:
    /// - `Ok(None)`: Data was sent successfully
    /// - `Ok(Some(data))`: All slots are in flight, data is returned to caller
    /// - `Err(status)`: The exchange is closed
    pub fn try_send_frame(&self, data: FlightData) -> Result<Option<FlightData>, Status> {
        if !self.try_acquire_send_slot() {
            return Ok(Some(data));
        }

        self.send_frame(data).map(|()| None)
    }

    /// Claim an in-flight slot without sending. On success the caller must follow
    /// up with `send_frame` or `release_send_slot`.
    pub(crate) fn try_acquire_send_slot(&self) -> bool {
        self.in_flight
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                (held < self.max_in_flight).then_some(held + 1)
            })
            .is_ok()
    }

    /// Send a frame after acquiring an in-flight slot, or fail the exchange.
    /// Each queued request holds a slot and the queue holds one request per slot,
    /// so a full request queue while holding a slot is a protocol invariant violation.
    pub(crate) fn send_frame(&self, data: FlightData) -> Result<(), Status> {
        let size = flight_data_size(&data) as u64;
        // The lock orders the send time with the request, as responses follow requests.
        let mut log = self.send_log.lock();
        match self.send_tx.try_send(data) {
            Ok(_) => {
                let now = Instant::now();
                log.send_times.push_back(now);
                log.first_send.get_or_insert(now);
                log.bytes += size;
                Ok(())
            }
            Err(TrySendError::Closed(_)) => {
                drop(log);
                self.release_send_slot();
                Err(Status::aborted(REMOTE_FLIGHT_CHANNEL_CLOSED_MESSAGE))
            }
            Err(TrySendError::Full(_)) => {
                drop(log);
                self.release_send_slot();
                Err(Status::internal(
                    "Flight request queue is full while holding the send slot",
                ))
            }
        }
    }

    pub fn release_send_slot(&self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

impl PingPongTransport {
    /// Wrap a do_exchange connection. A request channel of capacity `n` allows
    /// `n` requests in flight.
    pub fn from_stream(
        num_threads: usize,
        send_tx: async_channel::Sender<FlightData>,
        stream: impl futures::Stream<Item = Result<FlightData, Status>> + Send + 'static,
        local_node_id: impl Into<String>,
        remote_node_id: impl Into<String>,
    ) -> Self {
        let inner = Arc::new(PingPongTransportInner {
            in_flight: AtomicUsize::new(0),
            max_in_flight: send_tx.capacity().unwrap_or(1).max(1),
            send_log: Mutex::new(SendLog::default()),
            send_tx,
            local_node_id: local_node_id.into(),
            remote_node_id: remote_node_id.into(),
            shutdown: WatchNotify::new(),
        });

        Self {
            inner,
            num_threads,
            response_stream: Mutex::new(Some(Box::pin(stream))),
            trace: None,
        }
    }

    pub fn with_trace(mut self, trace: ConnectionTrace) -> Self {
        self.trace = Some(trace);
        self
    }

    pub fn trace(&self) -> Option<&ConnectionTrace> {
        self.trace.as_ref()
    }

    pub fn local_node_id(&self) -> &str {
        &self.inner.local_node_id
    }

    pub fn remote_node_id(&self) -> &str {
        &self.inner.remote_node_id
    }

    /// Start the receiver with the given callback.
    ///
    /// This should be called before sending data. The callback will be invoked
    /// for each response received from the remote end.
    ///
    /// Returns an error if the receiver has already been started.
    pub fn start(
        &self,
        callback: Arc<dyn PingPongCallback>,
        runtime: &Runtime,
    ) -> Result<JoinHandle<()>, Status> {
        let Some(mut stream) = self.response_stream.lock().take() else {
            return Err(Status::already_exists("Receiver already started"));
        };

        let inner = self.inner.clone();
        Ok(runtime.spawn(async move {
            enum Event {
                Shutdown,
                Response(Option<Result<FlightData, Status>>),
            }

            let mut finished = false;

            loop {
                if finished && !callback.has_pending() {
                    inner.send_tx.close();
                }

                let event = if finished {
                    // WatchNotify is sticky; calling notified() again would spin forever.
                    Event::Response(stream.next().await)
                } else {
                    let shutdown_fut = Box::pin(inner.shutdown.notified());
                    match select(shutdown_fut, stream.next()).await {
                        Either::Left(_) => Event::Shutdown,
                        Either::Right((response, _)) => Event::Response(response),
                    }
                };

                match event {
                    Event::Shutdown => finished = true,
                    Event::Response(None) => {
                        callback.on_closed();
                        break;
                    }
                    Event::Response(Some(Ok(data))) => {
                        let rtt = inner.get_rtt();
                        callback.on_response(PingPongResponse {
                            data: Ok(data),
                            rtt,
                        });
                    }
                    Event::Response(Some(Err(status))) => {
                        let rtt = inner.get_rtt();
                        callback.on_response(PingPongResponse {
                            data: Err(status),
                            rtt,
                        });
                        break;
                    }
                }
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use arrow_flight::FlightData;
    use tonic::Status;

    use super::*;

    struct NoPendingCallback;

    impl PingPongCallback for NoPendingCallback {
        fn has_pending(&self) -> bool {
            false
        }

        fn on_response(&self, _response: PingPongResponse) {}

        fn on_closed(&self) {}
    }

    fn create_mock_exchange(
        num_threads: usize,
    ) -> (
        PingPongTransport,
        async_channel::Receiver<FlightData>,
        async_channel::Sender<Result<FlightData, Status>>,
    ) {
        let (send_tx, send_rx) = async_channel::bounded(1);
        let (pong_tx, pong_rx) = async_channel::unbounded();
        let exchange = PingPongTransport::from_stream(
            num_threads,
            send_tx,
            pong_rx,
            "query-node-0",
            "query-node-1",
        );
        (exchange, send_rx, pong_tx)
    }

    fn make_flight_data(len: usize) -> FlightData {
        FlightData {
            data_body: bytes::Bytes::from(vec![0u8; len]),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_ping_pong_basic_send_recv() {
        let (exchange, send_rx, _pong_tx) = create_mock_exchange(2);

        // First send should succeed
        assert!(
            exchange
                .try_send_frame(make_flight_data(10))
                .unwrap()
                .is_none()
        );

        // Data should arrive on send_rx
        let received = send_rx.recv().await.unwrap();
        assert_eq!(received.data_body.len(), 10);

        // Simulate pong by clearing in_flight
        exchange.release_send_slot();

        // Second send should succeed
        assert!(
            exchange
                .try_send_frame(make_flight_data(20))
                .unwrap()
                .is_none()
        );
        let received = send_rx.recv().await.unwrap();
        assert_eq!(received.data_body.len(), 20);
    }

    #[tokio::test]
    async fn test_ping_pong_in_flight_returns_data() {
        let (exchange, send_rx, _pong_tx) = create_mock_exchange(2);

        // First send succeeds
        assert!(
            exchange
                .try_send_frame(make_flight_data(1))
                .unwrap()
                .is_none()
        );

        // Second send returns data back (in-flight)
        let returned = exchange.try_send_frame(make_flight_data(2)).unwrap();
        assert!(returned.is_some());
        assert_eq!(returned.unwrap().data_body.len(), 2);

        // Drain the channel and simulate pong
        let _ = send_rx.recv().await.unwrap();
        exchange.release_send_slot();

        // Now send should succeed again
        assert!(
            exchange
                .try_send_frame(make_flight_data(3))
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_ping_pong_slots_follow_request_channel_capacity() {
        let (send_tx, send_rx) = async_channel::bounded(3);
        let (_pong_tx, pong_rx) = async_channel::unbounded::<Result<FlightData, Status>>();
        let exchange = PingPongTransport::from_stream(1, send_tx, pong_rx, "local", "remote");

        for len in 1..=3 {
            assert!(
                exchange
                    .try_send_frame(make_flight_data(len))
                    .unwrap()
                    .is_none()
            );
        }
        let returned = exchange.try_send_frame(make_flight_data(4)).unwrap();
        assert_eq!(returned.unwrap().data_body.len(), 4);

        // A response releases one slot; frames arrive in send order.
        assert_eq!(send_rx.recv().await.unwrap().data_body.len(), 1);
        exchange.release_send_slot();
        assert!(
            exchange
                .try_send_frame(make_flight_data(5))
                .unwrap()
                .is_none()
        );
        for len in [2, 3, 5] {
            assert_eq!(send_rx.recv().await.unwrap().data_body.len(), len);
        }
    }

    #[tokio::test]
    async fn test_ping_pong_closed_returns_error() {
        let (exchange, send_rx, _pong_tx) = create_mock_exchange(1);

        // Drop the receiver to close the channel
        drop(send_rx);

        let result = exchange.try_send_frame(make_flight_data(1));
        let status = result.expect_err("closed request channel should abort the exchange");
        assert_eq!(status.code(), tonic::Code::Aborted);
        assert_eq!(status.message(), REMOTE_FLIGHT_CHANNEL_CLOSED_MESSAGE);
    }

    // Regression test for https://github.com/databendlabs/databend/issues/20228.
    #[tokio::test]
    async fn test_ping_pong_shutdown_finishes_after_response_stream_closes() {
        let runtime = Runtime::with_worker_threads(1, None).unwrap();
        let (exchange, send_rx, pong_tx) = create_mock_exchange(1);
        let mut handle = exchange
            .start(Arc::new(NoPendingCallback), &runtime)
            .unwrap();

        drop(exchange);

        let send_result = tokio::time::timeout(Duration::from_secs(2), send_rx.recv()).await;
        if send_result.is_err() {
            handle.abort();
        }
        assert!(
            send_result
                .expect("shutdown should close the request stream")
                .is_err(),
            "request stream should be closed without sending data"
        );

        drop(pong_tx);

        let join_result = tokio::time::timeout(Duration::from_secs(2), &mut handle).await;
        if join_result.is_err() {
            handle.abort();
        }
        join_result
            .expect("receiver task should finish after the response stream closes")
            .expect("receiver task should not panic");
    }

    #[tokio::test]
    async fn test_ping_pong_send_stats() {
        let (exchange, send_rx, _pong_tx) = create_mock_exchange(1);
        assert_eq!(exchange.send_stats().frames, 0);

        assert!(
            exchange
                .try_send_frame(make_flight_data(10))
                .unwrap()
                .is_none()
        );
        let _ = send_rx.recv().await.unwrap();
        let stats = exchange.send_stats();
        assert_eq!(stats.bytes, 10);
        // A frame is counted once answered.
        assert_eq!(stats.frames, 0);

        tokio::time::sleep(Duration::from_millis(5)).await;
        let rtt = exchange.get_rtt();
        assert!(rtt >= Duration::from_millis(5));
        let stats = exchange.send_stats();
        assert_eq!(stats.frames, 1);
        assert_eq!(stats.rtt_total, rtt);
        assert_eq!(stats.rtt_max, rtt);
        assert!(stats.active >= rtt);

        // A response without a request in flight is not counted.
        assert_eq!(exchange.get_rtt(), Duration::ZERO);
        assert_eq!(exchange.send_stats().frames, 1);
    }
}
