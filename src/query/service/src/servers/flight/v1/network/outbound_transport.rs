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
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::FlightData;
use async_channel::Sender;
use async_channel::TrySendError;
use databend_common_base::base::WatchNotify;
use databend_common_base::runtime::Runtime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use futures::StreamExt;
use futures::stream::BoxStream;
use parking_lot::Mutex;
use tokio::task::JoinHandle;
use tonic::Status;

use super::ExchangePacket;

type FlightDataStream = BoxStream<'static, std::result::Result<FlightData, Status>>;

/// Callback trait for handling ping-pong responses.
pub trait PingPongCallback: Send + Sync + 'static {
    fn pop_pending(&self) -> Option<FlightData>;
    fn on_remote_finished(&self);
    fn on_error(&self, status: Status);
}

/// One physical do_exchange stream. Reconnect replaces this while the owning
/// PingPongExchange keeps the logical sequence and in-flight packet.
pub struct PingPongTransport {
    pub send_tx: Sender<FlightData>,
    pub response_stream: FlightDataStream,
}

pub type PingPongConnector =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<PingPongTransport>> + Send>> + Send + Sync>;

struct InFlightPacket {
    kind: InFlightKind,
    sequence: u64,
    data: FlightData,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum InFlightKind {
    Data,
    EndOfStream,
}

enum AcknowledgeResult {
    Ignored,
    Data,
    EndOfStream,
}

enum ReconnectOutcome {
    Reconnected(FlightDataStream),
    RemoteFinished,
    Aborted,
    Failed(Status),
}

struct SendState {
    send_tx: Option<Sender<FlightData>>,
    in_flight: Option<InFlightPacket>,
    next_sequence: u64,
    finishing: bool,
    terminal: Option<std::result::Result<(), Status>>,
}

pub struct PingPongExchangeInner {
    send_state: Mutex<SendState>,
    local_node_id: String,
    remote_node_id: String,
    pub finish: WatchNotify,
    pub shutdown: WatchNotify,
}

/// A non-blocking ping-pong style flight exchange.
///
/// This exchange guarantees that at most one request is in-flight at any time.
/// When a request is sent, subsequent sends will return the data back to the caller
/// until a response is received.
pub struct PingPongExchange {
    pub num_threads: usize,
    inner: Arc<PingPongExchangeInner>,
    response_stream: Mutex<Option<FlightDataStream>>,
    connector: Option<PingPongConnector>,
    retry_times: u64,
    retry_interval: Duration,
}

impl Drop for PingPongExchange {
    fn drop(&mut self) {
        self.inner.shutdown.notify_waiters();
    }
}

impl std::ops::Deref for PingPongExchange {
    type Target = PingPongExchangeInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

// Lifecycle vocabulary for one logical exchange:
// - request_finish: normal pipeline completion; non-terminal while draining and sending EOS.
// - finish_remote: successful terminal state, such as an EOS ACK or LIMIT early stop.
// - fail: protocol violation or exhausted reconnects; error terminal reported via callback.
// - abort: forced local cancellation, such as KILL QUERY; stop without EOS or callback.
impl PingPongExchangeInner {
    /// Try to send data through the exchange.
    ///
    /// Returns:
    /// - `Ok(None)`: Data was sent successfully
    /// - `Ok(Some(data))`: A request is already in-flight, data is returned to caller
    /// - `Err(status)`: The exchange is closed
    pub fn try_send(&self, data: FlightData) -> std::result::Result<Option<FlightData>, Status> {
        let mut pending = None;
        self.send_or_enqueue(data, |data| pending = Some(data))?;
        Ok(pending)
    }

    pub fn send_or_enqueue(
        &self,
        data: FlightData,
        enqueue: impl FnOnce(FlightData),
    ) -> std::result::Result<(), Status> {
        // ACK handling takes this lock before inspecting the pending queue. Making the
        // send-or-enqueue decision under the same lock prevents an ACK from observing an
        // empty queue and clearing in_flight immediately before a producer enqueues data.
        let mut state = self.send_state.lock();
        if let Some(terminal) = &state.terminal {
            return terminal.clone();
        }
        if state.finishing {
            return Err(Status::failed_precondition(
                "cannot send data after do_exchange producer completion",
            ));
        }
        if state.in_flight.is_some() {
            enqueue(data);
            return Ok(());
        }

        let sequence = state.next_sequence;
        let encoded = ExchangePacket::data(sequence, data.clone()).encode();
        state.next_sequence += 1;
        state.in_flight = Some(InFlightPacket {
            kind: InFlightKind::Data,
            sequence,
            data: encoded.clone(),
        });

        let Some(send_tx) = &state.send_tx else {
            return Ok(());
        };
        match send_tx.try_send(encoded) {
            Ok(()) | Err(TrySendError::Closed(_)) => Ok(()),
            Err(TrySendError::Full(_)) => {
                state.in_flight = None;
                state.next_sequence -= 1;
                enqueue(data);
                Ok(())
            }
        }
    }

    fn acknowledge_and_advance(
        &self,
        sequence: u64,
        pop_pending: impl FnOnce() -> Option<FlightData>,
    ) -> std::result::Result<AcknowledgeResult, Status> {
        let mut state = self.send_state.lock();
        let Some(in_flight) = &state.in_flight else {
            return Ok(AcknowledgeResult::Ignored);
        };
        if sequence < in_flight.sequence {
            return Ok(AcknowledgeResult::Ignored);
        }
        if sequence > in_flight.sequence {
            return Err(Status::invalid_argument(format!(
                "Logical error, unexpected do_exchange ACK sequence {}, expected {}",
                sequence, in_flight.sequence
            )));
        }
        if in_flight.kind == InFlightKind::EndOfStream {
            state.in_flight = None;
            return Ok(AcknowledgeResult::EndOfStream);
        }

        let Some(data) = pop_pending() else {
            state.in_flight = None;
            return Ok(AcknowledgeResult::Data);
        };

        let sequence = state.next_sequence;
        let encoded = ExchangePacket::data(sequence, data).encode();
        state.next_sequence += 1;
        state.in_flight = Some(InFlightPacket {
            kind: InFlightKind::Data,
            sequence,
            data: encoded.clone(),
        });

        let Some(send_tx) = &state.send_tx else {
            return Ok(AcknowledgeResult::Data);
        };
        match send_tx.try_send(encoded) {
            Ok(()) => Ok(AcknowledgeResult::Data),
            Err(TrySendError::Closed(_)) => Ok(AcknowledgeResult::Data),
            Err(TrySendError::Full(_)) => Err(Status::resource_exhausted(
                "do_exchange request channel is unexpectedly full after an acknowledgement",
            )),
        }
    }

    #[cfg(test)]
    pub fn ready_send(&self) {
        self.send_state.lock().in_flight = None;
    }

    fn has_in_flight(&self) -> bool {
        self.send_state.lock().in_flight.is_some()
    }

    pub fn request_finish(&self) {
        {
            let mut state = self.send_state.lock();
            if state.terminal.is_some() {
                return;
            }
            state.finishing = true;
        }
        self.finish.notify_waiters();
    }

    fn send_end_of_stream(&self) -> std::result::Result<(), Status> {
        let mut state = self.send_state.lock();
        if let Some(terminal) = &state.terminal {
            return terminal.clone();
        }
        if state.in_flight.is_some() {
            return Ok(());
        }
        if !state.finishing {
            return Err(Status::failed_precondition(
                "cannot end do_exchange before producer completion",
            ));
        }

        let sequence = state.next_sequence;
        let encoded = ExchangePacket::end_of_stream(sequence).encode();
        state.next_sequence += 1;
        state.in_flight = Some(InFlightPacket {
            kind: InFlightKind::EndOfStream,
            sequence,
            data: encoded.clone(),
        });

        let Some(send_tx) = &state.send_tx else {
            return Err(Status::unavailable(
                "do_exchange transport is unavailable while sending EndOfStream",
            ));
        };
        match send_tx.try_send(encoded) {
            Ok(()) | Err(TrySendError::Closed(_)) => Ok(()),
            Err(TrySendError::Full(_)) => Err(Status::resource_exhausted(
                "do_exchange request channel is unexpectedly full while sending EndOfStream",
            )),
        }
    }

    fn disconnect(&self) {
        self.send_state.lock().send_tx = None;
    }

    fn install_transport(&self, send_tx: Sender<FlightData>) -> std::result::Result<(), Status> {
        let mut state = self.send_state.lock();
        if state.terminal.is_some() {
            send_tx.close();
            return Ok(());
        }
        if let Some(in_flight) = &state.in_flight {
            match send_tx.try_send(in_flight.data.clone()) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    return Err(Status::resource_exhausted(
                        "new do_exchange transport is unexpectedly full",
                    ));
                }
                Err(TrySendError::Closed(_)) => {
                    return Err(Status::unavailable(
                        "new do_exchange transport closed before resend",
                    ));
                }
            }
        }
        state.send_tx = Some(send_tx);
        Ok(())
    }

    async fn reconnect(
        &self,
        connector: Option<&PingPongConnector>,
        status: Status,
        retry_times: u64,
        retry_interval: Duration,
    ) -> ReconnectOutcome {
        let Some(connector) = connector else {
            return ReconnectOutcome::Failed(status);
        };

        if !is_retryable_status(&status) || retry_times == 0 {
            return ReconnectOutcome::Failed(status);
        }

        self.disconnect();
        let mut last_error = status.to_string();

        for attempt in 0..retry_times {
            if attempt > 0 && !retry_interval.is_zero() {
                tokio::select! {
                    _ = self.shutdown.notified() => return ReconnectOutcome::Aborted,
                    _ = tokio::time::sleep(retry_interval) => {}
                }
            }

            let transport = tokio::select! {
                _ = self.shutdown.notified() => return ReconnectOutcome::Aborted,
                result = connector() => result,
            };

            match transport {
                Ok(transport) => {
                    if let Err(status) = self.install_transport(transport.send_tx) {
                        last_error = status.to_string();
                        continue;
                    }
                    return ReconnectOutcome::Reconnected(transport.response_stream);
                }
                Err(error) if error.code() == ErrorCode::CLOSED_QUERY => {
                    return ReconnectOutcome::RemoteFinished;
                }
                Err(error) => last_error = error.to_string(),
            }
        }

        ReconnectOutcome::Failed(Status::unavailable(format!(
            "do_exchange reconnect exhausted: {}",
            last_error
        )))
    }

    fn finish_remote(&self) {
        let mut state = self.send_state.lock();
        state.send_tx = None;
        state.in_flight = None;
        state.terminal = Some(Ok(()));
    }

    fn fail(&self, status: Status) {
        let mut state = self.send_state.lock();
        state.send_tx = None;
        state.in_flight = None;
        state.terminal = Some(Err(status));
    }

    fn abort(&self) {
        let mut state = self.send_state.lock();
        state.send_tx = None;
        state.in_flight = None;
        state.terminal = Some(Err(Status::cancelled("do_exchange was cancelled")));
    }
}

impl PingPongExchange {
    #[cfg(test)]
    pub fn from_stream(
        num_threads: usize,
        send_tx: async_channel::Sender<FlightData>,
        stream: impl futures::Stream<Item = std::result::Result<FlightData, Status>> + Send + 'static,
        local_node_id: impl Into<String>,
        remote_node_id: impl Into<String>,
    ) -> Self {
        let mut next_ack = 0;
        let stream = stream.map(move |response| {
            response.map(|_| {
                let ack = ExchangePacket::ack(next_ack).encode();
                next_ack += 1;
                ack
            })
        });
        let inner = Arc::new(PingPongExchangeInner {
            send_state: Mutex::new(SendState {
                send_tx: Some(send_tx),
                in_flight: None,
                next_sequence: 0,
                finishing: false,
                terminal: None,
            }),
            local_node_id: local_node_id.into(),
            remote_node_id: remote_node_id.into(),
            finish: WatchNotify::new(),
            shutdown: WatchNotify::new(),
        });

        Self {
            inner,
            num_threads,
            response_stream: Mutex::new(Some(Box::pin(stream))),
            connector: None,
            retry_times: 0,
            retry_interval: Duration::ZERO,
        }
    }

    pub fn from_reconnectable(
        num_threads: usize,
        transport: PingPongTransport,
        connector: PingPongConnector,
        retry_times: u64,
        retry_interval: Duration,
        local_node_id: String,
        remote_node_id: String,
    ) -> Self {
        let inner = Arc::new(PingPongExchangeInner {
            send_state: Mutex::new(SendState {
                send_tx: Some(transport.send_tx),
                in_flight: None,
                next_sequence: 0,
                finishing: false,
                terminal: None,
            }),
            local_node_id,
            remote_node_id,
            finish: WatchNotify::new(),
            shutdown: WatchNotify::new(),
        });
        Self {
            inner,
            num_threads,
            response_stream: Mutex::new(Some(transport.response_stream)),
            connector: Some(connector),
            retry_times,
            retry_interval,
        }
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
    ) -> std::result::Result<JoinHandle<()>, Status> {
        let Some(mut stream) = self.response_stream.lock().take() else {
            return Err(Status::already_exists("Receiver already started"));
        };

        let inner = self.inner.clone();
        let connector = self.connector.clone();
        let retry_times = self.retry_times;
        let retry_interval = self.retry_interval;
        Ok(runtime.spawn(async move {
            enum Event {
                Abort,
                Finish,
                Response(Option<std::result::Result<FlightData, Status>>),
            }

            let mut finishing = false;

            loop {
                if finishing && !inner.has_in_flight() {
                    if let Err(status) = inner.send_end_of_stream() {
                        inner.fail(status.clone());
                        callback.on_error(status);
                        break;
                    }
                }

                let event = if finishing {
                    tokio::select! {
                        _ = inner.shutdown.notified() => Event::Abort,
                        response = stream.next() => Event::Response(response),
                    }
                } else {
                    tokio::select! {
                        _ = inner.shutdown.notified() => Event::Abort,
                        _ = inner.finish.notified() => Event::Finish,
                        response = stream.next() => Event::Response(response),
                    }
                };

                match event {
                    Event::Abort => {
                        inner.abort();
                        break;
                    }
                    Event::Finish => finishing = true,
                    Event::Response(None) => {
                        inner.finish_remote();
                        callback.on_remote_finished();
                        break;
                    }
                    Event::Response(Some(Ok(data))) => {
                        let sequence = match ExchangePacket::decode(data) {
                            Ok(ExchangePacket::Ack { sequence }) => sequence,
                            Ok(ExchangePacket::Data { .. })
                            | Ok(ExchangePacket::EndOfStream { .. }) => {
                                let status: Status = ErrorCode::Internal(
                                    "Logical error, received a request packet on the do_exchange response stream",
                                )
                                .into();
                                inner.fail(status.clone());
                                callback.on_error(status);
                                break;
                            }
                            Err(cause) => {
                                let status: Status = cause.into();
                                inner.fail(status.clone());
                                callback.on_error(status);
                                break;
                            }
                        };
                        match inner.acknowledge_and_advance(sequence, || callback.pop_pending()) {
                            Ok(AcknowledgeResult::Ignored) => continue,
                            Err(status) => {
                                inner.fail(status.clone());
                                callback.on_error(status);
                                break;
                            }
                            Ok(AcknowledgeResult::Data) => {}
                            Ok(AcknowledgeResult::EndOfStream) => {
                                inner.finish_remote();
                                callback.on_remote_finished();
                                break;
                            }
                        }
                    }
                    Event::Response(Some(Err(status))) => {
                        match inner
                            .reconnect(
                                connector.as_ref(),
                                status,
                                retry_times,
                                retry_interval,
                            )
                            .await
                        {
                            ReconnectOutcome::Reconnected(response_stream) => {
                                stream = response_stream;
                            }
                            ReconnectOutcome::RemoteFinished => {
                                inner.finish_remote();
                                callback.on_remote_finished();
                                break;
                            }
                            ReconnectOutcome::Aborted => {
                                inner.abort();
                                break;
                            }
                            ReconnectOutcome::Failed(status) => {
                                inner.fail(status.clone());
                                callback.on_error(status);
                                break;
                            }
                        }
                    }
                }
            }
        }))
    }
}

fn is_retryable_status(status: &Status) -> bool {
    status.details().is_empty()
        && matches!(
            status.code(),
            tonic::Code::Cancelled
                | tonic::Code::Unknown
                | tonic::Code::DeadlineExceeded
                | tonic::Code::Internal
                | tonic::Code::Unavailable
        )
}

#[cfg(test)]
mod tests {
    use arrow_flight::FlightData;
    use tonic::Status;

    use super::*;

    struct NoPendingCallback;

    impl PingPongCallback for NoPendingCallback {
        fn pop_pending(&self) -> Option<FlightData> {
            None
        }

        fn on_remote_finished(&self) {}

        fn on_error(&self, _status: Status) {}
    }

    fn create_mock_exchange(
        num_threads: usize,
    ) -> (
        PingPongExchange,
        async_channel::Receiver<FlightData>,
        async_channel::Sender<std::result::Result<FlightData, Status>>,
    ) {
        let (send_tx, send_rx) = async_channel::bounded(1);
        let (pong_tx, pong_rx) = async_channel::unbounded();
        let exchange = PingPongExchange::from_stream(
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
        assert!(exchange.try_send(make_flight_data(10)).unwrap().is_none());

        // Data should arrive on send_rx
        let received = send_rx.recv().await.unwrap();
        assert_eq!(received.data_body.len(), 10);

        // Simulate pong by clearing in_flight
        exchange.ready_send();

        // Second send should succeed
        assert!(exchange.try_send(make_flight_data(20)).unwrap().is_none());
        let received = send_rx.recv().await.unwrap();
        assert_eq!(received.data_body.len(), 20);
    }

    #[tokio::test]
    async fn test_ping_pong_in_flight_returns_data() {
        let (exchange, send_rx, _pong_tx) = create_mock_exchange(2);

        // First send succeeds
        assert!(exchange.try_send(make_flight_data(1)).unwrap().is_none());

        // Second send returns data back (in-flight)
        let returned = exchange.try_send(make_flight_data(2)).unwrap();
        assert!(returned.is_some());
        assert_eq!(returned.unwrap().data_body.len(), 2);

        // Drain the channel and simulate pong
        let _ = send_rx.recv().await.unwrap();
        exchange.ready_send();

        // Now send should succeed again
        assert!(exchange.try_send(make_flight_data(3)).unwrap().is_none());
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
}
