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
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::FlightData;
use async_channel::Receiver;
use async_channel::Sender;
use async_channel::TrySendError;
use databend_common_base::base::WatchNotify;
use databend_common_base::runtime::Runtime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use futures::StreamExt;
use futures::stream::BoxStream;
use log::warn;
use parking_lot::Mutex;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tonic::Status;

use super::DoExchangeRequest;
use super::DoExchangeResponse;
use super::FlightConnectionAttempts;
use super::FlightReconnectPolicy;
use crate::servers::flight::FlightOperation;
use crate::servers::flight::add_flight_error_context;
use crate::servers::flight::v1::transport::OutboundStream;
use crate::servers::flight::v1::transport::StreamSendOutcome;
use crate::servers::flight::v1::transport::batch;
use crate::servers::flight::v1::transport::frame_lane;

type FlightDataStream = BoxStream<'static, std::result::Result<FlightData, Status>>;

pub struct DoExchangeTransport {
    pub send_tx: Sender<FlightData>,
    pub response_stream: FlightDataStream,
}

pub type DoExchangeConnector = Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = Result<DoExchangeTransport>> + Send>> + Send + Sync,
>;

/// A queued DATA packet holds its permit until it becomes the next in-flight request.
struct PendingPacket {
    data: FlightData,
    _permit: OwnedSemaphorePermit,
}

/// The sole request awaiting a response, retained for reconnect replay.
struct InFlightPacket {
    encoded: FlightData,
    expected: ExpectedResponse,
    // A replacement transport consumes this budget; only logical progress installs a fresh one.
    reconnect_attempts: FlightConnectionAttempts,
}

enum ExpectedResponse {
    Ack(u64),
    ReceiverClosed,
}

#[derive(Clone)]
enum OutboundTerminal {
    ReceiverClosed,
    Failed(ErrorCode),
}

impl OutboundTerminal {
    fn into_send_result(self) -> Result<StreamSendOutcome> {
        match self {
            Self::ReceiverClosed => Ok(StreamSendOutcome::ConsumerClosed),
            Self::Failed(cause) => Err(cause),
        }
    }
}

struct CompletionState {
    terminal: Mutex<Option<OutboundTerminal>>,
    notified: WatchNotify,
}

impl CompletionState {
    fn new() -> Self {
        Self {
            terminal: Mutex::new(None),
            notified: WatchNotify::new(),
        }
    }

    fn current(&self) -> Option<Result<StreamSendOutcome>> {
        self.terminal
            .lock()
            .clone()
            .map(OutboundTerminal::into_send_result)
    }

    fn complete(&self, terminal: OutboundTerminal) {
        let mut current = self.terminal.lock();
        if current.is_some() {
            return;
        }
        *current = Some(terminal);
        drop(current);
        self.notified.notify_waiters();
    }

    async fn wait(&self) -> Result<StreamSendOutcome> {
        if let Some(result) = self.current() {
            return result;
        }
        self.notified.notified().await;
        self.current()
            .expect("do_exchange completion must publish a terminal state")
    }
}

enum OutboundCommand {
    Data {
        channel: usize,
        data: FlightData,
        permit: OwnedSemaphorePermit,
    },
    Finish,
    SenderFail(ErrorCode),
}

struct OutboundDriver {
    physical: PhysicalConnection,
    commands: Receiver<OutboundCommand>,
    cancellation: CancellationToken,
    logical: LogicalConnection,
}

struct PhysicalConnection {
    transport: Option<DoExchangeTransport>,
    connector: DoExchangeConnector,
    reconnect: FlightReconnectPolicy,
    local_node_id: String,
    remote_node_id: String,
}

struct LogicalConnection {
    close: Option<CloseIntent>,
    in_flight: Option<InFlightPacket>,
    next_sequence: u64,
    pending: Vec<VecDeque<PendingPacket>>,
    max_batch_bytes: Option<usize>,
}

enum CloseIntent {
    /// Producer input ended normally; drain accepted DATA before FINISH.
    Finish,
    /// Producer failed; discard unsent DATA and preserve this error during terminal cleanup.
    SenderFail(ErrorCode),
}

pub struct ReliableOutbound {
    num_threads: usize,
    commands: Sender<OutboundCommand>,
    slots: Arc<Semaphore>,
    cancellation: CancellationToken,
    completion: Arc<CompletionState>,
}

pub struct PendingReliableOutbound {
    num_threads: usize,
    physical: PhysicalConnection,
}

impl PendingReliableOutbound {
    pub async fn connect(
        num_threads: usize,
        connector: DoExchangeConnector,
        reconnect: FlightReconnectPolicy,
        local_node_id: String,
        remote_node_id: String,
    ) -> Result<Self> {
        Ok(Self {
            num_threads,
            physical: PhysicalConnection::open(connector, reconnect, local_node_id, remote_node_id)
                .await?,
        })
    }
    pub fn start(
        self,
        slots: Arc<Semaphore>,
        max_batch_bytes: Option<usize>,
        runtime: &Runtime,
    ) -> ReliableOutbound {
        let (commands_tx, commands_rx) = async_channel::unbounded();
        let cancellation = CancellationToken::new();
        let completion = Arc::new(CompletionState::new());
        let driver = OutboundDriver {
            physical: self.physical,
            commands: commands_rx,
            cancellation: cancellation.clone(),
            logical: LogicalConnection {
                close: None,
                in_flight: None,
                next_sequence: 0,
                pending: (0..self.num_threads).map(|_| VecDeque::new()).collect(),
                max_batch_bytes,
            },
        };
        let task_completion = completion.clone();
        runtime.spawn(async move {
            task_completion.complete(driver.run().await);
        });

        ReliableOutbound {
            num_threads: self.num_threads,
            commands: commands_tx,
            slots,
            cancellation,
            completion,
        }
    }
}

#[async_trait::async_trait]
impl OutboundStream for ReliableOutbound {
    async fn send(&self, lane: usize, data: FlightData) -> Result<StreamSendOutcome> {
        debug_assert!(lane < self.num_threads, "too many channels");
        let data = frame_lane(lane, data)?;
        if let Some(result) = self.completion.current() {
            return result;
        }
        let permit = tokio::select! {
            permit = self.slots.clone().acquire_owned() => permit.unwrap(),
            result = self.completion.wait() => return result,
        };
        if let Some(result) = self.completion.current() {
            return result;
        }
        if self
            .commands
            .send(OutboundCommand::Data {
                channel: lane,
                data,
                permit,
            })
            .await
            .is_err()
        {
            return self.completion.wait().await;
        }
        Ok(StreamSendOutcome::Accepted)
    }

    /// Drains all accepted DATA before sending FINISH, then waits for ReceiverClosed.
    async fn finish(&self) -> Result<()> {
        if self.completion.current().is_none() {
            let _ = self.commands.send(OutboundCommand::Finish).await;
        }
        self.completion.wait().await.map(|_| ())
    }

    /// Keeps the transport alive for the bounded failure handshake. This is cleanup and must not
    /// delay returning the producer's original error to the query.
    async fn fail(&self, cause: ErrorCode) {
        if self.completion.current().is_none() {
            let _ = self.commands.send(OutboundCommand::SenderFail(cause)).await;
        }
        let _ = self.completion.wait().await;
    }

    fn abort(&self) {
        self.cancellation.cancel();
    }

    fn is_closed(&self) -> bool {
        self.completion.current().is_some()
    }
}

impl Drop for ReliableOutbound {
    fn drop(&mut self) {
        OutboundStream::abort(self);
    }
}

impl OutboundDriver {
    async fn run(mut self) -> OutboundTerminal {
        let terminal = self.drive().await;
        self.commands.close();
        match &self.logical.close {
            Some(CloseIntent::SenderFail(cause)) => OutboundTerminal::Failed(cause.clone()),
            _ => terminal,
        }
    }

    async fn drive(&mut self) -> OutboundTerminal {
        loop {
            enum Event {
                Cancelled,
                Command(std::result::Result<OutboundCommand, async_channel::RecvError>),
                Response(std::result::Result<FlightData, Status>),
            }

            let event = tokio::select! {
                _ = self.cancellation.cancelled() => Event::Cancelled,
                command = self.commands.recv() => Event::Command(command),
                response = self.physical.response_stream().next() => Event::Response(
                    response.unwrap_or_else(|| Err(Status::unavailable(
                        "do_exchange response stream ended before a terminal packet",
                    )))
                ),
            };

            match event {
                Event::Cancelled | Event::Command(Err(_)) => {
                    return self.failed(ErrorCode::AbortedQuery("do_exchange was cancelled"));
                }
                Event::Command(Ok(command)) => {
                    if let Err(cause) = self.handle_command(command) {
                        return self.failed(cause);
                    }
                }
                Event::Response(Ok(data)) => match DoExchangeResponse::decode(data) {
                    Ok(DoExchangeResponse::ReceiverClosed) => {
                        return OutboundTerminal::ReceiverClosed;
                    }
                    Ok(DoExchangeResponse::Fail(cause)) => {
                        return self.failed(cause);
                    }
                    Ok(DoExchangeResponse::Ack { sequence }) => {
                        if let Err(cause) = self.acknowledge(sequence) {
                            return self.failed(cause);
                        }
                    }
                    Err(cause) => {
                        return self.failed(cause);
                    }
                },
                Event::Response(Err(status)) => {
                    if let Err(cause) = self.reconnect_transport(status).await {
                        return OutboundTerminal::Failed(cause);
                    }
                }
            }
        }
    }

    fn handle_command(&mut self, command: OutboundCommand) -> Result<()> {
        match command {
            OutboundCommand::Data {
                channel,
                data,
                permit,
            } => {
                match &self.logical.close {
                    Some(CloseIntent::SenderFail(_)) => return Ok(()),
                    Some(CloseIntent::Finish) => {
                        return Err(ErrorCode::Internal(
                            "cannot send data after do_exchange producer completion",
                        ));
                    }
                    None => {}
                }
                if self.logical.in_flight.is_none() {
                    self.install_data(data);
                } else {
                    self.logical.pending[channel].push_back(PendingPacket {
                        data,
                        _permit: permit,
                    });
                }
            }
            OutboundCommand::Finish => {
                if self.logical.close.is_none() {
                    self.logical.close = Some(CloseIntent::Finish);
                }
                self.send_next();
            }
            OutboundCommand::SenderFail(cause) => {
                if !matches!(&self.logical.close, Some(CloseIntent::SenderFail(_))) {
                    self.logical.close = Some(CloseIntent::SenderFail(cause));
                }
                for channel in &mut self.logical.pending {
                    channel.clear();
                }
                self.send_next();
            }
        }
        Ok(())
    }

    fn acknowledge(&mut self, sequence: u64) -> Result<()> {
        match self.logical.in_flight.take() {
            Some(InFlightPacket {
                expected: ExpectedResponse::Ack(expected),
                ..
            }) if sequence == expected => {
                self.send_next();
                Ok(())
            }
            Some(InFlightPacket {
                expected: ExpectedResponse::Ack(expected),
                ..
            }) => Err(ErrorCode::Internal(format!(
                "received do_exchange ACK for sequence {}, expected {}",
                sequence, expected
            ))),
            Some(InFlightPacket {
                expected: ExpectedResponse::ReceiverClosed,
                ..
            }) => Err(ErrorCode::Internal(
                "received do_exchange ACK while waiting for ReceiverClosed",
            )),
            None => Err(ErrorCode::Internal(
                "received do_exchange ACK without an in-flight request",
            )),
        }
    }

    fn send_next(&mut self) {
        if self.logical.in_flight.is_some() {
            return;
        }
        if let Some(CloseIntent::SenderFail(cause)) = &self.logical.close {
            self.install_sender_fail(cause.clone());
            return;
        }
        if let Some(data) = pop_pending(&mut self.logical.pending, self.logical.max_batch_bytes) {
            self.install_data(data);
            return;
        }
        if matches!(&self.logical.close, Some(CloseIntent::Finish)) {
            self.install_finish();
        }
    }

    fn install_data(&mut self, data: FlightData) {
        let sequence = self.logical.next_sequence;
        self.logical.next_sequence += 1;
        self.install_request(
            DoExchangeRequest::data(sequence, data),
            ExpectedResponse::Ack(sequence),
        );
    }

    fn install_finish(&mut self) {
        self.install_request(
            DoExchangeRequest::finish(),
            ExpectedResponse::ReceiverClosed,
        );
    }

    fn install_sender_fail(&mut self, cause: ErrorCode) {
        self.install_request(
            DoExchangeRequest::sender_fail(cause),
            ExpectedResponse::ReceiverClosed,
        );
    }

    fn install_request(&mut self, request: DoExchangeRequest, expected: ExpectedResponse) {
        let encoded = request.encode();
        self.physical.send(&encoded);
        self.logical.in_flight = Some(InFlightPacket {
            encoded,
            expected,
            reconnect_attempts: self.physical.reconnect.reconnect_attempts(),
        });
    }

    async fn reconnect_transport(&mut self, status: Status) -> Result<()> {
        let (replay, attempts) = match &self.logical.in_flight {
            Some(packet) => (Some(packet.encoded.clone()), packet.reconnect_attempts),
            None => (None, self.physical.reconnect.reconnect_attempts()),
        };
        let attempts_used = self
            .physical
            .reconnect(status, replay, attempts, &self.cancellation)
            .await?;
        if let Some(packet) = &mut self.logical.in_flight {
            packet.reconnect_attempts = packet.reconnect_attempts.consume(attempts_used);
        }
        Ok(())
    }

    fn failed(&self, cause: ErrorCode) -> OutboundTerminal {
        let cause = self.physical.contextualize(cause);
        self.physical.warn_failure(&cause);
        OutboundTerminal::Failed(cause)
    }
}

impl PhysicalConnection {
    async fn open(
        connector: DoExchangeConnector,
        reconnect: FlightReconnectPolicy,
        local_node_id: String,
        remote_node_id: String,
    ) -> Result<Self> {
        let mut connection = Self {
            transport: None,
            connector,
            reconnect,
            local_node_id,
            remote_node_id,
        };
        let attempts = reconnect.initial_attempts();
        let (transport, _) = connection.establish(None, attempts).await?;
        connection.transport = Some(transport);
        Ok(connection)
    }

    fn response_stream(&mut self) -> &mut FlightDataStream {
        &mut self
            .transport
            .as_mut()
            .expect("driver must reconnect before polling a transport")
            .response_stream
    }

    fn send(&self, encoded: &FlightData) {
        let send_tx = &self
            .transport
            .as_ref()
            .expect("sending requires a physical transport")
            .send_tx;
        match send_tx.try_send(encoded.clone()) {
            Ok(()) | Err(TrySendError::Closed(_)) => {}
            Err(TrySendError::Full(_)) => {
                unreachable!("stop-and-wait request channel unexpectedly full")
            }
        }
    }

    async fn reconnect(
        &mut self,
        status: Status,
        replay: Option<FlightData>,
        attempts: FlightConnectionAttempts,
        cancellation: &CancellationToken,
    ) -> Result<u64> {
        self.transport = None;
        if !is_retryable_status(&status) || attempts.is_empty() {
            let cause = status_to_error(status, &self.local_node_id, &self.remote_node_id);
            self.warn_failure(&cause);
            return Err(cause);
        }

        let cancelled = self.contextualize(ErrorCode::AbortedQuery("do_exchange was cancelled"));
        let establish = self.establish(replay, attempts);
        let (transport, attempts_used) = tokio::select! {
            _ = cancellation.cancelled() => {
                self.warn_failure(&cancelled);
                return Err(cancelled);
            }
            result = establish => result,
        }?;
        warn!(
            "do_exchange sender reconnected: client={}, service={}, attempts={}, initial_status={}",
            self.local_node_id, self.remote_node_id, attempts_used, status
        );
        self.transport = Some(transport);
        Ok(attempts_used)
    }

    fn establish(
        &self,
        replay: Option<FlightData>,
        attempts: FlightConnectionAttempts,
    ) -> impl Future<Output = Result<(DoExchangeTransport, u64)>> + Send + 'static {
        let connector = self.connector.clone();
        let reconnect = self.reconnect;
        let local_node_id = self.local_node_id.clone();
        let remote_node_id = self.remote_node_id.clone();

        async move {
            let attempts = attempts.remaining();
            for attempt in 0..attempts {
                if attempt > 0 {
                    tokio::time::sleep(reconnect.retry_interval).await;
                }

                let failure = match tokio::time::timeout(reconnect.timeout, (connector)()).await {
                    Ok(Ok(transport)) => {
                        let Some(encoded) = &replay else {
                            return Ok((transport, attempt + 1));
                        };
                        match transport.send_tx.try_send(encoded.clone()) {
                            Ok(()) => return Ok((transport, attempt + 1)),
                            Err(TrySendError::Full(_)) => unreachable!(
                                "new do_exchange request channel cannot be full before replay"
                            ),
                            Err(TrySendError::Closed(_)) => status_to_error(
                                Status::unavailable(
                                    "new do_exchange transport closed before replay",
                                ),
                                &local_node_id,
                                &remote_node_id,
                            ),
                        }
                    }
                    Ok(Err(cause)) if cause.code() == ErrorCode::CANNOT_CONNECT_NODE => cause,
                    Ok(Err(cause)) => {
                        warn!(
                            "do_exchange connection attempt failed: client={}, service={}, attempt={}/{}, error={}",
                            local_node_id,
                            remote_node_id,
                            attempt + 1,
                            attempts,
                            cause
                        );
                        return Err(cause);
                    }
                    Err(_) => status_to_error(
                        Status::deadline_exceeded(format!(
                            "connection attempt {}/{} exceeded its {:?} deadline",
                            attempt + 1,
                            attempts,
                            reconnect.timeout
                        )),
                        &local_node_id,
                        &remote_node_id,
                    ),
                };

                warn!(
                    "do_exchange connection attempt failed: client={}, service={}, attempt={}/{}, error={}",
                    local_node_id,
                    remote_node_id,
                    attempt + 1,
                    attempts,
                    failure
                );

                if attempt + 1 == attempts {
                    return Err(failure.add_message_back(format!(
                        "do_exchange connection exhausted after {} attempts",
                        attempts
                    )));
                }
            }

            unreachable!("establish returns from its final attempt")
        }
    }

    fn contextualize(&self, cause: ErrorCode) -> ErrorCode {
        add_flight_error_context(
            cause,
            FlightOperation::DoExchange,
            &self.local_node_id,
            &self.remote_node_id,
        )
    }

    fn warn_failure(&self, cause: &ErrorCode) {
        warn!(
            "do_exchange sender failed: client={}, service={}, error={}",
            self.local_node_id, self.remote_node_id, cause
        );
    }
}

fn status_to_error(status: Status, local_node_id: &str, remote_node_id: &str) -> ErrorCode {
    let cause = if status.code() == tonic::Code::Aborted {
        ErrorCode::AbortedQuery(status.message())
    } else {
        status.into()
    };
    add_flight_error_context(
        cause,
        FlightOperation::DoExchange,
        local_node_id,
        remote_node_id,
    )
}

fn pop_pending(
    pending: &mut [VecDeque<PendingPacket>],
    max_batch_bytes: Option<usize>,
) -> Option<FlightData> {
    let channel = pending.iter_mut().max_by_key(|channel| channel.len())?;
    let first = channel.pop_front()?;
    let Some(max_batch_bytes) = max_batch_bytes else {
        return Some(first.data);
    };

    let mut total = first.data.data_body.len();
    let mut items = vec![first.data];
    while total < max_batch_bytes {
        let Some(next) = channel.pop_front() else {
            break;
        };
        total += next.data.data_body.len();
        items.push(next.data);
    }
    if items.len() == 1 {
        return items.pop();
    }
    Some(batch::merge(items))
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
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use super::*;
    use crate::servers::flight::v1::transport::DeliveryOutcome;
    use crate::servers::flight::v1::transport::InboundDelivery;
    use crate::servers::flight::v1::transport::reliable::ReliableInboundSource;

    struct TestDelivery {
        sender: async_channel::Sender<FlightData>,
        terminal: Mutex<Option<Option<ErrorCode>>>,
    }

    impl TestDelivery {
        fn create() -> (Arc<Self>, async_channel::Receiver<FlightData>) {
            let (sender, receiver) = async_channel::unbounded();
            (
                Arc::new(Self {
                    sender,
                    terminal: Mutex::new(None),
                }),
                receiver,
            )
        }
    }

    #[async_trait::async_trait]
    impl InboundDelivery for TestDelivery {
        async fn deliver(&self, _lane: usize, data: FlightData) -> Result<DeliveryOutcome> {
            self.sender
                .send(data)
                .await
                .map(|_| DeliveryOutcome::Accepted)
                .map_err(|_| ErrorCode::AbortedQuery("test delivery closed"))
        }

        fn is_closed(&self) -> bool {
            self.sender.is_closed()
        }

        fn consumer_closed(&self) -> Option<futures::future::BoxFuture<'static, ()>> {
            None
        }

        fn terminate(&self, cause: Option<ErrorCode>) {
            *self.terminal.lock() = Some(cause);
            self.sender.close();
        }
    }

    fn reconnect_policy(retry_times: u64) -> FlightReconnectPolicy {
        FlightReconnectPolicy::new(retry_times, Duration::ZERO, Duration::from_secs(1))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_reconnect_replays_unacknowledged_data_once() {
        let runtime = Arc::new(Runtime::with_worker_threads(1, None).unwrap());
        let (delivery, receiver) = TestDelivery::create();
        let source = Arc::new(ReliableInboundSource::new(
            delivery,
            Duration::from_secs(60),
            "test source".to_string(),
        ));
        let connector_calls = Arc::new(AtomicUsize::new(0));

        let connector: DoExchangeConnector = {
            let source = source.clone();
            let runtime = runtime.clone();
            let connector_calls = connector_calls.clone();
            Arc::new(move || {
                let source = source.clone();
                let runtime = runtime.clone();
                let connection_index = connector_calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    let (send_tx, send_rx) = async_channel::bounded(1);
                    let (response_tx, response_rx) = async_channel::unbounded();
                    let connection = source.connect(
                        runtime,
                        ErrorCode::CannotConnectNode("mock connection was lost"),
                    );

                    databend_common_base::runtime::spawn(async move {
                        while let Ok(encoded) = send_rx.recv().await {
                            let request = DoExchangeRequest::decode(encoded).unwrap();
                            let response = connection.handle_request(request).await.unwrap();

                            if connection_index == 0 {
                                // The receiver accepted DATA, but its ACK was lost with the stream.
                                return;
                            }

                            let terminal = matches!(
                                &response,
                                DoExchangeResponse::ReceiverClosed | DoExchangeResponse::Fail(_)
                            );
                            response_tx.send(Ok(response.encode())).await.unwrap();
                            if terminal {
                                return;
                            }
                        }
                    });

                    Ok(DoExchangeTransport {
                        send_tx,
                        response_stream: response_rx.boxed(),
                    })
                })
            })
        };

        let pending = PendingReliableOutbound::connect(
            1,
            connector,
            reconnect_policy(2),
            "sender".to_string(),
            "receiver".to_string(),
        )
        .await
        .unwrap();
        let outbound = pending.start(Arc::new(Semaphore::new(8)), None, &runtime);
        let payload = FlightData {
            app_metadata: vec![0, 0].into(),
            data_body: vec![1, 2, 3].into(),
            ..Default::default()
        };

        assert_eq!(
            outbound.send(0, payload.clone()).await.unwrap(),
            StreamSendOutcome::Accepted
        );
        outbound.finish().await.unwrap();

        let received = receiver.recv().await.unwrap();
        assert_eq!(received, payload);
        assert!(receiver.recv().await.is_err());
        assert_eq!(connector_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_sender_fail_preserves_original_error() {
        let runtime = Arc::new(Runtime::with_worker_threads(1, None).unwrap());
        let (delivery, _receiver) = TestDelivery::create();
        let source = Arc::new(ReliableInboundSource::new(
            delivery.clone(),
            Duration::ZERO,
            "test source".to_string(),
        ));
        let connector: DoExchangeConnector = {
            let source = source.clone();
            let runtime = runtime.clone();
            Arc::new(move || {
                let source = source.clone();
                let runtime = runtime.clone();
                Box::pin(async move {
                    let (send_tx, send_rx) = async_channel::bounded(1);
                    let (response_tx, response_rx) = async_channel::unbounded();
                    let connection = source.connect(
                        runtime,
                        ErrorCode::CannotConnectNode("mock connection was lost"),
                    );

                    databend_common_base::runtime::spawn(async move {
                        while let Ok(encoded) = send_rx.recv().await {
                            let request = DoExchangeRequest::decode(encoded).unwrap();
                            let response = connection.handle_request(request).await.unwrap();
                            let terminal = matches!(
                                &response,
                                DoExchangeResponse::ReceiverClosed | DoExchangeResponse::Fail(_)
                            );
                            response_tx.send(Ok(response.encode())).await.unwrap();
                            if terminal {
                                return;
                            }
                        }
                    });

                    Ok(DoExchangeTransport {
                        send_tx,
                        response_stream: response_rx.boxed(),
                    })
                })
            })
        };

        let pending = PendingReliableOutbound::connect(
            1,
            connector,
            reconnect_policy(0),
            "sender".to_string(),
            "receiver".to_string(),
        )
        .await
        .unwrap();
        let outbound = pending.start(Arc::new(Semaphore::new(1)), None, &runtime);
        let cause = ErrorCode::AbortedQuery("statistics serialization failed");

        outbound.fail(cause.clone()).await;

        assert!(outbound.is_closed());
        let error = delivery
            .terminal
            .lock()
            .clone()
            .flatten()
            .expect("SENDER_FAIL must fail the receiver delivery");
        assert_eq!(error.code(), cause.code());
        assert_eq!(error.message(), cause.message());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_reconnect_budget_exhaustion_returns_error() {
        let runtime = Arc::new(Runtime::with_worker_threads(1, None).unwrap());
        let connector_calls = Arc::new(AtomicUsize::new(0));
        let connector: DoExchangeConnector = {
            let connector_calls = connector_calls.clone();
            Arc::new(move || {
                let connection_index = connector_calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    if connection_index == 0 {
                        let (send_tx, send_rx) = async_channel::bounded(1);
                        let (response_tx, response_rx) = async_channel::unbounded();
                        drop(send_rx);
                        drop(response_tx);
                        return Ok(DoExchangeTransport {
                            send_tx,
                            response_stream: response_rx.boxed(),
                        });
                    }

                    Err(ErrorCode::CannotConnectNode("mock service unavailable"))
                })
            })
        };

        let pending = PendingReliableOutbound::connect(
            1,
            connector,
            reconnect_policy(2),
            "sender".to_string(),
            "receiver".to_string(),
        )
        .await
        .unwrap();
        let outbound = pending.start(Arc::new(Semaphore::new(1)), None, &runtime);

        let error = outbound.finish().await.unwrap_err();
        assert_eq!(error.code(), ErrorCode::CANNOT_CONNECT_NODE);
        assert!(error.message().contains("exhausted after 2 attempts"));
        assert_eq!(connector_calls.load(Ordering::SeqCst), 3);
    }
}
