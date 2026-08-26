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
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SendOutcome {
    Accepted,
    ReceiverClosed,
}

#[derive(Clone)]
enum OutboundTerminal {
    ReceiverClosed,
    Failed(ErrorCode),
}

impl OutboundTerminal {
    fn into_send_result(self) -> Result<SendOutcome> {
        match self {
            Self::ReceiverClosed => Ok(SendOutcome::ReceiverClosed),
            Self::Failed(cause) => Err(cause),
        }
    }
}

struct OutboundCompletion {
    terminal: Mutex<Option<OutboundTerminal>>,
    notified: WatchNotify,
}

impl OutboundCompletion {
    fn new() -> Self {
        Self {
            terminal: Mutex::new(None),
            notified: WatchNotify::new(),
        }
    }

    fn current(&self) -> Option<Result<SendOutcome>> {
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

    async fn wait(&self) -> Result<SendOutcome> {
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

pub struct NetworkOutbound {
    num_threads: usize,
    commands: Sender<OutboundCommand>,
    slots: Arc<Semaphore>,
    cancellation: CancellationToken,
    completion: Arc<OutboundCompletion>,
}

pub(crate) struct PendingNetworkOutbound {
    num_threads: usize,
    physical: PhysicalConnection,
}

impl PendingNetworkOutbound {
    pub(crate) async fn connect(
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
    pub(crate) fn start(
        self,
        slots: Arc<Semaphore>,
        max_batch_bytes: Option<usize>,
        runtime: &Runtime,
    ) -> NetworkOutbound {
        let (commands_tx, commands_rx) = async_channel::unbounded();
        let cancellation = CancellationToken::new();
        let completion = Arc::new(OutboundCompletion::new());
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

        NetworkOutbound {
            num_threads: self.num_threads,
            commands: commands_tx,
            slots,
            cancellation,
            completion,
        }
    }
}

impl NetworkOutbound {
    pub async fn send(&self, channel: usize, data: FlightData) -> Result<SendOutcome> {
        debug_assert!(channel < self.num_threads, "too many channels");
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
                channel,
                data,
                permit,
            })
            .await
            .is_err()
        {
            return self.completion.wait().await;
        }
        Ok(SendOutcome::Accepted)
    }

    /// Drains all accepted DATA before sending FINISH, then waits for ReceiverClosed.
    pub async fn finish(&self) -> Result<()> {
        if self.completion.current().is_none() {
            let _ = self.commands.send(OutboundCommand::Finish).await;
        }
        self.completion.wait().await.map(|_| ())
    }

    /// Keeps the transport alive for the bounded failure handshake. This is cleanup and must not
    /// delay returning the producer's original error to the query.
    pub async fn fail(&self, cause: ErrorCode) {
        if self.completion.current().is_none() {
            let _ = self.commands.send(OutboundCommand::SenderFail(cause)).await;
        }
        let _ = self.completion.wait().await;
    }

    pub fn abort(&self) {
        self.cancellation.cancel();
    }

    pub fn is_closed(&self) -> bool {
        self.completion.current().is_some()
    }
}

impl Drop for NetworkOutbound {
    fn drop(&mut self) {
        self.abort();
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
        let next_sequence = sequence + 1;
        let encoded = DoExchangeRequest::data(sequence, data).encode();
        self.physical.send(&encoded, "sending DATA");
        self.logical.next_sequence = next_sequence;
        self.logical.in_flight = Some(InFlightPacket {
            encoded,
            expected: ExpectedResponse::Ack(sequence),
            reconnect_attempts: self.physical.reconnect.reconnect_attempts(),
        });
    }

    fn install_finish(&mut self) {
        let encoded = DoExchangeRequest::finish().encode();
        self.physical.send(&encoded, "sending FINISH");
        self.logical.in_flight = Some(InFlightPacket {
            encoded,
            expected: ExpectedResponse::ReceiverClosed,
            reconnect_attempts: self.physical.reconnect.reconnect_attempts(),
        });
    }

    fn install_sender_fail(&mut self, cause: ErrorCode) {
        let encoded = DoExchangeRequest::sender_fail(cause).encode();
        self.physical.send(&encoded, "sending SENDER_FAIL");
        self.logical.in_flight = Some(InFlightPacket {
            encoded,
            expected: ExpectedResponse::ReceiverClosed,
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

    fn send(&self, encoded: &FlightData, operation: &str) {
        let send_tx = &self
            .transport
            .as_ref()
            .expect("sending requires a physical transport")
            .send_tx;
        match send_tx.try_send(encoded.clone()) {
            Ok(()) | Err(TrySendError::Closed(_)) => {}
            Err(TrySendError::Full(_)) => unreachable!(
                "{operation}: stop-and-wait allows only one sender-owned in-flight request"
            ),
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
            let cause = self.status_error(status);
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

    fn status_error(&self, status: Status) -> ErrorCode {
        status_to_error(status, &self.local_node_id, &self.remote_node_id)
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
    Some(merge_flight_data_batch(items))
}

const BATCH_MARKER: u8 = 0x02;

fn merge_flight_data_batch(items: Vec<FlightData>) -> FlightData {
    let tid = [items[0].app_metadata[0], items[0].app_metadata[1]];
    let mut app_metadata = BytesMut::with_capacity(5);
    app_metadata.put_slice(&tid);
    app_metadata.put_u16_le(items.len() as u16);
    app_metadata.put_u8(BATCH_MARKER);

    let estimated = items
        .iter()
        .map(|item| {
            12 + item.app_metadata.len() - 2 + item.data_header.len() + item.data_body.len()
        })
        .sum();
    let mut body = BytesMut::with_capacity(estimated);
    for item in items {
        let metadata = &item.app_metadata[2..];
        body.put_u32_le(metadata.len() as u32);
        body.put_slice(metadata);
        body.put_u32_le(item.data_header.len() as u32);
        body.put_slice(&item.data_header);
        body.put_u32_le(item.data_body.len() as u32);
        body.put_slice(&item.data_body);
    }

    FlightData {
        flight_descriptor: None,
        app_metadata: app_metadata.freeze(),
        data_header: Bytes::new(),
        data_body: body.freeze(),
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
