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
use std::time::Duration;
use std::time::Instant;

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

struct PendingPacket {
    data: FlightData,
    _permit: OwnedSemaphorePermit,
}

enum InFlightPacket {
    Data {
        encoded: FlightData,
        sequence: u64,
        sent_at: Instant,
    },
    Finish {
        encoded: FlightData,
        sent_at: Instant,
    },
}

impl InFlightPacket {
    fn encoded(&self) -> &FlightData {
        match self {
            Self::Data { encoded, .. } | Self::Finish { encoded, .. } => encoded,
        }
    }

    fn reset_timer(&mut self) {
        match self {
            Self::Data { sent_at, .. } | Self::Finish { sent_at, .. } => {
                *sent_at = Instant::now();
            }
        }
    }

    fn deadline(&self, timeout: Duration) -> Option<tokio::time::Instant> {
        let sent_at = match self {
            Self::Data { sent_at, .. } | Self::Finish { sent_at, .. } => *sent_at,
        };
        sent_at
            .checked_add(timeout)
            .map(tokio::time::Instant::from_std)
    }
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

    fn current(&self) -> Option<OutboundTerminal> {
        self.terminal.lock().clone()
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

    async fn wait(&self) -> OutboundTerminal {
        if let Some(terminal) = self.current() {
            return terminal;
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
    finishing: bool,
    in_flight: Option<InFlightPacket>,
    next_sequence: u64,
    pending: Vec<VecDeque<PendingPacket>>,
    max_batch_bytes: Option<usize>,
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

    #[cfg(test)]
    fn create(
        num_threads: usize,
        transport: DoExchangeTransport,
        connector: DoExchangeConnector,
        reconnect: FlightReconnectPolicy,
        local_node_id: String,
        remote_node_id: String,
    ) -> Self {
        Self {
            num_threads,
            physical: PhysicalConnection {
                transport: Some(transport),
                connector,
                reconnect,
                local_node_id,
                remote_node_id,
            },
        }
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
                finishing: false,
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
        if let Some(terminal) = self.completion.current() {
            return self.send_outcome(terminal);
        }
        let permit = tokio::select! {
            permit = self.slots.clone().acquire_owned() => permit.unwrap(),
            terminal = self.completion.wait() => return self.send_outcome(terminal),
        };
        if let Some(terminal) = self.completion.current() {
            return self.send_outcome(terminal);
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
            return self.send_outcome(self.completion.wait().await);
        }
        Ok(SendOutcome::Accepted)
    }

    pub async fn finish(&self) -> Result<()> {
        if self.completion.current().is_none() {
            let _ = self.commands.send(OutboundCommand::Finish).await;
            self.commands.close();
        }
        match self.completion.wait().await {
            OutboundTerminal::ReceiverClosed => Ok(()),
            OutboundTerminal::Failed(cause) => Err(cause),
        }
    }

    pub fn abort(&self) {
        self.cancellation.cancel();
    }

    pub fn is_closed(&self) -> bool {
        self.completion.current().is_some()
    }

    fn send_outcome(&self, terminal: OutboundTerminal) -> Result<SendOutcome> {
        match terminal {
            OutboundTerminal::ReceiverClosed => Ok(SendOutcome::ReceiverClosed),
            OutboundTerminal::Failed(cause) => Err(cause),
        }
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
        if let OutboundTerminal::Failed(cause) = &terminal {
            self.physical.warn_failure(cause);
        }
        terminal
    }

    async fn drive(&mut self) -> OutboundTerminal {
        loop {
            let deadline = self
                .logical
                .in_flight
                .as_ref()
                .and_then(|packet| packet.deadline(self.physical.ack_timeout()));

            enum Event {
                Cancelled,
                Command(std::result::Result<OutboundCommand, async_channel::RecvError>),
                Response(std::result::Result<FlightData, Status>),
                AckTimeout,
            }

            let event = tokio::select! {
                _ = self.cancellation.cancelled() => Event::Cancelled,
                command = receive_command(&self.commands, self.logical.finishing) => Event::Command(command),
                response = self.physical.response_stream().next() => Event::Response(
                    response.unwrap_or_else(|| Err(Status::unavailable(
                        "do_exchange response stream ended before a terminal packet",
                    )))
                ),
                _ = wait_for_deadline(deadline) => Event::AckTimeout,
            };

            match event {
                Event::Cancelled | Event::Command(Err(_)) => {
                    return OutboundTerminal::Failed(
                        self.physical
                            .contextualize(ErrorCode::AbortedQuery("do_exchange was cancelled")),
                    );
                }
                Event::Command(Ok(command)) => {
                    if let Err(status) = self.handle_command(command) {
                        return OutboundTerminal::Failed(self.status_error(status));
                    }
                }
                Event::Response(Ok(data)) => match DoExchangeResponse::decode(data) {
                    Ok(DoExchangeResponse::ReceiverClosed) => {
                        return OutboundTerminal::ReceiverClosed;
                    }
                    Ok(DoExchangeResponse::Fail(cause)) => {
                        return OutboundTerminal::Failed(self.physical.contextualize(cause));
                    }
                    Ok(DoExchangeResponse::Ack { sequence }) => {
                        if let Err(status) = self.acknowledge(sequence) {
                            return OutboundTerminal::Failed(self.status_error(status));
                        }
                    }
                    Err(cause) => {
                        return OutboundTerminal::Failed(self.physical.contextualize(cause));
                    }
                },
                Event::Response(Err(status)) => {
                    if let Err(cause) = self.reconnect_transport(status).await {
                        return OutboundTerminal::Failed(cause);
                    }
                }
                Event::AckTimeout => {
                    let status = Status::deadline_exceeded(format!(
                        "do_exchange ACK exceeded its {:?} deadline",
                        self.physical.ack_timeout()
                    ));
                    if let Err(cause) = self.reconnect_transport(status).await {
                        return OutboundTerminal::Failed(cause);
                    }
                }
            }
        }
    }

    fn handle_command(&mut self, command: OutboundCommand) -> std::result::Result<(), Status> {
        match command {
            OutboundCommand::Data {
                channel,
                data,
                permit,
            } => {
                if self.logical.finishing {
                    return Err(Status::failed_precondition(
                        "cannot send data after do_exchange producer completion",
                    ));
                }
                if self.logical.in_flight.is_none() {
                    self.install_data(data)?;
                } else {
                    self.logical.pending[channel].push_back(PendingPacket {
                        data,
                        _permit: permit,
                    });
                }
            }
            OutboundCommand::Finish => {
                self.logical.finishing = true;
                self.send_next()?;
            }
        }
        Ok(())
    }

    fn acknowledge(&mut self, sequence: u64) -> std::result::Result<(), Status> {
        match self.logical.in_flight.take() {
            Some(InFlightPacket::Data {
                sequence: expected, ..
            }) if sequence == expected => self.send_next(),
            Some(InFlightPacket::Data {
                sequence: expected, ..
            }) => Err(Status::internal(format!(
                "received do_exchange ACK for sequence {}, expected {}",
                sequence, expected
            ))),
            Some(InFlightPacket::Finish { .. }) => Err(Status::internal(
                "received do_exchange ACK while waiting for ReceiverClosed",
            )),
            None => Err(Status::internal(
                "received do_exchange ACK without an in-flight request",
            )),
        }
    }

    fn send_next(&mut self) -> std::result::Result<(), Status> {
        if self.logical.in_flight.is_some() {
            return Ok(());
        }
        if let Some(data) = pop_pending(&mut self.logical.pending, self.logical.max_batch_bytes) {
            return self.install_data(data);
        }
        if self.logical.finishing {
            self.install_finish()?;
        }
        Ok(())
    }

    fn install_data(&mut self, data: FlightData) -> std::result::Result<(), Status> {
        let sequence = self.logical.next_sequence;
        let next_sequence = sequence + 1;
        let encoded = DoExchangeRequest::data(sequence, data).encode();
        self.physical.send(&encoded, "sending DATA")?;
        self.logical.next_sequence = next_sequence;
        self.logical.in_flight = Some(InFlightPacket::Data {
            encoded,
            sequence,
            sent_at: Instant::now(),
        });
        Ok(())
    }

    fn install_finish(&mut self) -> std::result::Result<(), Status> {
        let encoded = DoExchangeRequest::finish().encode();
        self.physical.send(&encoded, "sending FINISH")?;
        self.logical.in_flight = Some(InFlightPacket::Finish {
            encoded,
            sent_at: Instant::now(),
        });
        Ok(())
    }

    async fn reconnect_transport(&mut self, status: Status) -> Result<()> {
        let replay = self
            .logical
            .in_flight
            .as_ref()
            .map(|packet| packet.encoded().clone());
        self.physical
            .reconnect(status, replay, &self.cancellation)
            .await?;
        if let Some(packet) = &mut self.logical.in_flight {
            packet.reset_timer();
        }
        Ok(())
    }

    fn status_error(&self, status: Status) -> ErrorCode {
        self.physical.status_error(status)
    }
}

impl PhysicalConnection {
    async fn open(
        connector: DoExchangeConnector,
        reconnect: FlightReconnectPolicy,
        local_node_id: String,
        remote_node_id: String,
    ) -> Result<Self> {
        let attempts = reconnect.retry_times + 1;
        let transport = Self::establish(
            connector.clone(),
            reconnect,
            local_node_id.clone(),
            remote_node_id.clone(),
            None,
            attempts,
        )
        .await?;
        Ok(Self {
            transport: Some(transport),
            connector,
            reconnect,
            local_node_id,
            remote_node_id,
        })
    }

    fn ack_timeout(&self) -> Duration {
        self.reconnect.timeout
    }

    fn response_stream(&mut self) -> &mut FlightDataStream {
        &mut self
            .transport
            .as_mut()
            .expect("driver must reconnect before polling a transport")
            .response_stream
    }

    fn send(&self, encoded: &FlightData, operation: &str) -> std::result::Result<(), Status> {
        let send_tx = &self
            .transport
            .as_ref()
            .expect("sending requires a physical transport")
            .send_tx;
        match send_tx.try_send(encoded.clone()) {
            Ok(()) | Err(TrySendError::Closed(_)) => Ok(()),
            Err(TrySendError::Full(_)) => Err(Status::internal(format!(
                "do_exchange stop-and-wait invariant violated: request channel is full while {}",
                operation
            ))),
        }
    }

    async fn reconnect(
        &mut self,
        status: Status,
        replay: Option<FlightData>,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        self.transport = None;
        if !is_retryable_status(&status) || self.reconnect.retry_times == 0 {
            return Err(self.status_error(status));
        }

        let cancelled = self.contextualize(ErrorCode::AbortedQuery("do_exchange was cancelled"));
        let establish = Self::establish(
            self.connector.clone(),
            self.reconnect,
            self.local_node_id.clone(),
            self.remote_node_id.clone(),
            replay,
            self.reconnect.retry_times,
        );
        let transport = tokio::select! {
            _ = cancellation.cancelled() => Err(cancelled),
            result = establish => result,
        }?;
        warn!(
            "do_exchange sender reconnected: client={}, service={}, initial_status={}",
            self.local_node_id, self.remote_node_id, status
        );
        self.transport = Some(transport);
        Ok(())
    }

    async fn establish(
        connector: DoExchangeConnector,
        policy: FlightReconnectPolicy,
        local_node_id: String,
        remote_node_id: String,
        replay: Option<FlightData>,
        attempts: u64,
    ) -> Result<DoExchangeTransport> {
        let mut last_failure = (
            tonic::Code::Unavailable,
            "no do_exchange connection attempt was made".to_string(),
        );

        for attempt in 0..attempts {
            if attempt > 0 {
                tokio::time::sleep(policy.retry_interval).await;
            }

            match tokio::time::timeout(policy.timeout, (connector)()).await {
                Ok(Ok(transport)) => {
                    if let Some(encoded) = &replay {
                        match transport.send_tx.try_send(encoded.clone()) {
                            Ok(()) => {}
                            Err(cause) if cause.is_full() => {
                                return Err(status_to_error(
                                    Status::internal(
                                        "do_exchange stop-and-wait invariant violated: new transport request channel is full before replay",
                                    ),
                                    &local_node_id,
                                    &remote_node_id,
                                ));
                            }
                            Err(_) => {
                                last_failure = (
                                    tonic::Code::Unavailable,
                                    "new do_exchange transport closed before replay".to_string(),
                                );
                                continue;
                            }
                        }
                    }
                    return Ok(transport);
                }
                Ok(Err(cause)) if cause.code() == ErrorCode::CANNOT_CONNECT_NODE => {
                    last_failure = (tonic::Code::Unavailable, cause.to_string());
                }
                Ok(Err(cause)) => return Err(cause),
                Err(_) => {
                    last_failure = (
                        tonic::Code::DeadlineExceeded,
                        format!(
                            "connection attempt {}/{} exceeded its {:?} deadline",
                            attempt + 1,
                            attempts,
                            policy.timeout
                        ),
                    );
                }
            }
        }

        Err(status_to_error(
            Status::new(
                last_failure.0,
                format!(
                    "do_exchange connection exhausted after {} attempts; last error: {}",
                    attempts, last_failure.1
                ),
            ),
            &local_node_id,
            &remote_node_id,
        ))
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

async fn receive_command(
    commands: &Receiver<OutboundCommand>,
    finishing: bool,
) -> std::result::Result<OutboundCommand, async_channel::RecvError> {
    if finishing {
        std::future::pending().await
    } else {
        commands.recv().await
    }
}

async fn wait_for_deadline(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use databend_common_base::runtime::Runtime;

    use super::*;

    #[tokio::test]
    async fn test_initial_connect_has_bounded_deadline() {
        let connector: DoExchangeConnector = Arc::new(|| {
            Box::pin(async { std::future::pending::<Result<DoExchangeTransport>>().await })
        });

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            PendingNetworkOutbound::connect(
                1,
                connector,
                FlightReconnectPolicy {
                    retry_times: 0,
                    retry_interval: Duration::ZERO,
                    timeout: Duration::from_millis(20),
                },
                "local".to_string(),
                "remote".to_string(),
            ),
        )
        .await
        .expect("initial connection must have a bounded deadline");
        let cause = result.err().expect("initial connection must time out");
        assert!(
            cause
                .message()
                .contains("connection exhausted after 1 attempts")
        );
        assert!(cause.message().contains("exceeded its 20ms deadline"));
    }

    #[tokio::test]
    async fn test_initial_connect_retries_after_attempt_timeout() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed_attempts = attempts.clone();
        let replacement = Arc::new(Mutex::new(Some({
            let (send_tx, _send_rx) = async_channel::bounded(1);
            DoExchangeTransport {
                send_tx,
                response_stream: futures::stream::pending().boxed(),
            }
        })));
        let connector: DoExchangeConnector = Arc::new(move || {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            let replacement = replacement.clone();
            Box::pin(async move {
                if attempt == 0 {
                    return std::future::pending::<Result<DoExchangeTransport>>().await;
                }
                replacement
                    .lock()
                    .take()
                    .ok_or_else(|| ErrorCode::Internal(""))
            })
        });

        let pending = PendingNetworkOutbound::connect(
            1,
            connector,
            FlightReconnectPolicy {
                retry_times: 1,
                retry_interval: Duration::ZERO,
                timeout: Duration::from_millis(20),
            },
            "local".to_string(),
            "remote".to_string(),
        )
        .await
        .unwrap();
        assert_eq!(observed_attempts.load(Ordering::SeqCst), 2);
        drop(pending);
    }

    #[test]
    fn test_full_request_channel_is_internal_error() {
        let (send_tx, _send_rx) = async_channel::bounded(1);
        send_tx.try_send(FlightData::default()).unwrap();
        let physical = PhysicalConnection {
            transport: Some(DoExchangeTransport {
                send_tx,
                response_stream: futures::stream::pending().boxed(),
            }),
            connector: Arc::new(|| {
                Box::pin(async { Err(ErrorCode::Internal("connector must not run")) })
            }),
            reconnect: FlightReconnectPolicy {
                retry_times: 0,
                retry_interval: Duration::ZERO,
                timeout: Duration::from_secs(1),
            },
            local_node_id: "local".to_string(),
            remote_node_id: "remote".to_string(),
        };

        let cause = physical
            .send(&FlightData::default(), "sending DATA")
            .unwrap_err();
        assert_eq!(cause.code(), tonic::Code::Internal);
        assert!(cause.message().contains("stop-and-wait invariant violated"));
    }

    #[tokio::test]
    async fn test_full_replay_channel_is_not_retried() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed_attempts = attempts.clone();
        let connector: DoExchangeConnector = Arc::new(move || {
            attempts.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                let (send_tx, send_rx) = async_channel::bounded(1);
                send_tx.try_send(FlightData::default()).unwrap();
                Ok(DoExchangeTransport {
                    send_tx,
                    response_stream: send_rx.map(Ok).boxed(),
                })
            })
        });

        let result = PhysicalConnection::establish(
            connector,
            FlightReconnectPolicy {
                retry_times: 2,
                retry_interval: Duration::ZERO,
                timeout: Duration::from_secs(1),
            },
            "local".to_string(),
            "remote".to_string(),
            Some(FlightData::default()),
            2,
        )
        .await;
        let cause = match result {
            Ok(_) => panic!("a full replay channel must fail"),
            Err(cause) => cause,
        };

        assert_eq!(observed_attempts.load(Ordering::SeqCst), 1);
        assert!(cause.message().contains("stop-and-wait invariant violated"));
    }

    #[tokio::test]
    async fn test_ack_timeout_fails_outbound_without_retries() {
        let runtime = Runtime::with_worker_threads(1, None).unwrap();
        let (send_tx, send_rx) = async_channel::bounded(1);
        let connector: DoExchangeConnector =
            Arc::new(|| Box::pin(async { Err(ErrorCode::Internal("connector must not run")) }));
        let pending = PendingNetworkOutbound::create(
            1,
            DoExchangeTransport {
                send_tx,
                response_stream: futures::stream::pending().boxed(),
            },
            connector,
            FlightReconnectPolicy {
                retry_times: 0,
                retry_interval: Duration::ZERO,
                timeout: Duration::from_millis(20),
            },
            "local".to_string(),
            "remote".to_string(),
        );
        let outbound = pending.start(Arc::new(Semaphore::new(1)), None, &runtime);

        assert_eq!(
            outbound.send(0, FlightData::default()).await.unwrap(),
            SendOutcome::Accepted
        );
        send_rx.recv().await.unwrap();
        let cause = tokio::time::timeout(Duration::from_secs(2), outbound.finish())
            .await
            .expect("ACK timeout must terminate the outbound")
            .unwrap_err();
        assert!(cause.message().contains("ACK exceeded"));
    }

    #[tokio::test]
    async fn test_finish_waits_for_receiver_closed() {
        let runtime = Runtime::with_worker_threads(1, None).unwrap();
        let (send_tx, send_rx) = async_channel::bounded(1);
        let (response_tx, response_rx) = async_channel::unbounded();
        let connector: DoExchangeConnector =
            Arc::new(|| Box::pin(async { Err(ErrorCode::Internal("connector must not run")) }));
        let pending = PendingNetworkOutbound::create(
            1,
            DoExchangeTransport {
                send_tx,
                response_stream: response_rx.boxed(),
            },
            connector,
            FlightReconnectPolicy {
                retry_times: 0,
                retry_interval: Duration::ZERO,
                timeout: Duration::from_secs(1),
            },
            "local".to_string(),
            "remote".to_string(),
        );
        let outbound = Arc::new(pending.start(Arc::new(Semaphore::new(1)), None, &runtime));

        outbound.send(0, FlightData::default()).await.unwrap();
        let request = DoExchangeRequest::decode(send_rx.recv().await.unwrap()).unwrap();
        assert!(matches!(request, DoExchangeRequest::Data {
            sequence: 0,
            ..
        }));
        response_tx
            .send(Ok(DoExchangeResponse::ack(0).encode()))
            .await
            .unwrap();

        let finish = {
            let outbound = outbound.clone();
            tokio::spawn(async move { outbound.finish().await })
        };
        let request = DoExchangeRequest::decode(send_rx.recv().await.unwrap()).unwrap();
        assert!(matches!(request, DoExchangeRequest::Finish));
        assert!(!finish.is_finished());
        response_tx
            .send(Ok(DoExchangeResponse::receiver_closed().encode()))
            .await
            .unwrap();
        finish.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_reconnect_replays_unacknowledged_data() {
        let runtime = Runtime::with_worker_threads(1, None).unwrap();
        let (first_send_tx, first_send_rx) = async_channel::bounded(1);
        let (first_response_tx, first_response_rx) = async_channel::unbounded();
        let (replacement_send_tx, replacement_send_rx) = async_channel::bounded(1);
        let (replacement_response_tx, replacement_response_rx) = async_channel::unbounded();
        let replacement = Arc::new(Mutex::new(Some(DoExchangeTransport {
            send_tx: replacement_send_tx,
            response_stream: replacement_response_rx.boxed(),
        })));
        let connector: DoExchangeConnector = Arc::new(move || {
            let replacement = replacement.clone();
            Box::pin(async move {
                replacement
                    .lock()
                    .take()
                    .ok_or_else(|| ErrorCode::Internal("replacement transport already used"))
            })
        });
        let pending = PendingNetworkOutbound::create(
            1,
            DoExchangeTransport {
                send_tx: first_send_tx,
                response_stream: first_response_rx.boxed(),
            },
            connector,
            FlightReconnectPolicy {
                retry_times: 1,
                retry_interval: Duration::ZERO,
                timeout: Duration::from_secs(1),
            },
            "local".to_string(),
            "remote".to_string(),
        );
        let outbound = Arc::new(pending.start(Arc::new(Semaphore::new(1)), None, &runtime));

        outbound.send(0, FlightData::default()).await.unwrap();
        let first = DoExchangeRequest::decode(first_send_rx.recv().await.unwrap()).unwrap();
        assert!(matches!(first, DoExchangeRequest::Data { sequence: 0, .. }));
        first_response_tx
            .send(Err(Status::unavailable("physical connection lost")))
            .await
            .unwrap();

        let replay = DoExchangeRequest::decode(replacement_send_rx.recv().await.unwrap()).unwrap();
        assert!(matches!(replay, DoExchangeRequest::Data {
            sequence: 0,
            ..
        }));
        replacement_response_tx
            .send(Ok(DoExchangeResponse::ack(0).encode()))
            .await
            .unwrap();

        let finish = {
            let outbound = outbound.clone();
            tokio::spawn(async move { outbound.finish().await })
        };
        assert!(matches!(
            DoExchangeRequest::decode(replacement_send_rx.recv().await.unwrap()).unwrap(),
            DoExchangeRequest::Finish
        ));
        replacement_response_tx
            .send(Ok(DoExchangeResponse::receiver_closed().encode()))
            .await
            .unwrap();
        finish.await.unwrap().unwrap();
    }
}
