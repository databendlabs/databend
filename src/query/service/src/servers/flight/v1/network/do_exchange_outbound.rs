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
use async_channel::Sender;
use async_channel::TrySendError;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use databend_common_base::runtime::Runtime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use futures::StreamExt;
use futures::stream::BoxStream;
use log::warn;
use mea::shutdown::ShutdownRecv;
use mea::shutdown::ShutdownSend;
use parking_lot::Mutex;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tonic::Status;

use super::DoExchangeRequest;
use super::DoExchangeResponse;
use super::FlightReconnectPolicy;
use crate::servers::flight::FlightClientInfo;
use crate::servers::flight::FlightOperation;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SendOutcome {
    Accepted,
    ReceiverClosed,
}

enum TrySendOutcome {
    Complete(SendOutcome),
    Pending(FlightData),
}

enum OutboundLifecycle {
    Active,
    Finishing,
    Closed,
    Failed(Status),
}

impl OutboundLifecycle {
    fn is_terminal(&self) -> bool {
        matches!(self, Self::Closed | Self::Failed(_))
    }
}

enum ReconnectOutcome {
    Reconnected(FlightDataStream),
    Aborted,
    Failed(Status),
}

struct OutboundState {
    send_tx: Option<Sender<FlightData>>,
    in_flight: Option<FlightData>,
    next_sequence: u64,
    lifecycle: OutboundLifecycle,
    pending: Vec<VecDeque<PendingPacket>>,
    slots: Arc<Semaphore>,
    max_batch_bytes: Option<usize>,
}

struct NetworkOutboundInner {
    state: Mutex<OutboundState>,
    info: FlightClientInfo,
    shutdown: ShutdownSend,
}

pub struct NetworkOutbound {
    num_threads: usize,
    inner: Arc<NetworkOutboundInner>,
}

pub(crate) struct PendingNetworkOutbound {
    num_threads: usize,
    transport: DoExchangeTransport,
    connector: DoExchangeConnector,
    reconnect: FlightReconnectPolicy,
    info: FlightClientInfo,
}

impl Drop for NetworkOutbound {
    fn drop(&mut self) {
        self.abort_if_active();
    }
}

impl NetworkOutboundInner {
    fn status_to_error(&self, status: Status) -> ErrorCode {
        let cause = if status.code() == tonic::Code::Aborted {
            ErrorCode::AbortedQuery(status.message())
        } else {
            status.into()
        };
        self.info
            .add_error_context(cause, FlightOperation::DoExchange)
    }

    fn closed_send_outcome(
        state: &OutboundState,
    ) -> std::result::Result<Option<SendOutcome>, Status> {
        match &state.lifecycle {
            OutboundLifecycle::Active => Ok(None),
            OutboundLifecycle::Finishing => Err(Status::failed_precondition(
                "cannot send data after do_exchange producer completion",
            )),
            OutboundLifecycle::Closed => Ok(Some(SendOutcome::ReceiverClosed)),
            OutboundLifecycle::Failed(cause) => Err(cause.clone()),
        }
    }

    fn try_send(&self, data: FlightData) -> std::result::Result<TrySendOutcome, Status> {
        let mut state = self.state.lock();
        if let Some(outcome) = Self::closed_send_outcome(&state)? {
            return Ok(TrySendOutcome::Complete(outcome));
        }
        if state.in_flight.is_some() {
            return Ok(TrySendOutcome::Pending(data));
        }

        Self::install_data(&mut state, data)?;
        Ok(TrySendOutcome::Complete(SendOutcome::Accepted))
    }

    fn enqueue_or_send(
        &self,
        channel: usize,
        data: FlightData,
        permit: OwnedSemaphorePermit,
    ) -> std::result::Result<SendOutcome, Status> {
        let mut state = self.state.lock();
        if let Some(outcome) = Self::closed_send_outcome(&state)? {
            return Ok(outcome);
        }

        if state.in_flight.is_none() {
            Self::install_data(&mut state, data)?;
            return Ok(SendOutcome::Accepted);
        }

        state.pending[channel].push_back(PendingPacket {
            data,
            _permit: permit,
        });
        Ok(SendOutcome::Accepted)
    }

    fn install_data(
        state: &mut OutboundState,
        data: FlightData,
    ) -> std::result::Result<(), Status> {
        let sequence = state.next_sequence;
        let encoded = DoExchangeRequest::data(sequence, data).encode();

        if let Some(send_tx) = &state.send_tx {
            match send_tx.try_send(encoded.clone()) {
                Ok(()) | Err(TrySendError::Closed(_)) => {}
                Err(TrySendError::Full(_)) => {
                    return Err(Status::resource_exhausted(
                        "do_exchange request channel is full without an in-flight packet",
                    ));
                }
            }
        }

        state.next_sequence += 1;
        state.in_flight = Some(encoded);
        Ok(())
    }

    fn acknowledge(&self) -> std::result::Result<(), Status> {
        let mut state = self.state.lock();
        if state.in_flight.take().is_none() {
            return Err(Status::internal(
                "received do_exchange ACK without an in-flight request",
            ));
        }
        let Some(data) = pop_pending(&mut state) else {
            return Ok(());
        };
        Self::install_data(&mut state, data)?;
        Ok(())
    }

    fn request_finish(&self) -> std::result::Result<(), Status> {
        let mut state = self.state.lock();
        match state.lifecycle {
            OutboundLifecycle::Active => state.lifecycle = OutboundLifecycle::Finishing,
            OutboundLifecycle::Finishing => {}
            OutboundLifecycle::Closed | OutboundLifecycle::Failed(_) => return Ok(()),
        }

        Self::send_finish_if_ready(&mut state)
    }

    fn send_finish_if_ready(state: &mut OutboundState) -> std::result::Result<(), Status> {
        if state.in_flight.is_some() {
            return Ok(());
        }

        let encoded = DoExchangeRequest::finish().encode();
        if let Some(send_tx) = &state.send_tx {
            match send_tx.try_send(encoded.clone()) {
                Ok(()) | Err(TrySendError::Closed(_)) => {}
                Err(TrySendError::Full(_)) => {
                    return Err(Status::resource_exhausted(
                        "do_exchange request channel is full while sending Finish",
                    ));
                }
            }
        }
        state.in_flight = Some(encoded);
        Ok(())
    }

    fn install_transport(&self, send_tx: Sender<FlightData>) -> std::result::Result<(), Status> {
        let mut state = self.state.lock();
        if state.lifecycle.is_terminal() {
            send_tx.close();
            return Ok(());
        }
        if let Some(in_flight) = &state.in_flight {
            send_tx.try_send(in_flight.clone()).map_err(|cause| {
                if cause.is_full() {
                    Status::resource_exhausted("new do_exchange transport is full")
                } else {
                    Status::unavailable("new do_exchange transport closed before replay")
                }
            })?;
        }
        state.send_tx = Some(send_tx);
        Ok(())
    }

    async fn reconnect(
        &self,
        shutdown: &ShutdownRecv,
        connector: &DoExchangeConnector,
        status: Status,
        policy: FlightReconnectPolicy,
    ) -> ReconnectOutcome {
        if !is_retryable_status(&status) || policy.retry_times == 0 {
            return ReconnectOutcome::Failed(status);
        }

        self.state.lock().send_tx = None;

        let reconnect = async {
            let mut last_error = status.to_string();
            for attempt in 0..policy.retry_times {
                if attempt > 0 && !policy.retry_interval.is_zero() {
                    tokio::time::sleep(policy.retry_interval).await;
                }
                match connector().await {
                    Ok(transport) => {
                        if let Err(cause) = self.install_transport(transport.send_tx) {
                            last_error = cause.to_string();
                            continue;
                        }
                        warn!(
                            "do_exchange sender reconnected: client={}, service={}, attempt={}/{}, initial_status={}",
                            self.info.local_node_id,
                            self.info.remote_node_id,
                            attempt + 1,
                            policy.retry_times,
                            status
                        );
                        return ReconnectOutcome::Reconnected(transport.response_stream);
                    }
                    Err(cause) if cause.code() != ErrorCode::CANNOT_CONNECT_NODE => {
                        return ReconnectOutcome::Failed(cause.into());
                    }
                    Err(cause) => last_error = cause.to_string(),
                }
            }
            ReconnectOutcome::Failed(Status::unavailable(format!(
                "do_exchange reconnect exhausted after {} attempts; initial error: {}; last error: {}",
                policy.retry_times, status, last_error
            )))
        };

        tokio::select! {
            _ = shutdown.is_shutdown() => ReconnectOutcome::Aborted,
            result = tokio::time::timeout(policy.retry_timeout, reconnect) => match result {
                Ok(outcome) => outcome,
                Err(_) => ReconnectOutcome::Failed(Status::deadline_exceeded(format!(
                    "do_exchange reconnect exceeded its {:?} deadline; initial error: {}",
                    policy.retry_timeout, status
                ))),
            }
        }
    }

    fn terminate(&self, lifecycle: OutboundLifecycle) {
        let mut state = self.state.lock();
        if state.lifecycle.is_terminal() {
            return;
        }
        state.send_tx = None;
        state.in_flight = None;
        state.lifecycle = lifecycle;
        clear_pending(&mut state);
    }

    // `close` is successful protocol completion confirmed by ReceiverClosed
    fn close(&self) {
        self.terminate(OutboundLifecycle::Closed);
    }

    // `fail` preserves a transport or protocol error for callers
    fn fail(&self, status: Status) {
        warn!(
            "do_exchange sender failed: client={}, service={}, status={}",
            self.info.local_node_id, self.info.remote_node_id, status
        );
        self.terminate(OutboundLifecycle::Failed(status));
    }

    // `abort` is local cancellation requested by the owner.
    fn abort(&self) {
        self.terminate(OutboundLifecycle::Failed(Status::cancelled(
            "do_exchange was cancelled",
        )));
    }
}

impl PendingNetworkOutbound {
    pub(crate) fn num_threads(&self) -> usize {
        self.num_threads
    }
    pub(crate) fn create(
        num_threads: usize,
        transport: DoExchangeTransport,
        connector: DoExchangeConnector,
        reconnect: FlightReconnectPolicy,
        info: FlightClientInfo,
    ) -> Self {
        Self {
            num_threads,
            transport,
            connector,
            reconnect,
            info,
        }
    }

    pub(crate) fn start(
        self,
        slots: Arc<Semaphore>,
        max_batch_bytes: Option<usize>,
        runtime: &Runtime,
    ) -> NetworkOutbound {
        let Self {
            num_threads,
            transport,
            connector,
            reconnect,
            info,
        } = self;
        let (shutdown, shutdown_recv) = mea::shutdown::new_pair();
        let inner = Arc::new(NetworkOutboundInner {
            state: Mutex::new(OutboundState {
                send_tx: Some(transport.send_tx),
                in_flight: None,
                next_sequence: 0,
                lifecycle: OutboundLifecycle::Active,
                pending: (0..num_threads).map(|_| VecDeque::new()).collect(),
                slots,
                max_batch_bytes,
            }),
            info,
            shutdown,
        });
        spawn_outbound_task(
            inner.clone(),
            shutdown_recv,
            transport.response_stream,
            connector,
            reconnect,
            runtime,
        );
        NetworkOutbound { num_threads, inner }
    }
}

fn spawn_outbound_task(
    inner: Arc<NetworkOutboundInner>,
    shutdown: ShutdownRecv,
    mut stream: FlightDataStream,
    connector: DoExchangeConnector,
    reconnect: FlightReconnectPolicy,
    runtime: &Runtime,
) {
    runtime.spawn(async move {
        loop {
            let finishing = matches!(inner.state.lock().lifecycle, OutboundLifecycle::Finishing);
            if finishing {
                let result = {
                    let mut state = inner.state.lock();
                    NetworkOutboundInner::send_finish_if_ready(&mut state)
                };
                if let Err(status) = result {
                    inner.fail(status);
                    break;
                }
            }

            let response = tokio::select! {
                _ = shutdown.is_shutdown() => {
                    inner.abort();
                    break;
                }
                response = stream.next() => response,
            };

            let status = match response {
                Some(Ok(data)) => match DoExchangeResponse::decode(data) {
                    Ok(DoExchangeResponse::ReceiverClosed) => {
                        inner.close();
                        break;
                    }
                    Ok(DoExchangeResponse::Ack) => {
                        if let Err(status) = inner.acknowledge() {
                            inner.fail(status);
                            break;
                        }
                        continue;
                    }
                    Err(cause) => {
                        inner.fail(cause.into());
                        break;
                    }
                },
                Some(Err(status)) => status,
                None => Status::unavailable(
                    "do_exchange response stream ended before a terminal packet",
                ),
            };
            match inner
                .reconnect(&shutdown, &connector, status, reconnect)
                .await
            {
                ReconnectOutcome::Reconnected(response_stream) => stream = response_stream,
                ReconnectOutcome::Aborted => {
                    inner.abort();
                    break;
                }
                ReconnectOutcome::Failed(status) => {
                    inner.fail(status);
                    break;
                }
            }
        }
        drop(stream);
    });
}

impl NetworkOutbound {
    pub async fn send(&self, channel: usize, data: FlightData) -> Result<SendOutcome> {
        if channel >= self.num_threads {
            return Err(ErrorCode::Internal(format!(
                "do_exchange channel {} is out of range for {} channels",
                channel, self.num_threads
            )));
        }
        let data = match self.inner.try_send(data) {
            Ok(TrySendOutcome::Complete(outcome)) => return Ok(outcome),
            Ok(TrySendOutcome::Pending(data)) => data,
            Err(status) => return Err(self.inner.status_to_error(status)),
        };
        let slots = self.inner.state.lock().slots.clone();
        let permit = slots
            .acquire_owned()
            .await
            .expect("NetworkOutbound owns every handle that can close its queue semaphore");
        self.inner
            .enqueue_or_send(channel, data, permit)
            .map_err(|status| self.inner.status_to_error(status))
    }

    pub async fn finish(&self) -> Result<()> {
        if let Err(status) = self.inner.request_finish() {
            self.inner.fail(status);
            self.inner.shutdown.shutdown();
        }
        self.inner.shutdown.clone().await_shutdown().await;

        match &self.inner.state.lock().lifecycle {
            OutboundLifecycle::Closed => Ok(()),
            OutboundLifecycle::Failed(status) => Err(self.inner.status_to_error(status.clone())),
            OutboundLifecycle::Active | OutboundLifecycle::Finishing => Err(ErrorCode::Internal(
                "do_exchange sender task exited without a terminal state",
            )),
        }
    }

    pub fn abort(&self) {
        self.inner.shutdown.shutdown();
    }

    pub fn abort_if_active(&self) {
        let state = self.inner.state.lock();
        if !state.lifecycle.is_terminal() {
            drop(state);
            self.abort();
        }
    }

    pub fn is_closed(&self) -> bool {
        self.inner.state.lock().lifecycle.is_terminal()
    }
}

fn pop_pending(state: &mut OutboundState) -> Option<FlightData> {
    let channel = state
        .pending
        .iter_mut()
        .max_by_key(|channel| channel.len())?;
    let first = channel.pop_front()?;
    let Some(max_batch_bytes) = state.max_batch_bytes else {
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

fn clear_pending(state: &mut OutboundState) {
    for channel in &mut state.pending {
        channel.clear();
    }
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
