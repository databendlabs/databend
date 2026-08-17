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

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use arrow_flight::FlightData;
use databend_common_base::runtime::Runtime;
use databend_common_exception::ErrorCode;
use log::warn;
use parking_lot::Mutex;
use tokio::sync::Semaphore;
use tokio_stream::StreamExt;
use tonic::Status;
use tonic::Streaming;

use super::DoExchangeRequest;
use super::DoExchangeResponse;
use super::inbound_channel::NetworkInboundChannelSet;
use super::inbound_channel::extract_tid;
use super::inbound_channel::is_batch;
use super::inbound_channel::split_batch_flight_data;
use super::inbound_quota::SubQueue;

pub struct NetworkInboundSource {
    destinations: Vec<InboundDestination>,
    next_sequence: tokio::sync::Mutex<u64>,
    lifecycle: Mutex<InboundLifecycle>,
    /// Grace period after the last physical attachment disconnects. Expiry without a replacement
    /// fails the logical source and releases its inbound queues.
    reconnect_lease: Duration,
    source_label: String,
}

struct InboundDestination {
    queue: Arc<SubQueue>,
    failure: Arc<Mutex<Option<ErrorCode>>>,
}

#[derive(Clone)]
enum InboundTerminal {
    Completed,
    SenderFailed(ErrorCode),
    ReceiverFailed(ErrorCode),
}

struct InboundLifecycle {
    terminal: Option<InboundTerminal>,
    attachments: usize,
    generation: u64,
}

enum DeliveryOutcome {
    Accepted,
    AllReceiversClosed,
}

pub struct NetworkInboundConnection {
    source: Arc<NetworkInboundSource>,
    runtime: Arc<Runtime>,
    disconnect_error: Option<ErrorCode>,
}

impl NetworkInboundSource {
    pub fn new(
        channel_set: &NetworkInboundChannelSet,
        max_bytes_per_source: usize,
        reconnect_lease: Duration,
        source_label: String,
    ) -> Self {
        let semaphore = Arc::new(Semaphore::new(max_bytes_per_source));
        let mut destinations = Vec::with_capacity(channel_set.channels.len());

        for channel in channel_set.channels.iter() {
            channel.sender_count.fetch_add(1, Ordering::AcqRel);
            destinations.push(InboundDestination {
                queue: Arc::new(SubQueue {
                    max_bytes_per_connection: max_bytes_per_source,
                    sender: channel.sender.clone(),
                    receiver: channel.receiver.clone(),
                    semaphore: semaphore.clone(),
                    sender_count: channel.sender_count.clone(),
                }),
                failure: channel.failure.clone(),
            });
        }

        Self {
            destinations,
            next_sequence: tokio::sync::Mutex::new(0),
            lifecycle: Mutex::new(InboundLifecycle {
                terminal: None,
                attachments: 0,
                generation: 0,
            }),
            reconnect_lease,
            source_label,
        }
    }

    pub fn connect(
        self: &Arc<Self>,
        runtime: Arc<Runtime>,
        disconnect_error: ErrorCode,
    ) -> NetworkInboundConnection {
        let disconnect_error = {
            let mut lifecycle = self.lifecycle.lock();
            if lifecycle.terminal.is_some() {
                None
            } else {
                let reconnect = lifecycle.generation != 0;
                lifecycle.attachments += 1;
                lifecycle.generation += 1;
                if reconnect {
                    warn!(
                        "do_exchange receiver accepted replacement connection: {}",
                        self.source_label
                    );
                }
                Some(disconnect_error)
            }
        };
        NetworkInboundConnection {
            source: self.clone(),
            runtime,
            disconnect_error,
        }
    }

    async fn add_data(
        &self,
        sequence: u64,
        data: FlightData,
    ) -> Result<DoExchangeResponse, ErrorCode> {
        let mut next_sequence = self.next_sequence.lock().await;
        if let Some(response) = self.terminal_response() {
            return Ok(response);
        }
        if sequence < *next_sequence {
            return Ok(DoExchangeResponse::ack(sequence));
        }
        if sequence > *next_sequence {
            return Err(ErrorCode::Internal(format!(
                "out-of-order do_exchange packet: expected {}, got {}",
                *next_sequence, sequence
            )));
        }

        let accepted = self.add_channel_data(data).await;
        *next_sequence += 1;
        if let Some(response) = self.terminal_response() {
            return Ok(response);
        }

        match accepted? {
            DeliveryOutcome::Accepted => Ok(DoExchangeResponse::ack(sequence)),
            DeliveryOutcome::AllReceiversClosed => {
                drop(next_sequence);
                Ok(self.terminate(InboundTerminal::Completed).response())
            }
        }
    }

    async fn add_channel_data(&self, data: FlightData) -> Result<DeliveryOutcome, ErrorCode> {
        if is_batch(&data) {
            for item in split_batch_flight_data(data) {
                if matches!(
                    self.add_single_channel_data(item).await?,
                    DeliveryOutcome::AllReceiversClosed
                ) {
                    return Ok(DeliveryOutcome::AllReceiversClosed);
                }
            }
            return Ok(DeliveryOutcome::Accepted);
        }
        self.add_single_channel_data(data).await
    }

    async fn add_single_channel_data(
        &self,
        data: FlightData,
    ) -> Result<DeliveryOutcome, ErrorCode> {
        let tid = extract_tid(&data);
        let Some(destination) = self.destinations.get(tid) else {
            return Err(ErrorCode::BadBytes(format!(
                "do_exchange thread id {} is out of range for {} channels",
                tid,
                self.destinations.len()
            )));
        };
        match destination.queue.add_data(data).await {
            Ok(()) => Ok(DeliveryOutcome::Accepted),
            Err(()) if self.all_receivers_closed() => Ok(DeliveryOutcome::AllReceiversClosed),
            Err(()) => Ok(DeliveryOutcome::Accepted),
        }
    }

    async fn finish(&self) -> Result<DoExchangeResponse, ErrorCode> {
        let _next_sequence = self.next_sequence.lock().await;
        if let Some(response) = self.terminal_response() {
            return Ok(response);
        }
        Ok(self.terminate(InboundTerminal::Completed).response())
    }

    async fn sender_fail(&self, cause: ErrorCode) -> DoExchangeResponse {
        let _next_sequence = self.next_sequence.lock().await;
        self.terminate(InboundTerminal::SenderFailed(cause))
            .response()
    }

    fn all_receivers_closed(&self) -> bool {
        self.destinations
            .iter()
            .all(|destination| destination.queue.sender.is_closed())
    }

    pub(crate) fn fail(&self, cause: ErrorCode) {
        self.terminate(InboundTerminal::ReceiverFailed(cause));
    }

    fn terminate(&self, requested: InboundTerminal) -> InboundTerminal {
        let (terminal, installed) = {
            let mut lifecycle = self.lifecycle.lock();
            match &lifecycle.terminal {
                Some(terminal) => (terminal.clone(), false),
                None => {
                    lifecycle.terminal = Some(requested.clone());
                    (requested, true)
                }
            }
        };
        if installed {
            self.release(&terminal);
        }
        terminal
    }

    fn detach(self: &Arc<Self>, runtime: &Arc<Runtime>, cause: ErrorCode) {
        let lease = {
            let mut lifecycle = self.lifecycle.lock();
            if lifecycle.terminal.is_some() {
                return;
            }
            lifecycle.attachments -= 1;
            if lifecycle.attachments != 0 {
                return;
            }

            lifecycle.generation += 1;
            let generation = lifecycle.generation;
            if self.reconnect_lease.is_zero() {
                lifecycle.terminal = Some(InboundTerminal::ReceiverFailed(cause.clone()));
                None
            } else {
                Some((generation, self.reconnect_lease))
            }
        };

        let Some((generation, reconnect_lease)) = lease else {
            self.release(&InboundTerminal::ReceiverFailed(cause));
            return;
        };

        let source = Arc::downgrade(self);
        runtime.spawn(async move {
            tokio::time::sleep(reconnect_lease).await;
            if let Some(source) = source.upgrade() {
                source.expire_lease(generation, cause);
            }
        });
    }

    fn expire_lease(&self, generation: u64, cause: ErrorCode) {
        let failed = {
            let mut lifecycle = self.lifecycle.lock();
            if lifecycle.terminal.is_some()
                || lifecycle.attachments != 0
                || lifecycle.generation != generation
            {
                false
            } else {
                lifecycle.terminal = Some(InboundTerminal::ReceiverFailed(cause.clone()));
                true
            }
        };
        if failed {
            self.release(&InboundTerminal::ReceiverFailed(cause));
        }
    }

    fn release(&self, terminal: &InboundTerminal) {
        if let Some(cause) = terminal.cause() {
            warn!(
                "do_exchange logical source failed: {}, error={}",
                self.source_label, cause
            );
        }
        for destination in &self.destinations {
            destination.release(terminal);
        }
    }

    fn terminal_response(&self) -> Option<DoExchangeResponse> {
        self.lifecycle
            .lock()
            .terminal
            .as_ref()
            .map(InboundTerminal::response)
    }
}

impl InboundTerminal {
    fn response(&self) -> DoExchangeResponse {
        match self {
            Self::Completed => DoExchangeResponse::receiver_closed(),
            Self::SenderFailed(_) => DoExchangeResponse::receiver_closed(),
            Self::ReceiverFailed(cause) => DoExchangeResponse::fail(cause.clone()),
        }
    }

    fn cause(&self) -> Option<&ErrorCode> {
        match self {
            Self::Completed => None,
            Self::SenderFailed(cause) | Self::ReceiverFailed(cause) => Some(cause),
        }
    }
}

impl InboundDestination {
    fn release(&self, terminal: &InboundTerminal) {
        if let Some(cause) = terminal.cause() {
            let mut failure = self.failure.lock();
            if failure.is_none() {
                *failure = Some(cause.clone());
            }
            drop(failure);
            self.queue.sender.close();
            self.queue.semaphore.close();
        }

        if self.queue.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.queue.sender.close();
        }
    }
}

impl NetworkInboundConnection {
    fn disconnect(&mut self) {
        if let Some(cause) = self.disconnect_error.take() {
            self.source.detach(&self.runtime, cause);
        }
    }

    pub(crate) async fn handle_request(
        &self,
        request: DoExchangeRequest,
    ) -> Result<DoExchangeResponse, ErrorCode> {
        match request {
            DoExchangeRequest::Data { sequence, payload } => {
                self.source.add_data(sequence, payload).await
            }
            DoExchangeRequest::Finish => self.source.finish().await,
            DoExchangeRequest::SenderFail(cause) => Ok(self.source.sender_fail(cause).await),
        }
    }

    fn fail(&self, cause: ErrorCode) -> DoExchangeResponse {
        self.source
            .terminate(InboundTerminal::ReceiverFailed(cause))
            .response()
    }

    pub(crate) async fn serve(
        self,
        mut stream: Streaming<FlightData>,
        tx: async_channel::Sender<std::result::Result<FlightData, Status>>,
    ) {
        if let Some(response) = self.source.terminal_response() {
            let _ = tx.send(Ok(response.encode())).await;
            return;
        }

        while let Some(result) = stream.next().await {
            let Ok(flight_data) = result else {
                return;
            };
            let request = match DoExchangeRequest::decode(flight_data) {
                Ok(request) => request,
                Err(cause) => {
                    let response = self.fail(cause);
                    let _ = tx.send(Ok(response.encode())).await;
                    return;
                }
            };
            let response = match self.handle_request(request).await {
                Ok(response) => response,
                Err(cause) => self.fail(cause),
            };
            let terminal = matches!(
                response,
                DoExchangeResponse::ReceiverClosed | DoExchangeResponse::Fail(_)
            );
            if tx.send(Ok(response.encode())).await.is_err() || terminal {
                return;
            }
        }
    }
}

impl Drop for NetworkInboundConnection {
    fn drop(&mut self) {
        self.disconnect();
    }
}

impl Drop for NetworkInboundSource {
    fn drop(&mut self) {
        self.terminate(InboundTerminal::ReceiverFailed(ErrorCode::AbortedQuery(
            "do_exchange logical source dropped before reaching a terminal state",
        )));
    }
}
