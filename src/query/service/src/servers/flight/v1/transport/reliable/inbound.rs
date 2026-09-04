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
use std::time::Duration;

use arrow_flight::FlightData;
use databend_common_base::base::WatchNotify;
use databend_common_base::runtime::Runtime;
use databend_common_exception::ErrorCode;
use log::info;
use log::warn;
use parking_lot::Mutex;
use tokio_stream::StreamExt;
use tonic::Status;
use tonic::Streaming;

use super::DoExchangeRequest;
use super::DoExchangeResponse;
use crate::servers::flight::v1::transport::DeliveryOutcome;
use crate::servers::flight::v1::transport::InboundDelivery;
use crate::servers::flight::v1::transport::batch;
use crate::servers::flight::v1::transport::take_lane;

pub struct ReliableInboundSource {
    delivery: Arc<dyn InboundDelivery>,
    next_sequence: tokio::sync::Mutex<u64>,
    lifecycle: Mutex<InboundLifecycle>,
    /// Grace period after the last physical attachment disconnects. Expiry without a replacement
    /// fails the logical source and releases its inbound delivery.
    reconnect_lease: Duration,
    source_label: String,
    terminal_notified: WatchNotify,
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
    receiver_monitor_started: bool,
}

pub struct ReliableInboundConnection {
    source: Arc<ReliableInboundSource>,
    runtime: Arc<Runtime>,
    disconnect_error: Option<ErrorCode>,
}

impl ReliableInboundSource {
    pub fn new(
        delivery: Arc<dyn InboundDelivery>,
        reconnect_lease: Duration,
        source_label: String,
    ) -> Self {
        Self {
            delivery,
            next_sequence: tokio::sync::Mutex::new(0),
            lifecycle: Mutex::new(InboundLifecycle {
                terminal: None,
                attachments: 0,
                generation: 0,
                receiver_monitor_started: false,
            }),
            reconnect_lease,
            source_label,
            terminal_notified: WatchNotify::new(),
        }
    }

    pub fn connect(
        self: &Arc<Self>,
        runtime: Arc<Runtime>,
        disconnect_error: ErrorCode,
    ) -> ReliableInboundConnection {
        let (disconnect_error, consumer_closed) = {
            let mut lifecycle = self.lifecycle.lock();
            if lifecycle.terminal.is_some() {
                (None, None)
            } else {
                let reconnect = lifecycle.generation != 0;
                lifecycle.attachments += 1;
                lifecycle.generation += 1;
                let consumer_closed = if lifecycle.receiver_monitor_started {
                    None
                } else {
                    lifecycle.receiver_monitor_started = true;
                    self.delivery.consumer_closed()
                };
                if reconnect {
                    warn!(
                        "do_exchange receiver accepted replacement connection: {}",
                        self.source_label
                    );
                }
                (Some(disconnect_error), consumer_closed)
            }
        };

        if let Some(consumer_closed) = consumer_closed {
            let source = self.clone();
            runtime.spawn(async move {
                tokio::select! {
                    _ = consumer_closed => {
                        source.terminate(InboundTerminal::Completed);
                    }
                    _ = source.terminal_notified.notified() => {}
                }
            });
        }

        ReliableInboundConnection {
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
            info!(
                "do_exchange receiver ignored replayed data: {}, sequence={}, expected={}",
                self.source_label, sequence, *next_sequence
            );
            return Ok(DoExchangeResponse::ack(sequence));
        }
        if sequence > *next_sequence {
            return Err(ErrorCode::Internal(format!(
                "out-of-order do_exchange packet: expected {}, got {}",
                *next_sequence, sequence
            )));
        }

        let accepted = self.deliver(data).await;
        *next_sequence += 1;
        if let Some(response) = self.terminal_response() {
            return Ok(response);
        }

        match accepted? {
            DeliveryOutcome::Accepted => Ok(DoExchangeResponse::ack(sequence)),
            DeliveryOutcome::ConsumerClosed => {
                // No downstream consumer can accept more data, so tell the sender to stop.
                Ok(self.terminate(InboundTerminal::Completed).response())
            }
        }
    }

    async fn deliver(&self, data: FlightData) -> Result<DeliveryOutcome, ErrorCode> {
        if !batch::is_batch(&data) {
            let (lane, data) = take_lane(data)?;
            return self.delivery.deliver(lane, data).await;
        }

        for item in batch::split(data) {
            let (lane, item) = take_lane(item)?;
            if self.delivery.deliver(lane, item).await? == DeliveryOutcome::ConsumerClosed {
                return Ok(DeliveryOutcome::ConsumerClosed);
            }
        }
        Ok(DeliveryOutcome::Accepted)
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

    /// Terminates the logical source because its local consumers can no longer accept data.
    /// This releases its destinations and wakes active connections with a `FAIL` response.
    pub fn fail(&self, cause: ErrorCode) {
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
            self.terminal_notified.notify_waiters();
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

        info!(
            "do_exchange receiver waiting for replacement connection: {}, lease={:?}, generation={}",
            self.source_label, reconnect_lease, generation
        );
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
        self.delivery.terminate(terminal.cause().cloned());
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

impl ReliableInboundConnection {
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

    pub async fn serve(
        self,
        mut stream: Streaming<FlightData>,
        tx: async_channel::Sender<std::result::Result<FlightData, Status>>,
    ) {
        if let Some(response) = self.source.terminal_response() {
            info!(
                "do_exchange receiver serving terminal response to a late connection: {}",
                self.source.source_label
            );
            let _ = tx.send(Ok(response.encode())).await;
            return;
        }

        loop {
            let result = tokio::select! {
                result = stream.next() => result,
                _ = self.source.terminal_notified.notified() => {
                    if let Some(response) = self.source.terminal_response() {
                        let _ = tx.send(Ok(response.encode())).await;
                        return;
                    }
                    continue;
                }
            };
            let Some(result) = result else {
                return;
            };
            let flight_data = match result {
                Ok(flight_data) => flight_data,
                Err(status) => {
                    info!(
                        "do_exchange receiver request stream failed: {}, status={}",
                        self.source.source_label, status
                    );
                    return;
                }
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

impl Drop for ReliableInboundConnection {
    fn drop(&mut self) {
        self.disconnect();
    }
}

impl Drop for ReliableInboundSource {
    fn drop(&mut self) {
        self.terminate(InboundTerminal::ReceiverFailed(ErrorCode::AbortedQuery(
            "do_exchange logical source dropped before reaching a terminal state",
        )));
    }
}
