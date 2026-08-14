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
    Failed(ErrorCode),
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
        *next_sequence = next_sequence
            .checked_add(1)
            .ok_or_else(|| ErrorCode::Internal("do_exchange sequence number exhausted"))?;
        if let Some(response) = self.terminal_response() {
            return Ok(response);
        }

        match accepted? {
            DeliveryOutcome::Accepted => Ok(DoExchangeResponse::ack(sequence)),
            DeliveryOutcome::AllReceiversClosed => {
                drop(next_sequence);
                self.terminate(InboundTerminal::Completed);
                Ok(DoExchangeResponse::receiver_closed())
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
        self.terminate(InboundTerminal::Completed);
        Ok(DoExchangeResponse::receiver_closed())
    }

    fn all_receivers_closed(&self) -> bool {
        self.destinations
            .iter()
            .all(|destination| destination.queue.sender.is_closed())
    }

    pub(crate) fn fail(&self, cause: ErrorCode) {
        self.terminate(InboundTerminal::Failed(cause));
    }

    fn terminate(&self, terminal: InboundTerminal) {
        {
            let mut lifecycle = self.lifecycle.lock();
            if lifecycle.terminal.is_some() {
                return;
            }
            lifecycle.terminal = Some(terminal.clone());
        }
        self.release(&terminal);
    }

    fn detach(self: &Arc<Self>, runtime: &Arc<Runtime>, cause: ErrorCode) {
        let lease = {
            let mut lifecycle = self.lifecycle.lock();
            if lifecycle.terminal.is_some() {
                return;
            }
            lifecycle.attachments = lifecycle
                .attachments
                .checked_sub(1)
                .expect("an attached do_exchange connection must detach exactly once");
            if lifecycle.attachments != 0 {
                return;
            }

            lifecycle.generation += 1;
            let generation = lifecycle.generation;
            if self.reconnect_lease.is_zero() {
                lifecycle.terminal = Some(InboundTerminal::Failed(cause.clone()));
                None
            } else {
                Some((generation, self.reconnect_lease))
            }
        };

        let Some((generation, reconnect_lease)) = lease else {
            self.release(&InboundTerminal::Failed(cause));
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
                lifecycle.terminal = Some(InboundTerminal::Failed(cause.clone()));
                true
            }
        };
        if failed {
            self.release(&InboundTerminal::Failed(cause));
        }
    }

    fn release(&self, terminal: &InboundTerminal) {
        if let InboundTerminal::Failed(cause) = terminal {
            warn!(
                "do_exchange receiver failed: {}, error={}",
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
            Self::Failed(cause) => DoExchangeResponse::fail(cause.clone()),
        }
    }
}

impl InboundDestination {
    fn release(&self, terminal: &InboundTerminal) {
        if let InboundTerminal::Failed(cause) = terminal {
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
        }
    }

    pub fn fail(&self, cause: ErrorCode) {
        self.source.fail(cause);
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
                    self.fail(cause.clone());
                    let _ = tx.send(Ok(DoExchangeResponse::fail(cause).encode())).await;
                    return;
                }
            };
            let response = match self.handle_request(request).await {
                Ok(response) => response,
                Err(cause) => {
                    self.fail(cause.clone());
                    DoExchangeResponse::fail(cause)
                }
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
        self.terminate(InboundTerminal::Completed);
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use databend_common_base::runtime::Runtime;

    use super::*;

    #[tokio::test]
    async fn test_physical_reattach_deduplicates_logical_data() {
        let runtime = Arc::new(Runtime::with_worker_threads(1, None).unwrap());
        let channels = NetworkInboundChannelSet::new(1);
        let source = Arc::new(NetworkInboundSource::new(
            &channels,
            1024,
            Duration::from_millis(20),
            "test source".to_string(),
        ));
        let data = FlightData {
            app_metadata: Bytes::from_static(&[0, 0]),
            ..Default::default()
        };

        let first = source.connect(
            runtime.clone(),
            ErrorCode::CannotConnectNode("first connection did not reconnect"),
        );
        assert!(matches!(
            first
                .handle_request(DoExchangeRequest::data(0, data.clone()))
                .await
                .unwrap(),
            DoExchangeResponse::Ack { sequence: 0 }
        ));
        drop(first);

        let replacement = source.connect(
            runtime,
            ErrorCode::CannotConnectNode("replacement connection did not reconnect"),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(matches!(
            replacement
                .handle_request(DoExchangeRequest::data(0, data))
                .await
                .unwrap(),
            DoExchangeResponse::Ack { sequence: 0 }
        ));

        assert!(channels.channels[0].receiver.try_recv().is_ok());
        assert!(channels.channels[0].receiver.try_recv().is_err());
        assert!(matches!(
            replacement
                .handle_request(DoExchangeRequest::finish())
                .await
                .unwrap(),
            DoExchangeResponse::ReceiverClosed
        ));
        assert!(channels.channels[0].recv_raw().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_physical_disconnect_expires_logical_source() {
        let runtime = Arc::new(Runtime::with_worker_threads(1, None).unwrap());
        let channels = NetworkInboundChannelSet::new(1);
        let source = Arc::new(NetworkInboundSource::new(
            &channels,
            1024,
            Duration::from_millis(20),
            "expired source".to_string(),
        ));

        let connection = source.connect(
            runtime.clone(),
            ErrorCode::CannotConnectNode("source reconnect lease expired"),
        );
        drop(connection);

        let result = tokio::time::timeout(Duration::from_secs(2), channels.channels[0].recv_raw())
            .await
            .expect("disconnect lease must wake the logical receiver");
        let cause = match result {
            Ok(_) => panic!("an expired source must fail instead of ending normally"),
            Err(cause) => cause,
        };
        assert!(cause.message().contains("source reconnect lease expired"));
    }
}
