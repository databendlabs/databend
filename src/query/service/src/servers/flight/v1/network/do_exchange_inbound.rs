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
use async_channel::Receiver;
use async_channel::Sender;
use databend_common_base::runtime::Runtime;
use databend_common_exception::ErrorCode;
use log::warn;
use parking_lot::Mutex;
use tokio::sync::Semaphore;

use super::DoExchangeRequest;
use super::DoExchangeResponse;
use super::inbound_channel::NetworkInboundChannelSet;
use super::inbound_channel::extract_tid;
use super::inbound_channel::is_batch;
use super::inbound_channel::split_batch_flight_data;
use super::inbound_quota::SubQueue;

/// One admitted **logical** source, retained across its physical do_exchange connections.
///
/// Keeping sequence and queue state here lets a replacement connection resend its
/// unacknowledged packet without creating another input to the pipeline. A physical EOF cannot
/// close this source immediately for the same reason, but a reconnect lease eventually fails it
/// so a permanently disconnected producer cannot keep the query open forever.
pub struct NetworkInboundSource {
    destination: InboundDestination,
    next_sequence: tokio::sync::Mutex<u64>,
    lifecycle: Mutex<InboundLifecycle>,
    reconnect_lease: Duration,
    source_label: String,
}

enum InboundDestination {
    Channels(Vec<InboundChannelDestination>),
    Packets(Sender<databend_common_exception::Result<FlightData>>),
}

struct InboundChannelDestination {
    queue: Arc<SubQueue>,
    failure: Arc<Mutex<Option<ErrorCode>>>,
}

enum InboundLifecycle {
    Active { attachments: usize, generation: u64 },
    Completed,
    Failed(ErrorCode),
}

/// One **physical** attachment to a reconnectable logical source.
///
/// Dropping a physical connection without first reaching a logical terminal state starts the
/// source's reconnect lease. The logical source itself stays in the query coordinator so a new
/// physical connection can resume from the last acknowledged sequence.
pub struct NetworkInboundAttachment {
    source: Arc<NetworkInboundSource>,
    runtime: Arc<Runtime>,
    disconnect_error: Option<ErrorCode>,
}

impl NetworkInboundSource {
    /// Adds one admitted source queue to every channel in the set.
    pub fn new(
        channel_set: &NetworkInboundChannelSet,
        max_bytes_per_source: usize,
        reconnect_lease: Duration,
        source_label: String,
    ) -> Self {
        let semaphore = Arc::new(Semaphore::new(max_bytes_per_source));
        let mut sub_queues = Vec::with_capacity(channel_set.channels.len());

        for channel in channel_set.channels.iter() {
            channel.sender_count.fetch_add(1, Ordering::AcqRel);

            let sub_queue = Arc::new(SubQueue {
                max_bytes_per_connection: max_bytes_per_source,
                sender: channel.sender.clone(),
                receiver: channel.receiver.clone(),
                semaphore: semaphore.clone(),
                sender_count: channel.sender_count.clone(),
            });

            sub_queues.push(InboundChannelDestination {
                queue: sub_queue,
                failure: channel.failure.clone(),
            });
        }

        Self {
            destination: InboundDestination::Channels(sub_queues),
            next_sequence: tokio::sync::Mutex::new(0),
            lifecycle: Mutex::new(InboundLifecycle::Active {
                attachments: 0,
                generation: 0,
            }),
            reconnect_lease,
            source_label,
        }
    }

    pub fn new_packets(
        queue_capacity: usize,
        reconnect_lease: Duration,
        source_label: String,
    ) -> (
        Self,
        Receiver<databend_common_exception::Result<FlightData>>,
    ) {
        let (tx, rx) = async_channel::bounded(queue_capacity);
        (
            Self {
                destination: InboundDestination::Packets(tx),
                next_sequence: tokio::sync::Mutex::new(0),
                lifecycle: Mutex::new(InboundLifecycle::Active {
                    attachments: 0,
                    generation: 0,
                }),
                reconnect_lease,
                source_label,
            },
            rx,
        )
    }

    pub fn attach(
        self: &Arc<Self>,
        runtime: Arc<Runtime>,
        disconnect_error: ErrorCode,
    ) -> Result<Option<NetworkInboundAttachment>, ErrorCode> {
        let mut lifecycle = self.lifecycle.lock();
        let (attachments, generation) = match &mut *lifecycle {
            InboundLifecycle::Failed(cause) => return Err(cause.clone()),
            InboundLifecycle::Completed => return Ok(None),
            InboundLifecycle::Active {
                attachments,
                generation,
            } => (attachments, generation),
        };

        let is_reconnect = *generation != 0;
        *attachments += 1;
        *generation += 1;
        drop(lifecycle);

        if is_reconnect {
            warn!(
                "do_exchange receiver accepted replacement connection: {}",
                self.source_label
            );
        }

        Ok(Some(NetworkInboundAttachment {
            source: self.clone(),
            runtime,
            disconnect_error: Some(disconnect_error),
        }))
    }

    /// Serializing sequence validation with queue insertion makes an ACK proof that the
    /// packet has crossed the receiver's deduplication boundary.
    async fn add_data(
        &self,
        sequence: u64,
        data: FlightData,
    ) -> Result<DoExchangeResponse, ErrorCode> {
        let mut next_sequence = self.next_sequence.lock().await;
        if let Some(response) = self.terminal_response()? {
            return Ok(response);
        }
        if sequence < *next_sequence {
            return Ok(DoExchangeResponse::ack());
        }
        if sequence > *next_sequence {
            return Err(ErrorCode::Internal(format!(
                "Logical error, out-of-order do_exchange packet: expected {}, got {}",
                *next_sequence, sequence
            )));
        }

        let accepted = match &self.destination {
            InboundDestination::Channels(_) => self.add_channel_data(data).await,
            InboundDestination::Packets(sender) => sender.send(Ok(data)).await.map_err(|_| ()),
        };
        *next_sequence += 1;

        match accepted {
            Ok(()) => Ok(DoExchangeResponse::ack()),
            Err(()) => {
                drop(next_sequence);
                self.complete();
                Ok(DoExchangeResponse::receiver_closed())
            }
        }
    }

    async fn add_channel_data(&self, data: FlightData) -> Result<(), ()> {
        let InboundDestination::Channels(destinations) = &self.destination else {
            unreachable!()
        };

        if is_batch(&data) {
            return self.add_batch_data(data, destinations).await;
        }

        let tid = extract_tid(&data);
        match destinations[tid].queue.add_data(data).await {
            Ok(()) => Ok(()),
            Err(()) if self.all_receivers_closed() => Err(()),
            Err(()) => Ok(()),
        }
    }

    async fn finish(&self) -> Result<DoExchangeResponse, ErrorCode> {
        let _next_sequence = self.next_sequence.lock().await;
        if let Some(response) = self.terminal_response()? {
            return Ok(response);
        }
        self.complete();
        Ok(DoExchangeResponse::receiver_closed())
    }

    async fn add_batch_data(
        &self,
        data: FlightData,
        destinations: &[InboundChannelDestination],
    ) -> Result<(), ()> {
        let items = split_batch_flight_data(data);
        for item in items {
            let tid = extract_tid(&item);
            match destinations[tid].queue.add_data(item).await {
                Ok(()) => {}
                Err(()) => {
                    if self.all_receivers_closed() {
                        return Err(());
                    }
                }
            }
        }
        Ok(())
    }

    fn all_receivers_closed(&self) -> bool {
        match &self.destination {
            InboundDestination::Channels(destinations) => {
                destinations.iter().all(|v| v.queue.sender.is_closed())
            }
            InboundDestination::Packets(sender) => sender.is_closed(),
        }
    }

    fn complete(&self) {
        if self.terminate(InboundLifecycle::Completed) {
            self.finish_destination();
        }
    }

    fn fail(&self, cause: ErrorCode) {
        if self.terminate(InboundLifecycle::Failed(cause.clone())) {
            self.fail_destination(cause);
        }
    }

    fn terminate(&self, terminal: InboundLifecycle) -> bool {
        let mut lifecycle = self.lifecycle.lock();
        if !matches!(*lifecycle, InboundLifecycle::Active { .. }) {
            return false;
        }
        *lifecycle = terminal;
        true
    }

    fn finish_destination(&self) {
        match &self.destination {
            InboundDestination::Channels(destinations) => {
                for destination in destinations {
                    let queue = &destination.queue;
                    if queue.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
                        queue.sender.close();
                    }
                }
            }
            InboundDestination::Packets(sender) => {
                sender.close();
            }
        }
    }

    fn fail_destination(&self, cause: ErrorCode) {
        warn!(
            "do_exchange receiver failed: {}, error={}",
            self.source_label, cause
        );
        match &self.destination {
            InboundDestination::Channels(destinations) => {
                for destination in destinations {
                    let mut failure = destination.failure.lock();
                    if failure.is_none() {
                        *failure = Some(cause.clone());
                    }
                    drop(failure);

                    // A failed source invalidates the whole merged channel. Closing the shared
                    // sender wakes a receiver that may otherwise be waiting for other admitted
                    // sources and lets it observe the stored error instead of a successful EOF.
                    destination.queue.sender.close();
                    destination
                        .queue
                        .sender_count
                        .fetch_sub(1, Ordering::AcqRel);
                }
            }
            InboundDestination::Packets(sender) => {
                // The bounded queue may be full when transport failure is detected. Evicting one
                // stale data packet is safe because the stream is already failed, and guarantees
                // that the terminal error cannot be hidden behind a full queue.
                let _ = sender.force_send(Err(cause));
                sender.close();
            }
        }
    }

    fn detach(self: &Arc<Self>, runtime: &Arc<Runtime>, disconnect_error: ErrorCode) {
        let lease = {
            let mut lifecycle = self.lifecycle.lock();
            let InboundLifecycle::Active {
                attachments,
                generation,
            } = &mut *lifecycle
            else {
                return;
            };
            *attachments -= 1;
            if *attachments != 0 {
                return;
            }

            *generation += 1;
            let epoch = *generation;
            if self.reconnect_lease.is_zero() {
                *lifecycle = InboundLifecycle::Failed(disconnect_error.clone());
                None
            } else {
                Some((epoch, self.reconnect_lease))
            }
        };

        let Some((epoch, reconnect_lease)) = lease else {
            self.fail_destination(disconnect_error);
            return;
        };

        // The timer owns only a Weak source reference: query cleanup must still be able to drop
        // the logical source immediately. A later attach advances the generation, so a stale timer
        // from the replaced physical connection cannot fail the new connection.
        let source = Arc::downgrade(self);
        runtime.spawn(async move {
            tokio::time::sleep(reconnect_lease).await;
            if let Some(source) = source.upgrade() {
                source.expire_lease(epoch, disconnect_error);
            }
        });
    }

    fn expire_lease(&self, epoch: u64, cause: ErrorCode) {
        let should_fail = {
            let mut lifecycle = self.lifecycle.lock();
            match &*lifecycle {
                InboundLifecycle::Active {
                    attachments: 0,
                    generation,
                } if *generation == epoch => {
                    *lifecycle = InboundLifecycle::Failed(cause.clone());
                    true
                }
                _ => false,
            }
        };
        if should_fail {
            self.fail_destination(cause);
        }
    }

    fn terminal_response(&self) -> Result<Option<DoExchangeResponse>, ErrorCode> {
        match &*self.lifecycle.lock() {
            InboundLifecycle::Active { .. } => Ok(None),
            InboundLifecycle::Completed => Ok(Some(DoExchangeResponse::receiver_closed())),
            InboundLifecycle::Failed(cause) => Err(cause.clone()),
        }
    }
}

impl NetworkInboundAttachment {
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

    /// Fails the logical source immediately; subsequent reconnects are rejected with this error.
    pub fn fail(&self, cause: ErrorCode) {
        self.source.fail(cause);
    }

    /// Detaches this physical connection without failing the logical source immediately.
    /// The source remains available for reconnect until its lease expires. Taking the error makes
    /// this idempotent when an explicit disconnect is followed by dropping the attachment.
    pub fn disconnect(&mut self) {
        if let Some(cause) = self.disconnect_error.take() {
            self.source.detach(&self.runtime, cause);
        }
    }
}

impl Drop for NetworkInboundAttachment {
    fn drop(&mut self) {
        self.disconnect();
    }
}

impl Drop for NetworkInboundSource {
    fn drop(&mut self) {
        self.complete();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use arrow_flight::FlightData;
    use databend_common_base::runtime::Runtime;

    use super::*;

    #[tokio::test]
    async fn test_packet_source_disconnect_without_retries_fails() {
        let runtime = Arc::new(Runtime::with_worker_threads(1, None).unwrap());
        let (source, receiver) =
            NetworkInboundSource::new_packets(1, Duration::ZERO, "test packet source".to_string());
        let source = Arc::new(source);
        let attachment = source
            .attach(
                runtime.clone(),
                ErrorCode::CannotConnectNode("test reconnect lease expired"),
            )
            .unwrap()
            .unwrap();
        assert!(matches!(
            attachment
                .handle_request(DoExchangeRequest::data(0, FlightData::default()))
                .await
                .unwrap(),
            DoExchangeResponse::Ack
        ));

        drop(attachment);

        let received = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .expect("disconnect must wake the packet receiver")
            .expect("disconnect must enqueue a terminal packet");
        let error = received.expect_err("a disconnected logical source must not look like EOF");
        assert_eq!(error.code(), ErrorCode::CANNOT_CONNECT_NODE);
    }

    #[tokio::test]
    async fn test_reconnect_invalidates_stale_disconnect_lease() {
        let runtime = Arc::new(Runtime::with_worker_threads(1, None).unwrap());
        let reconnect_lease = Duration::from_millis(100);
        let (source, receiver) = NetworkInboundSource::new_packets(
            1,
            reconnect_lease,
            "test reconnecting source".to_string(),
        );
        let source = Arc::new(source);

        let first = source
            .attach(
                runtime.clone(),
                ErrorCode::CannotConnectNode("first physical connection expired"),
            )
            .unwrap()
            .unwrap();
        drop(first);

        tokio::time::sleep(Duration::from_millis(20)).await;
        let replacement = source
            .attach(
                runtime,
                ErrorCode::CannotConnectNode("replacement physical connection expired"),
            )
            .unwrap()
            .unwrap();

        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(matches!(
            replacement
                .handle_request(DoExchangeRequest::finish())
                .await
                .unwrap(),
            DoExchangeResponse::ReceiverClosed
        ));
        assert!(receiver.recv().await.is_err());
    }

    #[tokio::test]
    async fn test_reconnect_replays_completed_terminal_state() {
        let runtime = Arc::new(Runtime::with_worker_threads(1, None).unwrap());
        let (source, receiver) = NetworkInboundSource::new_packets(
            1,
            Duration::from_secs(1),
            "test completed source".to_string(),
        );
        let source = Arc::new(source);
        let first = source
            .attach(
                runtime.clone(),
                ErrorCode::CannotConnectNode("first connection expired"),
            )
            .unwrap()
            .unwrap();
        assert!(matches!(
            first
                .handle_request(DoExchangeRequest::finish())
                .await
                .unwrap(),
            DoExchangeResponse::ReceiverClosed
        ));
        drop(first);

        let replacement = source
            .attach(
                runtime,
                ErrorCode::CannotConnectNode("replacement connection expired"),
            )
            .unwrap();
        assert!(replacement.is_none());
        assert!(receiver.recv().await.is_err());
    }

    #[tokio::test]
    async fn test_reconnect_replays_completed_receiver_state() {
        let runtime = Arc::new(Runtime::with_worker_threads(1, None).unwrap());
        let (source, _receiver) = NetworkInboundSource::new_packets(
            1,
            Duration::from_secs(1),
            "test closed receiver".to_string(),
        );
        let source = Arc::new(source);
        let first = source
            .attach(
                runtime.clone(),
                ErrorCode::CannotConnectNode("first connection expired"),
            )
            .unwrap()
            .unwrap();
        source.complete();
        drop(first);

        let replacement = source
            .attach(
                runtime,
                ErrorCode::CannotConnectNode("replacement connection expired"),
            )
            .unwrap();
        assert!(replacement.is_none());
    }

    #[tokio::test]
    async fn test_reconnect_replays_failed_terminal_state() {
        let runtime = Arc::new(Runtime::with_worker_threads(1, None).unwrap());
        let (source, _receiver) = NetworkInboundSource::new_packets(
            1,
            Duration::from_secs(1),
            "test failed source".to_string(),
        );
        let source = Arc::new(source);
        source.fail(ErrorCode::AbortedQuery("query was killed"));

        let result = source.attach(
            runtime,
            ErrorCode::CannotConnectNode("replacement connection expired"),
        );
        let error = match result {
            Ok(_) => panic!("a failed source must reject reconnects"),
            Err(error) => error,
        };
        assert_eq!(error.code(), ErrorCode::ABORTED_QUERY);
    }

    #[tokio::test]
    async fn test_block_source_disconnect_propagates_channel_error() {
        let runtime = Arc::new(Runtime::with_worker_threads(1, None).unwrap());
        let channels = NetworkInboundChannelSet::new(1);
        let source = Arc::new(NetworkInboundSource::new(
            &channels,
            1024,
            Duration::from_millis(20),
            "test block source".to_string(),
        ));
        let attachment = source
            .attach(
                runtime.clone(),
                ErrorCode::CannotConnectNode("block source reconnect lease expired"),
            )
            .unwrap()
            .unwrap();

        drop(attachment);

        let received =
            tokio::time::timeout(Duration::from_secs(2), channels.channels[0].recv_raw())
                .await
                .expect("lease expiry must wake the block receiver");
        let error = match received {
            Err(error) => error,
            Ok(_) => panic!("a disconnected block source must not look like EOF"),
        };
        assert_eq!(error.code(), ErrorCode::CANNOT_CONNECT_NODE);
    }
}
