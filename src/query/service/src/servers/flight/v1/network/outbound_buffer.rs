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
use std::sync::Weak;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use arrow_flight::FlightData;
use concurrent_queue::ConcurrentQueue;
use concurrent_queue::PopError;
use databend_common_base::runtime::Runtime;
use databend_common_exception::Result;
use event_listener::Event;
use parking_lot::Mutex;
use tokio::sync::Semaphore;
use tonic::Status;

use super::outbound_transport::PingPongCallback;
use super::outbound_transport::PingPongExchange;
use super::outbound_transport::PingPongResponse;
use crate::servers::flight::v1::network::inbound_quota::RemoteQueueItem;

/// Configuration for ExchangeSinkBuffer.
#[derive(Clone)]
pub struct ExchangeBufferConfig {
    /// Queue capacity factor. Actual capacity = factor * num_destinations.
    pub queue_capacity_factor: usize,
    /// Maximum bytes per batch.
    pub max_batch_bytes: usize,
}

impl Default for ExchangeBufferConfig {
    fn default() -> Self {
        Self {
            queue_capacity_factor: 64,
            max_batch_bytes: 256 * 1024,
        }
    }
}

/// Per-sink channel containing its own pending queue.
struct Channel {
    pending_queue: ConcurrentQueue<RemoteQueueItem>,
}

impl Channel {
    fn new() -> Self {
        Self {
            pending_queue: ConcurrentQueue::unbounded(),
        }
    }

    fn remaining(&self) -> usize {
        self.pending_queue.len()
    }

    fn pop_front(&mut self, _max_batch_bytes: usize) -> Option<FlightData> {
        self.pending_queue.pop().ok().map(|x| x.into_data())
    }
}

/// Mutable state within RemoteInstance, protected by its own lock.
struct RemoteInstanceState {
    /// Pre-allocated channels, indexed by channel_id
    channels: Vec<Channel>,
    /// Last error from the exchange, returned on next poll_send call
    last_error: Option<Status>,
}

/// Per-destination remote instance containing multiple sink channels.
/// Each RemoteInstance has its own lock for fine-grained concurrency.
struct RemoteInstance {
    state: Mutex<RemoteInstanceState>,
    exchange: PingPongExchange,
}

impl RemoteInstance {
    fn new(num_threads: usize, exchange: PingPongExchange) -> Self {
        let channels = (0..num_threads).map(|_| Channel::new()).collect();
        Self {
            exchange,
            state: Mutex::new(RemoteInstanceState {
                channels,
                last_error: None,
            }),
        }
    }
}

/// Inner state of ExchangeSinkBuffer, shared with callbacks.
struct ExchangeSinkBufferInner {
    config: ExchangeBufferConfig,

    /// Pre-allocated remote instances, indexed by destination index
    remotes: Vec<Arc<RemoteInstance>>,
}

impl ExchangeSinkBufferInner {
    fn try_flush_remote(&self, dest_idx: usize, status: Option<Status>) {
        let remote = &self.remotes[dest_idx];
        let mut state = remote.state.lock();

        let Some(status) = status else {
            let Some(channel) = state.channels.iter_mut().max_by_key(|x| x.remaining()) else {
                return remote.exchange.ready_send();
            };

            let Some(flight) = channel.pop_front(self.config.max_batch_bytes) else {
                return remote.exchange.ready_send();
            };

            let Ok(_) = remote.exchange.force_send(flight) else {
                state.last_error = Some(Status::aborted("Exchange closed"));
                return remote.exchange.ready_send();
            };

            return;
        };

        state.last_error = Some(status);
        for channel in &state.channels {
            channel.pending_queue.close();
        }

        for channel in &state.channels {
            while channel.pending_queue.pop().is_ok() {}
        }
    }
}

/// Callback for handling PingPong responses.
struct SinkBufferCallback {
    dest_idx: usize,
    buffer: Weak<ExchangeSinkBufferInner>,
}

impl PingPongCallback for SinkBufferCallback {
    fn on_response(&self, response: PingPongResponse) {
        let Some(buffer) = self.buffer.upgrade() else {
            return;
        };

        buffer.try_flush_remote(self.dest_idx, response.data.err());
    }
}

pub struct ExchangeSinkBuffer {
    semaphore: Arc<Semaphore>,
    inner: Arc<ExchangeSinkBufferInner>,
}

impl ExchangeSinkBuffer {
    /// Create a new ExchangeSinkBuffer.
    ///
    /// - `exchanges`: Pre-created PingPongExchange instances (not yet started).
    /// - `num_threads`: Number of channels per remote instance.
    /// - `config`: Buffer configuration.
    pub fn create(
        exchanges: Vec<PingPongExchange>,
        config: ExchangeBufferConfig,
        runtime: &Runtime,
    ) -> Result<Self> {
        let queue_capacity = config.queue_capacity_factor * exchanges.len().max(1);

        let semaphore = Arc::new(Semaphore::new(queue_capacity));
        Ok(Self {
            semaphore,
            inner: Arc::new_cyclic(|weak_inner| {
                let mut remotes = Vec::with_capacity(exchanges.len());

                for (dest_idx, exchange) in exchanges.into_iter().enumerate() {
                    let _ = exchange.start(
                        Arc::new(SinkBufferCallback {
                            dest_idx,
                            buffer: weak_inner.clone(),
                        }),
                        runtime,
                    );

                    let num_threads = exchange.num_threads;
                    remotes.push(Arc::new(RemoteInstance::new(num_threads, exchange)));
                }

                ExchangeSinkBufferInner { config, remotes }
            }),
        })
    }

    pub async fn add_data(&self, tid: usize, dest_idx: usize, data: FlightData) -> Result<()> {
        let remote = &self.inner.remotes[dest_idx];

        // Try to send directly first
        if let Some(data) = remote.exchange.try_send(data)? {
            // Failed to send (in-flight or channel full), queue the data
            let semaphore = self.semaphore.clone();
            let owned_semaphore_permit = semaphore.acquire_owned().await.unwrap();

            let mut state = remote.state.lock();

            // Check for previous error
            if let Some(status) = state.last_error.clone().take() {
                return Err(status.into());
            }

            // Try to send again
            if let Some(data) = remote.exchange.try_send(data)? {
                let item = RemoteQueueItem::new(data, owned_semaphore_permit);
                let _ = state.channels[tid].pending_queue.push(item);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use arrow_flight::FlightData;
    use databend_common_base::runtime::Runtime;
    use databend_common_base::runtime::spawn;
    use tonic::Status;

    use super::*;
    use crate::servers::flight::v1::network::outbound_transport::PingPongExchange;

    fn test_runtime() -> Arc<Runtime> {
        Arc::new(Runtime::with_worker_threads(2, None).unwrap())
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
        let exchange = PingPongExchange::from_stream(num_threads, send_tx, pong_rx);
        (exchange, send_rx, pong_tx)
    }

    fn make_flight_data(len: usize) -> FlightData {
        FlightData {
            data_body: bytes::Bytes::from(vec![0u8; len]),
            ..Default::default()
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_buffer_direct_send() {
        let rt = test_runtime();
        let (exchange, send_rx, pong_tx) = create_mock_exchange(2);
        let buffer =
            ExchangeSinkBuffer::create(vec![exchange], ExchangeBufferConfig::default(), &rt)
                .unwrap();

        // First add_data should send directly
        buffer.add_data(0, 0, make_flight_data(10)).await.unwrap();
        let received = send_rx.recv().await.unwrap();
        assert_eq!(received.data_body.len(), 10);

        // Send pong to clear in_flight
        pong_tx.send(Ok(FlightData::default())).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Second add_data from different tid should also succeed
        buffer.add_data(1, 0, make_flight_data(20)).await.unwrap();
        let received = send_rx.recv().await.unwrap();
        assert_eq!(received.data_body.len(), 20);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_buffer_queues_when_in_flight() {
        let rt = test_runtime();
        let (exchange, send_rx, pong_tx) = create_mock_exchange(2);
        let buffer = Arc::new(
            ExchangeSinkBuffer::create(vec![exchange], ExchangeBufferConfig::default(), &rt)
                .unwrap(),
        );

        // First send goes directly
        buffer.add_data(0, 0, make_flight_data(10)).await.unwrap();
        let received = send_rx.recv().await.unwrap();
        assert_eq!(received.data_body.len(), 10);

        // Second send while in-flight gets queued
        buffer.add_data(0, 0, make_flight_data(20)).await.unwrap();

        // Nothing on send_rx yet (queued)
        assert!(send_rx.try_recv().is_err());

        // Pong triggers flush of queued data
        pong_tx.send(Ok(FlightData::default())).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let received = send_rx.recv().await.unwrap();
        assert_eq!(received.data_body.len(), 20);

        // Send second pong to clear
        pong_tx.send(Ok(FlightData::default())).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_buffer_multi_dest() {
        let rt = test_runtime();
        let (ex0, rx0, pong0) = create_mock_exchange(1);
        let (ex1, rx1, pong1) = create_mock_exchange(1);
        let buffer =
            ExchangeSinkBuffer::create(vec![ex0, ex1], ExchangeBufferConfig::default(), &rt)
                .unwrap();

        // Send to dest 0
        buffer.add_data(0, 0, make_flight_data(10)).await.unwrap();
        let r0 = rx0.recv().await.unwrap();
        assert_eq!(r0.data_body.len(), 10);

        // Send to dest 1
        buffer.add_data(0, 1, make_flight_data(20)).await.unwrap();
        let r1 = rx1.recv().await.unwrap();
        assert_eq!(r1.data_body.len(), 20);

        // Other dest should be empty
        assert!(rx1.try_recv().is_err());
        assert!(rx0.try_recv().is_err());

        // Cleanup
        drop(pong0);
        drop(pong1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_buffer_backpressure() {
        let rt = test_runtime();
        let (exchange, send_rx, pong_tx) = create_mock_exchange(2);
        let buffer = Arc::new(
            ExchangeSinkBuffer::create(
                vec![exchange],
                ExchangeBufferConfig {
                    queue_capacity_factor: 1, // capacity = 1 * 1 = 1
                    ..Default::default()
                },
                &rt,
            )
            .unwrap(),
        );

        // First send goes directly (not queued)
        buffer.add_data(0, 0, make_flight_data(1)).await.unwrap();
        let _ = send_rx.recv().await.unwrap();

        // Second send gets queued (queue_size becomes 1 = capacity)
        buffer.add_data(0, 0, make_flight_data(2)).await.unwrap();

        // Third send should block due to backpressure
        let buffer2 = buffer.clone();
        let send_handle = spawn(async move {
            buffer2.add_data(1, 0, make_flight_data(3)).await.unwrap();
        });

        // Verify it's blocked (not completed within timeout)
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!send_handle.is_finished());

        // Send pong to release — flushes queued item, reduces queue_size
        pong_tx.send(Ok(FlightData::default())).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The blocked sender should now complete
        tokio::time::timeout(Duration::from_secs(2), send_handle)
            .await
            .expect("send should unblock")
            .unwrap();
    }
}
