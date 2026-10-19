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

use arrow_flight::FlightData;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use concurrent_queue::ConcurrentQueue;
use databend_common_base::runtime::Runtime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use parking_lot::Mutex;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tonic::Code;
use tonic::Status;

use super::inbound_channel::flight_data_size;
use super::outbound_transport::PingPongCallback;
use super::outbound_transport::PingPongExchange;
use super::outbound_transport::PingPongResponse;
use super::outbound_transport::REMOTE_FLIGHT_CHANNEL_CLOSED_MESSAGE;
use crate::servers::flight::FlightOperation;
use crate::servers::flight::add_flight_error_context;
use crate::servers::flight::v1::network::inbound_quota::RemoteQueueItem;

/// Configuration for ExchangeSinkBuffer.
#[derive(Clone)]
pub struct ExchangeBufferConfig {
    /// Maximum bytes queued for all destinations of one exchange. Senders wait
    /// once the queue is full.
    pub max_queued_bytes: usize,
    /// Maximum bytes per batch.
    pub max_batch_bytes: usize,
}

impl Default for ExchangeBufferConfig {
    fn default() -> Self {
        Self {
            max_queued_bytes: 64 * 1024 * 1024,
            max_batch_bytes: 256 * 1024,
        }
    }
}

/// Pending FlightData for one destination node.
struct PendingQueue {
    queue: ConcurrentQueue<RemoteQueueItem>,
}

impl PendingQueue {
    fn new() -> Self {
        Self {
            queue: ConcurrentQueue::unbounded(),
        }
    }

    fn pop_front(&self, max_batch_bytes: usize) -> Option<FlightData> {
        let mut items = Vec::new();
        let mut total = 0usize;

        while let Ok(next) = self.queue.pop() {
            let data = next.into_data();
            total += data.data_body.len();
            items.push(data);
            if total >= max_batch_bytes {
                break;
            }
        }

        match items.len() {
            0 => None,
            1 => Some(items.into_iter().next().unwrap()),
            _ => Some(merge_flight_data_batch(items)),
        }
    }
}

pub(crate) const BATCH_MARKER: u8 = 0x02;

/// Pack several FlightData into one batch frame.
///
/// Each item keeps its whole app_metadata, including the tid prefix, so one frame
/// can carry data for any threads of the destination node.
///
/// Frame app_metadata: `[num_items: u32][BATCH_MARKER]`.
/// Frame body, per item: `[meta_len: u32][meta][header_len: u32][header][body_len: u32][body]`.
pub(crate) fn merge_flight_data_batch(items: Vec<FlightData>) -> FlightData {
    let mut app_metadata = BytesMut::with_capacity(5);
    app_metadata.put_u32_le(items.len() as u32);
    app_metadata.put_u8(BATCH_MARKER);

    let estimated: usize = items
        .iter()
        .map(|i| 12 + i.app_metadata.len() + i.data_header.len() + i.data_body.len())
        .sum();

    let mut body = BytesMut::with_capacity(estimated);
    for item in items {
        body.put_u32_le(item.app_metadata.len() as u32);
        body.put_slice(&item.app_metadata);
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

/// Mutable state within RemoteInstance, protected by its own lock.
struct RemoteInstanceState {
    pending: PendingQueue,
    /// Last error from the exchange, returned on next poll_send call
    last_error: Option<ErrorCode>,
}

impl RemoteInstanceState {
    fn close(&mut self, error: ErrorCode) {
        if self.last_error.is_none() {
            self.last_error = Some(error);
        }

        self.pending.queue.close();
        while self.pending.queue.pop().is_ok() {}
    }
}

/// Per-destination remote instance.
/// Each RemoteInstance has its own lock for fine-grained concurrency.
struct RemoteInstance {
    state: Mutex<RemoteInstanceState>,
    exchange: PingPongExchange,
}

impl RemoteInstance {
    fn new(exchange: PingPongExchange) -> Self {
        Self {
            exchange,
            state: Mutex::new(RemoteInstanceState {
                pending: PendingQueue::new(),
                last_error: None,
            }),
        }
    }

    fn close_state_and_notify(&self, state: &mut RemoteInstanceState, error: ErrorCode) {
        state.close(error);
        self.exchange.shutdown.notify_waiters();
    }
}

/// Inner state of ExchangeSinkBuffer, shared with callbacks.
struct ExchangeSinkBufferSharedState {
    config: ExchangeBufferConfig,

    /// Pre-allocated remote instances, indexed by destination index
    remotes: Vec<Arc<RemoteInstance>>,
}

/// Inner state of ExchangeSinkBuffer, shared with callbacks.
struct ExchangeSinkBufferInner {
    state: Arc<ExchangeSinkBufferSharedState>,
}

impl Drop for ExchangeSinkBufferInner {
    fn drop(&mut self) {
        for remote in &self.state.remotes {
            remote.exchange.shutdown.notify_waiters();
        }
    }
}

impl ExchangeSinkBufferSharedState {
    fn status_to_error(status: Status, local_node_id: &str, remote_node_id: &str) -> ErrorCode {
        let error = if status.code() == Code::Aborted {
            ErrorCode::AbortedQuery(status.message().to_string())
        } else {
            status.into()
        };

        add_flight_error_context(
            error,
            FlightOperation::DoExchange,
            local_node_id,
            remote_node_id,
        )
    }

    /// Send the next batch of pending data. The caller must own the in-flight slot.
    fn try_flush_remote(&self, dest_idx: usize, status: Option<Status>) {
        let remote = &self.remotes[dest_idx];
        let mut state = remote.state.lock();

        let Some(status) = status else {
            let Some(flight) = state.pending.pop_front(self.config.max_batch_bytes) else {
                return remote.exchange.ready_send();
            };

            let Err(status) = remote.exchange.force_send(flight) else {
                return;
            };

            let error = Self::status_to_error(
                status,
                remote.exchange.local_node_id(),
                remote.exchange.remote_node_id(),
            );
            remote.close_state_and_notify(&mut state, error);
            remote.exchange.ready_send();
            return;
        };

        state.close(Self::status_to_error(
            status,
            remote.exchange.local_node_id(),
            remote.exchange.remote_node_id(),
        ));
    }
}

/// Callback for handling PingPong responses.
struct SinkBufferCallback {
    dest_idx: usize,
    buffer: Arc<ExchangeSinkBufferSharedState>,
}

impl PingPongCallback for SinkBufferCallback {
    fn has_pending(&self) -> bool {
        let state = self.buffer.remotes[self.dest_idx].state.lock();
        !state.pending.queue.is_empty()
    }

    fn on_response(&self, response: PingPongResponse) {
        self.buffer
            .try_flush_remote(self.dest_idx, response.data.err());
    }

    fn on_closed(&self) {
        let remote = &self.buffer.remotes[self.dest_idx];
        let mut state = remote.state.lock();
        state.close(add_flight_error_context(
            ErrorCode::AbortedQuery(REMOTE_FLIGHT_CHANNEL_CLOSED_MESSAGE),
            FlightOperation::DoExchange,
            remote.exchange.local_node_id(),
            remote.exchange.remote_node_id(),
        ));
    }
}

/// Byte-bounded send queue shared by all senders of one exchange.
///
/// Each destination node has one FIFO queue and at most one request in flight.
/// Queued items to the same node are packed into batch frames of up to
/// `max_batch_bytes`, regardless of which thread of the node they target.
pub struct ExchangeSinkBuffer {
    max_permits: usize,
    semaphore: Arc<Semaphore>,
    inner: Arc<ExchangeSinkBufferInner>,
}

impl ExchangeSinkBuffer {
    /// Create a new ExchangeSinkBuffer.
    ///
    /// - `exchanges`: Pre-created PingPongExchange instances (not yet started).
    /// - `config`: Buffer configuration.
    pub fn create(
        exchanges: Vec<PingPongExchange>,
        config: ExchangeBufferConfig,
        runtime: &Runtime,
    ) -> Result<Self> {
        let max_permits = config.max_queued_bytes.clamp(1, u32::MAX as usize);
        let semaphore = Arc::new(Semaphore::new(max_permits));

        let remotes = exchanges
            .into_iter()
            .map(|exchange| Arc::new(RemoteInstance::new(exchange)))
            .collect();

        let shared_state = Arc::new(ExchangeSinkBufferSharedState { config, remotes });
        for (dest_idx, remote) in shared_state.remotes.iter().enumerate() {
            let _ = remote.exchange.start(
                Arc::new(SinkBufferCallback {
                    dest_idx,
                    buffer: shared_state.clone(),
                }),
                runtime,
            );
        }

        Ok(Self {
            max_permits,
            semaphore,
            inner: Arc::new(ExchangeSinkBufferInner {
                state: shared_state,
            }),
        })
    }

    pub async fn add_data(&self, dest_idx: usize, data: FlightData) -> Result<()> {
        self.add_items(dest_idx, vec![data]).await
    }

    /// Queue items for one destination node and send them, packed into as few
    /// frames as the batch size allows.
    ///
    /// Each item holds queue quota proportional to its size until it is sent.
    pub async fn add_items(&self, dest_idx: usize, items: Vec<FlightData>) -> Result<()> {
        let remote = &self.inner.state.remotes[dest_idx];

        if let Some(error) = remote.state.lock().last_error.clone() {
            return Err(error);
        }

        for data in items {
            let permits = flight_data_size(&data).min(self.max_permits) as u32;

            let permit = match self.semaphore.clone().try_acquire_many_owned(permits) {
                Ok(permit) => permit,
                Err(_) => {
                    // Queue is full: send what is queued before waiting for quota.
                    self.try_flush(dest_idx);
                    self.acquire(permits).await?
                }
            };

            let state = remote.state.lock();

            if let Some(error) = state.last_error.clone() {
                return Err(error);
            }

            let _ = state.pending.queue.push(RemoteQueueItem::new(data, permit));
        }

        self.try_flush(dest_idx);

        match remote.state.lock().last_error.clone() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn acquire(&self, permits: u32) -> Result<OwnedSemaphorePermit> {
        self.semaphore
            .clone()
            .acquire_many_owned(permits)
            .await
            .map_err(|_| ErrorCode::Internal("Logical error, exchange sink semaphore is closed."))
    }

    /// Start sending if the destination has no request in flight. Otherwise the
    /// response callback sends the queued data.
    fn try_flush(&self, dest_idx: usize) {
        let remote = &self.inner.state.remotes[dest_idx];

        if remote.exchange.try_acquire_send() {
            self.inner.state.try_flush_remote(dest_idx, None);
        }
    }

    pub fn is_closed(&self, dest_idx: usize) -> bool {
        let remote = &self.inner.state.remotes[dest_idx];
        let state = remote.state.lock();
        state.last_error.is_some()
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
    use crate::servers::flight::v1::network::inbound_channel::extract_tid;
    use crate::servers::flight::v1::network::inbound_channel::split_batch_flight_data;
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

    fn make_flight_data_with_tid(tid: u16, meta: &[u8], body_len: usize) -> FlightData {
        let mut app_metadata = BytesMut::with_capacity(2 + meta.len());
        app_metadata.put_u16_le(tid);
        app_metadata.put_slice(meta);
        FlightData {
            flight_descriptor: None,
            app_metadata: app_metadata.freeze(),
            data_header: Bytes::new(),
            data_body: Bytes::from(vec![0xABu8; body_len]),
        }
    }

    fn push(queue: &PendingQueue, semaphore: &Arc<Semaphore>, data: FlightData) {
        let permit = semaphore.clone().try_acquire_many_owned(1).unwrap();
        let _ = queue.queue.push(RemoteQueueItem::new(data, permit));
    }

    fn is_batch_frame(data: &FlightData) -> bool {
        data.app_metadata.len() == 5 && data.app_metadata[4] == BATCH_MARKER
    }

    fn batch_len(data: &FlightData) -> u32 {
        u32::from_le_bytes(data.app_metadata[..4].try_into().unwrap())
    }

    #[test]
    fn test_pop_front_single_item_no_batch() {
        let queue = PendingQueue::new();
        let semaphore = Arc::new(Semaphore::new(1));
        push(
            &queue,
            &semaphore,
            make_flight_data_with_tid(3, &[0x01], 100),
        );

        let result = queue.pop_front(256 * 1024).unwrap();
        // Single item: no batch wrapping, original data returned as-is
        assert_eq!(result.data_body.len(), 100);
        assert_eq!(result.app_metadata.len(), 3); // 2 tid + 1 marker
        assert_eq!(result.app_metadata[2], 0x01);
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[test]
    fn test_pop_front_batch_keeps_tid_per_item() {
        let queue = PendingQueue::new();
        let semaphore = Arc::new(Semaphore::new(10));

        for tid in [5u16, 0, 7] {
            push(
                &queue,
                &semaphore,
                make_flight_data_with_tid(tid, &[tid as u8, 0x01], 50),
            );
        }

        let result = queue.pop_front(256 * 1024).unwrap();
        assert!(is_batch_frame(&result));
        assert_eq!(batch_len(&result), 3);

        let items = split_batch_flight_data(result);
        let tids = items.iter().map(extract_tid).collect::<Vec<_>>();
        assert_eq!(tids, vec![5, 0, 7]);
        for item in &items {
            assert_eq!(item.app_metadata[2], extract_tid(item) as u8);
            assert_eq!(item.data_body.len(), 50);
        }
    }

    #[test]
    fn test_pop_front_budget_control() {
        let queue = PendingQueue::new();
        let semaphore = Arc::new(Semaphore::new(100));

        // Push 10 items of 100 bytes each
        for _ in 0..10 {
            push(
                &queue,
                &semaphore,
                make_flight_data_with_tid(0, &[0x01], 100),
            );
        }

        // Budget of 250 bytes: should pop 3 items (100+100+100 >= 250)
        let result = queue.pop_front(250).unwrap();
        assert!(is_batch_frame(&result));
        assert_eq!(batch_len(&result), 3);
        // 7 items remain
        assert_eq!(queue.queue.len(), 7);
    }

    #[test]
    fn test_pop_front_empty_queue() {
        let queue = PendingQueue::new();
        assert!(queue.pop_front(256 * 1024).is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_buffer_direct_send() {
        let rt = test_runtime();
        let (exchange, send_rx, pong_tx) = create_mock_exchange(2);
        let buffer =
            ExchangeSinkBuffer::create(vec![exchange], ExchangeBufferConfig::default(), &rt)
                .unwrap();

        // First add_data should send directly
        buffer.add_data(0, make_flight_data(10)).await.unwrap();
        let received = send_rx.recv().await.unwrap();
        assert_eq!(received.data_body.len(), 10);

        // Send pong to clear in_flight
        pong_tx.send(Ok(FlightData::default())).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        buffer.add_data(0, make_flight_data(20)).await.unwrap();
        let received = send_rx.recv().await.unwrap();
        assert_eq!(received.data_body.len(), 20);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_buffer_packs_items_into_one_frame() {
        let rt = test_runtime();
        let (exchange, send_rx, _pong_tx) = create_mock_exchange(4);
        let buffer =
            ExchangeSinkBuffer::create(vec![exchange], ExchangeBufferConfig::default(), &rt)
                .unwrap();

        let items = (0..4)
            .map(|tid| make_flight_data_with_tid(tid, &[0x01], 10))
            .collect();
        buffer.add_items(0, items).await.unwrap();

        let received = send_rx.recv().await.unwrap();
        assert!(is_batch_frame(&received));
        assert_eq!(batch_len(&received), 4);
        assert!(send_rx.try_recv().is_err());
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
        buffer.add_data(0, make_flight_data(10)).await.unwrap();
        let received = send_rx.recv().await.unwrap();
        assert_eq!(received.data_body.len(), 10);

        // Second send while in-flight gets queued
        buffer.add_data(0, make_flight_data(20)).await.unwrap();

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
        buffer.add_data(0, make_flight_data(10)).await.unwrap();
        let r0 = rx0.recv().await.unwrap();
        assert_eq!(r0.data_body.len(), 10);

        // Send to dest 1
        buffer.add_data(1, make_flight_data(20)).await.unwrap();
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
    async fn test_buffer_backpressure_by_bytes() {
        let rt = test_runtime();
        let (exchange, send_rx, pong_tx) = create_mock_exchange(2);
        let buffer = Arc::new(
            ExchangeSinkBuffer::create(
                vec![exchange],
                ExchangeBufferConfig {
                    max_queued_bytes: 10,
                    ..Default::default()
                },
                &rt,
            )
            .unwrap(),
        );

        // First send goes out directly and releases its quota.
        buffer.add_data(0, make_flight_data(10)).await.unwrap();
        let _ = send_rx.recv().await.unwrap();
        assert_eq!(buffer.semaphore.available_permits(), 10);

        // Second send is queued and holds the whole quota.
        buffer.add_data(0, make_flight_data(10)).await.unwrap();
        assert_eq!(buffer.semaphore.available_permits(), 0);

        // Third send should block due to backpressure
        let buffer2 = buffer.clone();
        let send_handle = spawn(async move {
            buffer2.add_data(0, make_flight_data(1)).await.unwrap();
        });

        // Verify it's blocked (not completed within timeout)
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!send_handle.is_finished());

        // Send pong to release — flushes queued item and its quota
        pong_tx.send(Ok(FlightData::default())).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The blocked sender should now complete
        tokio::time::timeout(Duration::from_secs(2), send_handle)
            .await
            .expect("send should unblock")
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_buffer_oversized_item_does_not_deadlock() {
        let rt = test_runtime();
        let (exchange, send_rx, _pong_tx) = create_mock_exchange(1);
        let buffer = ExchangeSinkBuffer::create(
            vec![exchange],
            ExchangeBufferConfig {
                max_queued_bytes: 4,
                ..Default::default()
            },
            &rt,
        )
        .unwrap();

        tokio::time::timeout(
            Duration::from_secs(2),
            buffer.add_data(0, make_flight_data(100)),
        )
        .await
        .expect("an item larger than the queue should still be sent")
        .unwrap();
        assert_eq!(send_rx.recv().await.unwrap().data_body.len(), 100);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_closed_path_closes_buffer_and_rejects_later_data() {
        let rt = test_runtime();
        let (exchange, send_rx, pong_tx) = create_mock_exchange(1);
        let buffer =
            ExchangeSinkBuffer::create(vec![exchange], ExchangeBufferConfig::default(), &rt)
                .unwrap();

        buffer.add_data(0, make_flight_data(1)).await.unwrap();
        let _ = send_rx.recv().await.unwrap();

        drop(pong_tx);

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let closed = {
                    let state = buffer.inner.state.remotes[0].state.lock();
                    state.last_error.is_some() && state.pending.queue.is_closed()
                };

                if closed {
                    break;
                }

                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("response stream close should close pending queues");

        let result = tokio::time::timeout(
            Duration::from_millis(200),
            buffer.add_data(0, make_flight_data(2)),
        )
        .await;
        assert!(result.is_ok(), "add_data should not block on the semaphore");
        let error = result
            .unwrap()
            .expect_err("closed stream should be reported as an error");
        assert_eq!(error.code(), ErrorCode::ABORTED_QUERY);
        assert!(
            send_rx.try_recv().is_err(),
            "data rejected after stream close should not be sent"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_force_send_error_drains_remaining_pending_queue() {
        let rt = test_runtime();
        let (exchange, send_rx, pong_tx) = create_mock_exchange(1);
        let buffer = ExchangeSinkBuffer::create(
            vec![exchange],
            ExchangeBufferConfig {
                max_queued_bytes: 8,
                max_batch_bytes: 1,
            },
            &rt,
        )
        .unwrap();

        buffer.add_data(0, make_flight_data(1)).await.unwrap();
        let _ = send_rx.recv().await.unwrap();

        buffer.add_data(0, make_flight_data(2)).await.unwrap();
        buffer.add_data(0, make_flight_data(3)).await.unwrap();

        {
            let state = buffer.inner.state.remotes[0].state.lock();
            assert_eq!(state.pending.queue.len(), 2);
        }
        assert_eq!(buffer.semaphore.available_permits(), 3);

        drop(send_rx);
        pong_tx.send(Ok(FlightData::default())).await.unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let has_error = {
                    let state = buffer.inner.state.remotes[0].state.lock();
                    state.last_error.is_some()
                };

                if has_error {
                    break;
                }

                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("force_send failure should set last_error");

        {
            let state = buffer.inner.state.remotes[0].state.lock();
            assert!(state.last_error.is_some());
            assert_eq!(
                state.pending.queue.len(),
                0,
                "force_send error should drain the rest of the pending queue"
            );
            assert!(
                state.pending.queue.is_closed(),
                "force_send error should close the pending queue"
            );
        }

        assert_eq!(
            buffer.semaphore.available_permits(),
            8,
            "draining pending items should release their semaphore permits"
        );

        let result = tokio::time::timeout(
            Duration::from_millis(200),
            buffer.add_data(0, make_flight_data(4)),
        )
        .await;
        assert!(
            result.is_ok(),
            "add_data should not block after force_send error"
        );
        let error = result
            .unwrap()
            .expect_err("later add_data should observe the exchange error");
        assert_eq!(error.code(), ErrorCode::ABORTED_QUERY);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_direct_send_closed_returns_aborted_query() {
        let rt = test_runtime();
        let (exchange, send_rx, _pong_tx) = create_mock_exchange(1);
        let buffer =
            ExchangeSinkBuffer::create(vec![exchange], ExchangeBufferConfig::default(), &rt)
                .unwrap();

        drop(send_rx);

        let error = buffer
            .add_data(0, make_flight_data(1))
            .await
            .expect_err("closed request channel should abort the exchange");
        assert_eq!(error.code(), ErrorCode::ABORTED_QUERY);
        assert_eq!(
            error.message(),
            format!(
                "{}\n(flight do_exchange, client=query-node-0, service=query-node-1)",
                REMOTE_FLIGHT_CHANNEL_CLOSED_MESSAGE
            )
        );
        assert!(
            buffer.is_closed(0),
            "direct send failure should mark the remote as closed"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_force_send_error_unblocks_waiting_add_data() {
        let rt = test_runtime();
        let (exchange, send_rx, pong_tx) = create_mock_exchange(1);
        let buffer = Arc::new(
            ExchangeSinkBuffer::create(
                vec![exchange],
                ExchangeBufferConfig {
                    max_queued_bytes: 2,
                    max_batch_bytes: 1,
                },
                &rt,
            )
            .unwrap(),
        );

        buffer.add_data(0, make_flight_data(1)).await.unwrap();
        let _ = send_rx.recv().await.unwrap();

        buffer.add_data(0, make_flight_data(2)).await.unwrap();

        let blocked_buffer = buffer.clone();
        let blocked = spawn(async move { blocked_buffer.add_data(0, make_flight_data(2)).await });

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !blocked.is_finished(),
            "third add_data should wait for the pending-queue semaphore"
        );

        drop(send_rx);
        pong_tx.send(Ok(FlightData::default())).await.unwrap();

        let result = tokio::time::timeout(Duration::from_secs(2), blocked)
            .await
            .expect("blocked add_data should be released after remote close")
            .unwrap();
        let error = result.expect_err("released add_data should observe exchange abort");
        assert_eq!(error.code(), ErrorCode::ABORTED_QUERY);
        assert_eq!(
            buffer.semaphore.available_permits(),
            2,
            "draining pending items should release the queued permit"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_response_stream_close_unblocks_waiting_add_data() {
        let rt = test_runtime();
        let (exchange, send_rx, pong_tx) = create_mock_exchange(1);
        let buffer = Arc::new(
            ExchangeSinkBuffer::create(
                vec![exchange],
                ExchangeBufferConfig {
                    max_queued_bytes: 2,
                    ..Default::default()
                },
                &rt,
            )
            .unwrap(),
        );

        buffer.add_data(0, make_flight_data(1)).await.unwrap();
        let _ = send_rx.recv().await.unwrap();

        buffer.add_data(0, make_flight_data(2)).await.unwrap();

        let blocked_buffer = buffer.clone();
        let blocked = spawn(async move { blocked_buffer.add_data(0, make_flight_data(2)).await });

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !blocked.is_finished(),
            "third add_data should wait for the pending-queue semaphore"
        );

        drop(pong_tx);

        let result = tokio::time::timeout(Duration::from_secs(2), blocked)
            .await
            .expect("blocked add_data should be released after response stream close")
            .unwrap();
        let error = result.expect_err("released add_data should observe exchange abort");
        assert_eq!(error.code(), ErrorCode::ABORTED_QUERY);
        assert_eq!(
            buffer.semaphore.available_permits(),
            2,
            "draining pending items should release the queued permit"
        );
    }
}
