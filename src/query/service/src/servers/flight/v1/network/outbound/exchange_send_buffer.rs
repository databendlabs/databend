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
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;

use arrow_flight::FlightData;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use parking_lot::Mutex;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tonic::Code;
use tonic::Status;

use super::super::ping_pong_transport::MAX_FRAMES_IN_FLIGHT;
use super::super::ping_pong_transport::PingPongCallback;
use super::super::ping_pong_transport::PingPongResponse;
use super::super::ping_pong_transport::PingPongTransport;
use super::super::ping_pong_transport::REMOTE_FLIGHT_CHANNEL_CLOSED_MESSAGE;
use super::super::wire_format::flight_data_size;
use super::super::wire_format::merge_flight_data_batch;
use crate::servers::flight::FlightOperation;
use crate::servers::flight::add_flight_error_context;

/// Configuration for ExchangeSendBuffer.
#[derive(Clone)]
pub struct ExchangeSendBufferConfig {
    /// Maximum bytes queued or in flight for all destinations of one exchange.
    /// Senders wait once the queue is full.
    pub max_queued_bytes: usize,
    /// Maximum total item bytes per batch, excluding batch framing.
    /// A single oversized item is sent on its own.
    pub max_batch_bytes: usize,
}

impl Default for ExchangeSendBufferConfig {
    fn default() -> Self {
        Self {
            max_queued_bytes: 64 * 1024 * 1024,
            max_batch_bytes: 1024 * 1024,
        }
    }
}

const MIN_BATCH_BYTES: usize = 256 * 1024;

impl ExchangeSendBufferConfig {
    /// Batch size used for `num_remotes` destinations.
    ///
    /// A destination has up to `MAX_FRAMES_IN_FLIGHT` frames awaiting responses, so
    /// too few bytes in flight leave the link idle for most of each round trip.
    /// Each destination gets half of its share of the queue for its frames in
    /// flight, so the next frames can fill up meanwhile, bounded by `max_batch_bytes`.
    fn batch_bytes(&self, num_remotes: usize) -> usize {
        let share = self.max_queued_bytes / (2 * num_remotes.max(1) * MAX_FRAMES_IN_FLIGHT);
        share.max(MIN_BATCH_BYTES).min(self.max_batch_bytes).max(1)
    }
}

/// Encoded data holding send quota while queued; batching transfers the permit
/// to the in-flight frame until acknowledgement or connection closure.
struct PendingSendItem {
    data: FlightData,
    permit: OwnedSemaphorePermit,
}

/// Pending encoded items for one destination node, protected by its state lock.
#[derive(Default)]
struct PendingSendQueue {
    items: VecDeque<PendingSendItem>,
}

impl PendingSendQueue {
    /// Take one batch and transfer its permits to the in-flight frame.
    /// An oversized first item must make progress, but is never batched with others.
    fn pop_batch(
        &mut self,
        max_batch_bytes: usize,
    ) -> Option<(FlightData, Vec<OwnedSemaphorePermit>)> {
        let mut items = Vec::new();
        let mut permits = Vec::new();
        let mut total = 0usize;

        while let Some(next) = self.items.front() {
            let size = flight_data_size(&next.data);
            if !items.is_empty() && size > max_batch_bytes.saturating_sub(total) {
                break;
            }
            let PendingSendItem { data, permit } = self.items.pop_front().unwrap();
            total += size;
            items.push(data);
            permits.push(permit);
            if total >= max_batch_bytes {
                break;
            }
        }

        let data = match items.len() {
            0 => return None,
            1 => items.into_iter().next().unwrap(),
            _ => merge_flight_data_batch(items),
        };
        Some((data, permits))
    }
}

/// Mutable state within RemoteInstance, protected by its own lock.
struct RemoteInstanceState {
    pending: PendingSendQueue,
    /// Quota of each frame in flight in send order, released by its response.
    in_flight: VecDeque<Vec<OwnedSemaphorePermit>>,
    /// Quota held by `in_flight`.
    in_flight_bytes: usize,
    /// Since when queued data waits for room in flight.
    held_back_since: Option<Instant>,
    /// First terminal error; checked under the same lock before every enqueue.
    last_error: Option<ErrorCode>,
}

impl RemoteInstanceState {
    /// Take the next frame unless it would raise the bytes in flight above
    /// `max_in_flight_bytes`. A frame always goes out when none is in flight, so an
    /// item larger than that makes progress.
    fn next_frame(
        &mut self,
        max_batch_bytes: usize,
        max_in_flight_bytes: usize,
        profile: &SendProfile,
    ) -> Option<FlightData> {
        let max_batch_bytes = match self.in_flight.is_empty() {
            true => max_batch_bytes,
            false => {
                let available = max_in_flight_bytes.saturating_sub(self.in_flight_bytes);
                let front = flight_data_size(&self.pending.items.front()?.data);
                if front > available {
                    self.held_back_since.get_or_insert_with(Instant::now);
                    return None;
                }
                max_batch_bytes.min(available)
            }
        };

        let (flight, permits) = self.pending.pop_batch(max_batch_bytes)?;
        if let Some(since) = self.held_back_since.take() {
            profile.record_held_back(since.elapsed());
        }
        self.in_flight_bytes += permits.iter().map(|p| p.num_permits()).sum::<usize>();
        self.in_flight.push_back(permits);
        Some(flight)
    }

    /// Queued data waits for a slot in flight.
    fn hold_back(&mut self) {
        if !self.pending.items.is_empty() {
            self.held_back_since.get_or_insert_with(Instant::now);
        }
    }

    /// Release the quota of the oldest frame in flight, answered by a response.
    fn acknowledge(&mut self) {
        if let Some(permits) = self.in_flight.pop_front() {
            self.in_flight_bytes -= permits.iter().map(|p| p.num_permits()).sum::<usize>();
        }
    }

    fn close(&mut self, error: ErrorCode) {
        if self.last_error.is_none() {
            self.last_error = Some(error);
        }

        self.pending.items.clear();
        self.in_flight.clear();
        self.in_flight_bytes = 0;
        self.held_back_since = None;
    }
}

/// Per-destination remote instance.
/// Each RemoteInstance has its own lock for fine-grained concurrency.
struct RemoteInstance {
    state: Mutex<RemoteInstanceState>,
    exchange: PingPongTransport,
}

impl RemoteInstance {
    fn new(exchange: PingPongTransport) -> Self {
        Self {
            exchange,
            state: Mutex::new(RemoteInstanceState {
                pending: PendingSendQueue::default(),
                in_flight: VecDeque::new(),
                in_flight_bytes: 0,
                held_back_since: None,
                last_error: None,
            }),
        }
    }
}

/// Profile of the processors sending through the buffer. Frames are also sent by
/// network callbacks, outside any processor, so the time data is held back goes
/// to the profile captured by the first enqueue. Time recorded after the node's
/// profile was last collected, while the buffer drains, is not reported.
#[derive(Default)]
struct SendProfile(OnceLock<Arc<Profile>>);

impl SendProfile {
    fn capture(&self) {
        if self.0.get().is_none()
            && let Some(profile) = Profile::current()
        {
            let _ = self.0.set(profile);
        }
    }

    fn record_held_back(&self, time: Duration) {
        if let Some(profile) = self.0.get() {
            profile.record_usize(
                ProfileStatisticsName::ExchangeHeldBackTime,
                time.as_nanos() as usize,
            );
        }
    }
}

/// State kept alive by response callbacks until all transports finish draining.
struct ExchangeSendBufferState {
    config: ExchangeSendBufferConfig,
    /// Quota one destination may have in flight: its share of frames in flight.
    max_in_flight_bytes: usize,
    profile: SendProfile,

    /// Pre-allocated remote instances, indexed by destination index
    remotes: Vec<RemoteInstance>,
}

impl ExchangeSendBufferState {
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

    /// Send pending frames while the destination has a free slot and quota in flight.
    /// `holds_slot` passes in a slot the caller already holds, such as the slot of
    /// an answered frame.
    ///
    /// A slot is released only under the state lock after finding nothing to send,
    /// so an item enqueued afterwards finds the slot free. Items held back by the
    /// quota in flight are sent by a response, as some frame is then in flight.
    fn flush_remote(&self, dest_idx: usize, mut holds_slot: bool) {
        let remote = &self.remotes[dest_idx];

        loop {
            if !holds_slot && !remote.exchange.try_acquire_send_slot() {
                return remote.state.lock().hold_back();
            }
            holds_slot = false;

            let mut state = remote.state.lock();
            let Some(flight) = state.next_frame(
                self.config.max_batch_bytes,
                self.max_in_flight_bytes,
                &self.profile,
            ) else {
                return remote.exchange.release_send_slot();
            };

            // A failed send releases its slot.
            if let Err(status) = remote.exchange.send_frame(flight) {
                let error = Self::status_to_error(
                    status,
                    remote.exchange.local_node_id(),
                    remote.exchange.remote_node_id(),
                );
                state.close(error);
                remote.exchange.shutdown.notify_waiters();
                return;
            }
        }
    }

    fn on_response(&self, dest_idx: usize, status: Option<Status>) {
        let remote = &self.remotes[dest_idx];

        let Some(status) = status else {
            remote.state.lock().acknowledge();
            return self.flush_remote(dest_idx, true);
        };

        remote.state.lock().close(Self::status_to_error(
            status,
            remote.exchange.local_node_id(),
            remote.exchange.remote_node_id(),
        ));
    }
}

/// Callback for handling PingPong responses.
struct SendBufferCallback {
    dest_idx: usize,
    buffer: Arc<ExchangeSendBufferState>,
}

impl PingPongCallback for SendBufferCallback {
    fn has_pending(&self) -> bool {
        let state = self.buffer.remotes[self.dest_idx].state.lock();
        !state.pending.items.is_empty()
    }

    fn on_response(&self, response: PingPongResponse) {
        self.buffer.on_response(self.dest_idx, response.data.err());
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
/// Each destination node has one FIFO queue and up to `MAX_FRAMES_IN_FLIGHT`
/// requests in flight, holding at most its share of the queue between them.
/// Queued items to the same node share batches regardless of their target thread.
/// `max_batch_bytes` bounds their total payload size, except for a single oversized
/// item; batch framing adds a small overhead.
pub struct ExchangeSendBuffer {
    max_permits: usize,
    semaphore: Arc<Semaphore>,
    state: Arc<ExchangeSendBufferState>,
}

impl Drop for ExchangeSendBuffer {
    fn drop(&mut self) {
        // Callbacks retain the state to drain pending frames after the final
        // producer releases this buffer. Do not clear their queues here.
        for remote in &self.state.remotes {
            remote.exchange.shutdown.notify_waiters();
        }
    }
}

impl ExchangeSendBuffer {
    /// Create a new ExchangeSendBuffer.
    ///
    /// - `exchanges`: Pre-created PingPongTransport instances (not yet started).
    /// - `config`: Buffer configuration.
    pub fn create(
        exchanges: Vec<PingPongTransport>,
        config: ExchangeSendBufferConfig,
        runtime: &Runtime,
    ) -> Result<Self> {
        let max_permits = config.max_queued_bytes.clamp(1, u32::MAX as usize);
        let semaphore = Arc::new(Semaphore::new(max_permits));
        let config = ExchangeSendBufferConfig {
            max_batch_bytes: config.batch_bytes(exchanges.len()),
            ..config
        };
        let max_in_flight_bytes = config.max_batch_bytes * MAX_FRAMES_IN_FLIGHT;

        let remotes = exchanges.into_iter().map(RemoteInstance::new).collect();

        let shared_state = Arc::new(ExchangeSendBufferState {
            config,
            max_in_flight_bytes,
            profile: SendProfile::default(),
            remotes,
        });
        for (dest_idx, remote) in shared_state.remotes.iter().enumerate() {
            let _ = remote.exchange.start(
                Arc::new(SendBufferCallback {
                    dest_idx,
                    buffer: shared_state.clone(),
                }),
                runtime,
            );
        }

        Ok(Self {
            max_permits,
            semaphore,
            state: shared_state,
        })
    }

    /// Queue items for one destination node and send them, packed into as few
    /// frames as the batch size allows.
    ///
    /// Completion means the items have entered the send buffer; it does not wait
    /// for a network response. Each item holds queue quota proportional to its
    /// size until the frame carrying it is answered.
    pub async fn enqueue_items(&self, dest_idx: usize, items: Vec<FlightData>) -> Result<()> {
        let remote = &self.state.remotes[dest_idx];

        if let Some(error) = remote.state.lock().last_error.clone() {
            return Err(error);
        }

        // Called by the sending processors, unlike the network callbacks.
        self.state.profile.capture();
        for data in items {
            let size = flight_data_size(&data);
            Profile::record_usize_profile(ProfileStatisticsName::ExchangeBytes, size);
            let permits = size.min(self.max_permits) as u32;

            let permit = match self.semaphore.clone().try_acquire_many_owned(permits) {
                Ok(permit) => permit,
                Err(_) => {
                    // Queue is full: send what is queued before waiting for quota.
                    self.try_flush(dest_idx);
                    let start = Instant::now();
                    let permit = self
                        .semaphore
                        .clone()
                        .acquire_many_owned(permits)
                        .await
                        .expect("exchange send quota is never closed");
                    Profile::record_usize_profile(
                        ProfileStatisticsName::ExchangeSendQueueWaitTime,
                        start.elapsed().as_nanos() as usize,
                    );
                    permit
                }
            };

            let mut state = remote.state.lock();

            if let Some(error) = state.last_error.clone() {
                return Err(error);
            }

            state
                .pending
                .items
                .push_back(PendingSendItem { data, permit });
        }

        self.try_flush(dest_idx);

        match remote.state.lock().last_error.clone() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Send queued data through the destination's free slots. Data left queued is
    /// sent by response callbacks.
    fn try_flush(&self, dest_idx: usize) {
        self.state.flush_remote(dest_idx, false);
    }

    pub fn is_closed(&self, dest_idx: usize) -> bool {
        let remote = &self.state.remotes[dest_idx];
        let state = remote.state.lock();
        state.last_error.is_some()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use arrow_flight::FlightData;
    use bytes::BufMut;
    use bytes::Bytes;
    use bytes::BytesMut;
    use databend_common_base::runtime::Runtime;
    use databend_common_base::runtime::ThreadTracker;
    use databend_common_base::runtime::spawn;
    use tonic::Status;

    use super::*;
    use crate::servers::flight::v1::network::ping_pong_transport::PingPongTransport;
    use crate::servers::flight::v1::network::wire_format::extract_tid;
    use crate::servers::flight::v1::network::wire_format::is_batch;
    use crate::servers::flight::v1::network::wire_format::split_batch_flight_data;

    fn test_runtime() -> Arc<Runtime> {
        Arc::new(Runtime::with_worker_threads(2, None).unwrap())
    }

    fn create_mock_exchange(
        num_threads: usize,
    ) -> (
        PingPongTransport,
        async_channel::Receiver<FlightData>,
        async_channel::Sender<std::result::Result<FlightData, Status>>,
    ) {
        create_mock_exchange_with_slots(num_threads, 1)
    }

    fn create_mock_exchange_with_slots(
        num_threads: usize,
        slots: usize,
    ) -> (
        PingPongTransport,
        async_channel::Receiver<FlightData>,
        async_channel::Sender<std::result::Result<FlightData, Status>>,
    ) {
        let (send_tx, send_rx) = async_channel::bounded(slots);
        let (pong_tx, pong_rx) = async_channel::unbounded();
        let exchange = PingPongTransport::from_stream(
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

    fn push(queue: &mut PendingSendQueue, semaphore: &Arc<Semaphore>, data: FlightData) {
        let permit = semaphore.clone().try_acquire_many_owned(1).unwrap();
        queue.items.push_back(PendingSendItem { data, permit });
    }

    fn batch_len(data: &FlightData) -> u32 {
        u32::from_le_bytes(data.app_metadata[..4].try_into().unwrap())
    }

    #[test]
    fn test_pop_front_single_item_no_batch() {
        let mut queue = PendingSendQueue::default();
        let semaphore = Arc::new(Semaphore::new(1));
        push(
            &mut queue,
            &semaphore,
            make_flight_data_with_tid(3, &[0x01], 100),
        );

        let (result, permits) = queue.pop_batch(256 * 1024).unwrap();
        // Single item: no batch wrapping, original data returned as-is
        assert_eq!(result.data_body.len(), 100);
        assert_eq!(result.app_metadata.len(), 3); // 2 tid + 1 marker
        assert_eq!(result.app_metadata[2], 0x01);
        // The quota moves with the frame.
        assert_eq!(semaphore.available_permits(), 0);
        drop(permits);
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[test]
    fn test_pop_front_batch_keeps_tid_per_item() {
        let mut queue = PendingSendQueue::default();
        let semaphore = Arc::new(Semaphore::new(10));

        for tid in [5u16, 0, 7] {
            push(
                &mut queue,
                &semaphore,
                make_flight_data_with_tid(tid, &[tid as u8, 0x01], 50),
            );
        }

        let (result, permits) = queue.pop_batch(256 * 1024).unwrap();
        assert!(is_batch(&result));
        assert_eq!(batch_len(&result), 3);
        assert_eq!(permits.len(), 3);

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
        let mut queue = PendingSendQueue::default();
        let semaphore = Arc::new(Semaphore::new(100));

        // Push 10 items with 100-byte bodies and 3-byte metadata each
        for _ in 0..10 {
            push(
                &mut queue,
                &semaphore,
                make_flight_data_with_tid(0, &[0x01], 100),
            );
        }

        // A third 103-byte item would exceed the 250-byte budget.
        let (result, _permits) = queue.pop_batch(250).unwrap();
        assert!(is_batch(&result));
        assert_eq!(batch_len(&result), 2);
        assert_eq!(queue.items.len(), 8);
    }

    #[test]
    fn test_pop_front_empty_queue() {
        let mut queue = PendingSendQueue::default();
        assert!(queue.pop_batch(256 * 1024).is_none());
    }

    #[test]
    fn test_batch_budget_counts_metadata_and_header_without_body() {
        let mut queue = PendingSendQueue::default();
        let semaphore = Arc::new(Semaphore::new(3));
        for tid in 0..3 {
            let mut data = make_flight_data_with_tid(tid, &[0x01; 30], 0);
            data.data_header = Bytes::from(vec![0; 64]);
            push(&mut queue, &semaphore, data);
        }

        let (frame, permits) = queue.pop_batch(192).unwrap();
        let items = split_batch_flight_data(frame);
        assert_eq!(items.iter().map(extract_tid).collect::<Vec<_>>(), vec![
            0, 1
        ]);
        assert_eq!(items.iter().map(flight_data_size).sum::<usize>(), 192);
        assert_eq!(queue.items.len(), 1);
        assert_eq!(semaphore.available_permits(), 0);
        drop(permits);
        assert_eq!(semaphore.available_permits(), 2);
    }

    #[test]
    fn test_oversized_item_is_sent_alone_without_reordering() {
        let mut queue = PendingSendQueue::default();
        let semaphore = Arc::new(Semaphore::new(2));
        push(
            &mut queue,
            &semaphore,
            make_flight_data_with_tid(3, &[0x01], 300),
        );
        push(
            &mut queue,
            &semaphore,
            make_flight_data_with_tid(1, &[0x01], 10),
        );

        let (frame, permits) = queue.pop_batch(100).unwrap();
        assert!(!is_batch(&frame));
        assert_eq!(extract_tid(&frame), 3);
        assert_eq!(frame.data_body.len(), 300);
        drop(permits);
        let (frame, permits) = queue.pop_batch(100).unwrap();
        assert_eq!(extract_tid(&frame), 1);
        assert!(queue.items.is_empty());
        drop(permits);
        assert_eq!(semaphore.available_permits(), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_last_buffer_owner_drop_drains_pending_frames_before_eof() {
        let rt = test_runtime();
        let (exchange, send_rx, pong_tx) = create_mock_exchange(1);
        let buffer = Arc::new(
            ExchangeSendBuffer::create(
                vec![exchange],
                ExchangeSendBufferConfig {
                    max_queued_bytes: 6,
                    max_batch_bytes: 1,
                },
                &rt,
            )
            .unwrap(),
        );
        let semaphore = buffer.semaphore.clone();
        let draining_state = Arc::downgrade(&buffer.state);

        tokio::time::timeout(Duration::from_secs(2), async {
            buffer
                .enqueue_items(0, vec![make_flight_data(1)])
                .await
                .unwrap();
            assert_eq!(send_rx.recv().await.unwrap().data_body.len(), 1);
            buffer
                .enqueue_items(0, vec![make_flight_data(2)])
                .await
                .unwrap();

            let remaining_owner = buffer.clone();
            drop(buffer);
            assert!(!send_rx.is_closed());
            remaining_owner
                .enqueue_items(0, vec![make_flight_data(3)])
                .await
                .unwrap();
            drop(remaining_owner);
            assert_eq!(semaphore.available_permits(), 0);

            // Shutdown must leave queued frames and their quota with the callbacks.
            pong_tx.send(Ok(FlightData::default())).await.unwrap();
            assert_eq!(send_rx.recv().await.unwrap().data_body.len(), 2);
            pong_tx.send(Ok(FlightData::default())).await.unwrap();
            assert_eq!(send_rx.recv().await.unwrap().data_body.len(), 3);
            assert!(send_rx.recv().await.is_err());

            // Request EOF does not release the final frame's quota before its pong.
            assert_eq!(semaphore.available_permits(), 3);
            pong_tx.send(Ok(FlightData::default())).await.unwrap();
            let permits = semaphore.clone().acquire_many_owned(6).await.unwrap();
            drop(permits);
            drop(pong_tx);
            // The last response task must release the drain state after peer EOF.
            while draining_state.strong_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("last-owner shutdown must drain pending frames and acknowledge them");
        assert_eq!(semaphore.available_permits(), 6);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_full_request_queue_fails_exchange_and_releases_quota() {
        let rt = test_runtime();
        let (send_tx, send_rx) = async_channel::bounded(1);
        let (pong_tx, pong_rx) = async_channel::unbounded();
        // Violate the transport invariant: a frame is queued without owning the slot.
        send_tx.try_send(make_flight_data(99)).unwrap();
        let exchange = PingPongTransport::from_stream(1, send_tx, pong_rx, "local", "remote");
        let buffer = ExchangeSendBuffer::create(
            vec![exchange],
            ExchangeSendBufferConfig {
                max_queued_bytes: 8,
                max_batch_bytes: 1,
            },
            &rt,
        )
        .unwrap();

        let error = buffer
            .enqueue_items(0, vec![make_flight_data(1), make_flight_data(2)])
            .await
            .expect_err("an unsent frame must not be reported as successful");
        assert!(error.message().contains("Flight request queue is full"));
        assert!(buffer.is_closed(0));
        assert_eq!(buffer.semaphore.available_permits(), 8);
        let later_error = buffer
            .enqueue_items(0, vec![make_flight_data(3)])
            .await
            .unwrap_err();
        assert_eq!(later_error.message(), error.message());
        assert_eq!(send_rx.recv().await.unwrap().data_body.len(), 99);
        assert!(
            tokio::time::timeout(Duration::from_secs(2), send_rx.recv())
                .await
                .expect("send failure must close the request stream")
                .is_err()
        );
        drop(pong_tx);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_buffer_direct_send() {
        let rt = test_runtime();
        let (exchange, send_rx, pong_tx) = create_mock_exchange(2);
        let buffer =
            ExchangeSendBuffer::create(vec![exchange], ExchangeSendBufferConfig::default(), &rt)
                .unwrap();

        // First enqueue_items should send directly
        buffer
            .enqueue_items(0, vec![make_flight_data(10)])
            .await
            .unwrap();
        let received = send_rx.recv().await.unwrap();
        assert_eq!(received.data_body.len(), 10);

        // Send pong to clear in_flight
        pong_tx.send(Ok(FlightData::default())).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        buffer
            .enqueue_items(0, vec![make_flight_data(20)])
            .await
            .unwrap();
        let received = send_rx.recv().await.unwrap();
        assert_eq!(received.data_body.len(), 20);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_buffer_queues_when_in_flight() {
        let rt = test_runtime();
        let (exchange, send_rx, pong_tx) = create_mock_exchange(2);
        let buffer = Arc::new(
            ExchangeSendBuffer::create(vec![exchange], ExchangeSendBufferConfig::default(), &rt)
                .unwrap(),
        );

        // First send goes directly
        buffer
            .enqueue_items(0, vec![make_flight_data(10)])
            .await
            .unwrap();
        let received = send_rx.recv().await.unwrap();
        assert_eq!(received.data_body.len(), 10);

        // Second send while in-flight gets queued
        buffer
            .enqueue_items(0, vec![make_flight_data(20)])
            .await
            .unwrap();

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
            ExchangeSendBuffer::create(vec![ex0, ex1], ExchangeSendBufferConfig::default(), &rt)
                .unwrap();

        // Send to dest 0
        buffer
            .enqueue_items(0, vec![make_flight_data(10)])
            .await
            .unwrap();
        let r0 = rx0.recv().await.unwrap();
        assert_eq!(r0.data_body.len(), 10);

        // Send to dest 1
        buffer
            .enqueue_items(1, vec![make_flight_data(20)])
            .await
            .unwrap();
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
            ExchangeSendBuffer::create(
                vec![exchange],
                ExchangeSendBufferConfig {
                    max_queued_bytes: 10,
                    ..Default::default()
                },
                &rt,
            )
            .unwrap(),
        );

        // The frame in flight keeps the whole quota until it is answered.
        buffer
            .enqueue_items(0, vec![make_flight_data(10)])
            .await
            .unwrap();
        let _ = send_rx.recv().await.unwrap();
        assert_eq!(buffer.semaphore.available_permits(), 0);

        // Second send should block due to backpressure
        let buffer2 = buffer.clone();
        let send_handle = spawn(async move {
            buffer2
                .enqueue_items(0, vec![make_flight_data(1)])
                .await
                .unwrap();
        });

        // Verify it's blocked (not completed within timeout)
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!send_handle.is_finished());

        // The response releases the quota of the answered frame
        pong_tx.send(Ok(FlightData::default())).await.unwrap();

        // The blocked sender should now complete and its data be sent
        tokio::time::timeout(Duration::from_secs(2), send_handle)
            .await
            .expect("send should unblock")
            .unwrap();
        assert_eq!(send_rx.recv().await.unwrap().data_body.len(), 1);
        assert_eq!(buffer.semaphore.available_permits(), 9);

        pong_tx.send(Ok(FlightData::default())).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while buffer.semaphore.available_permits() != 10 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the last response should release the remaining quota");
    }

    /// An item is charged for its metadata and header as well as its body, so items
    /// with an empty body still take quota.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_buffer_charges_metadata_and_header() {
        let rt = test_runtime();
        let (exchange, send_rx, _pong_tx) = create_mock_exchange(1);
        let buffer = ExchangeSendBuffer::create(
            vec![exchange],
            ExchangeSendBufferConfig {
                max_queued_bytes: 1024,
                ..Default::default()
            },
            &rt,
        )
        .unwrap();

        let mut item = make_flight_data_with_tid(0, &[0x01; 6], 0);
        item.data_header = Bytes::from(vec![0u8; 100]);
        let size = flight_data_size(&item);
        assert_eq!(size, 108);

        // Sent right away, the frame in flight keeps its quota.
        buffer.enqueue_items(0, vec![item]).await.unwrap();
        let _ = send_rx.recv().await.unwrap();
        assert_eq!(buffer.semaphore.available_permits(), 1024 - size);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_buffer_oversized_item_does_not_deadlock() {
        let rt = test_runtime();
        let (exchange, send_rx, _pong_tx) = create_mock_exchange(1);
        let buffer = ExchangeSendBuffer::create(
            vec![exchange],
            ExchangeSendBufferConfig {
                max_queued_bytes: 4,
                ..Default::default()
            },
            &rt,
        )
        .unwrap();

        tokio::time::timeout(
            Duration::from_secs(2),
            buffer.enqueue_items(0, vec![make_flight_data(100)]),
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
            ExchangeSendBuffer::create(vec![exchange], ExchangeSendBufferConfig::default(), &rt)
                .unwrap();

        buffer
            .enqueue_items(0, vec![make_flight_data(1)])
            .await
            .unwrap();
        let _ = send_rx.recv().await.unwrap();

        drop(pong_tx);

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let closed = {
                    let state = buffer.state.remotes[0].state.lock();
                    state.last_error.is_some() && state.pending.items.is_empty()
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
            buffer.enqueue_items(0, vec![make_flight_data(2)]),
        )
        .await;
        assert!(
            result.is_ok(),
            "enqueue_items should not block on the semaphore"
        );
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
    async fn test_send_frame_error_drains_remaining_pending_queue() {
        let rt = test_runtime();
        let (exchange, send_rx, pong_tx) = create_mock_exchange(1);
        let buffer = ExchangeSendBuffer::create(
            vec![exchange],
            ExchangeSendBufferConfig {
                max_queued_bytes: 8,
                max_batch_bytes: 1,
            },
            &rt,
        )
        .unwrap();

        buffer
            .enqueue_items(0, vec![make_flight_data(1)])
            .await
            .unwrap();
        let _ = send_rx.recv().await.unwrap();

        buffer
            .enqueue_items(0, vec![make_flight_data(2)])
            .await
            .unwrap();
        buffer
            .enqueue_items(0, vec![make_flight_data(3)])
            .await
            .unwrap();

        {
            let state = buffer.state.remotes[0].state.lock();
            assert_eq!(state.pending.items.len(), 2);
        }
        // The frame in flight still holds its quota.
        assert_eq!(buffer.semaphore.available_permits(), 2);

        drop(send_rx);
        pong_tx.send(Ok(FlightData::default())).await.unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let has_error = {
                    let state = buffer.state.remotes[0].state.lock();
                    state.last_error.is_some()
                };

                if has_error {
                    break;
                }

                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("send_frame failure should set last_error");

        {
            let state = buffer.state.remotes[0].state.lock();
            assert!(state.last_error.is_some());
            assert_eq!(
                state.pending.items.len(),
                0,
                "send_frame error should drain the rest of the pending queue"
            );
        }

        assert_eq!(
            buffer.semaphore.available_permits(),
            8,
            "draining pending items should release their semaphore permits"
        );

        let result = tokio::time::timeout(
            Duration::from_millis(200),
            buffer.enqueue_items(0, vec![make_flight_data(4)]),
        )
        .await;
        assert!(
            result.is_ok(),
            "enqueue_items should not block after send_frame error"
        );
        let error = result
            .unwrap()
            .expect_err("later enqueue_items should observe the exchange error");
        assert_eq!(error.code(), ErrorCode::ABORTED_QUERY);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_direct_send_closed_returns_aborted_query() {
        let rt = test_runtime();
        let (exchange, send_rx, _pong_tx) = create_mock_exchange(1);
        let buffer =
            ExchangeSendBuffer::create(vec![exchange], ExchangeSendBufferConfig::default(), &rt)
                .unwrap();

        drop(send_rx);

        let error = buffer
            .enqueue_items(0, vec![make_flight_data(1)])
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
    async fn test_send_frame_error_unblocks_waiting_enqueue_items() {
        let rt = test_runtime();
        let (exchange, send_rx, pong_tx) = create_mock_exchange(1);
        let buffer = Arc::new(
            ExchangeSendBuffer::create(
                vec![exchange],
                ExchangeSendBufferConfig {
                    max_queued_bytes: 3,
                    max_batch_bytes: 1,
                },
                &rt,
            )
            .unwrap(),
        );

        buffer
            .enqueue_items(0, vec![make_flight_data(1)])
            .await
            .unwrap();
        let _ = send_rx.recv().await.unwrap();

        buffer
            .enqueue_items(0, vec![make_flight_data(2)])
            .await
            .unwrap();

        let mut blocked = Box::pin(buffer.enqueue_items(0, vec![make_flight_data(2)]));
        assert!(
            futures::poll!(blocked.as_mut()).is_pending(),
            "third enqueue_items should wait for the pending-queue semaphore"
        );

        drop(send_rx);
        pong_tx.send(Ok(FlightData::default())).await.unwrap();

        let result = tokio::time::timeout(Duration::from_secs(2), blocked)
            .await
            .expect("blocked enqueue_items should be released after remote close");
        let error = result.expect_err("released enqueue_items should observe exchange abort");
        assert_eq!(error.code(), ErrorCode::ABORTED_QUERY);
        assert_eq!(
            buffer.semaphore.available_permits(),
            3,
            "closing should release the queued and in-flight permits"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_response_failure_unblocks_waiting_enqueue_items() {
        for status in [None, Some(Status::unavailable("peer disconnected"))] {
            let rt = test_runtime();
            let (exchange, send_rx, pong_tx) = create_mock_exchange(1);
            let buffer = ExchangeSendBuffer::create(
                vec![exchange],
                ExchangeSendBufferConfig {
                    max_queued_bytes: 3,
                    ..Default::default()
                },
                &rt,
            )
            .unwrap();

            buffer
                .enqueue_items(0, vec![make_flight_data(1)])
                .await
                .unwrap();
            let _ = send_rx.recv().await.unwrap();
            buffer
                .enqueue_items(0, vec![make_flight_data(2)])
                .await
                .unwrap();

            let mut blocked = Box::pin(buffer.enqueue_items(0, vec![make_flight_data(2)]));
            assert!(futures::poll!(blocked.as_mut()).is_pending());
            let expected_code = if let Some(status) = status {
                pong_tx.send(Err(status)).await.unwrap();
                ErrorCode::CANNOT_CONNECT_NODE
            } else {
                ErrorCode::ABORTED_QUERY
            };
            drop(pong_tx);

            let error = tokio::time::timeout(Duration::from_secs(2), blocked)
                .await
                .expect("response failure must wake a sender waiting for quota")
                .expect_err("a waiting sender must observe the connection failure");
            assert_eq!(error.code(), expected_code);
            assert!(
                error
                    .message()
                    .contains("client=query-node-0, service=query-node-1")
            );
            assert!(buffer.is_closed(0));
            assert_eq!(buffer.semaphore.available_permits(), 3);

            let later_error = buffer
                .enqueue_items(0, vec![make_flight_data(1)])
                .await
                .expect_err("a failed destination must reject subsequent data");
            assert_eq!(later_error.message(), error.message());
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_frames_fill_free_slots_and_batch_behind_them() {
        let rt = test_runtime();
        let (exchange, send_rx, pong_tx) = create_mock_exchange_with_slots(1, MAX_FRAMES_IN_FLIGHT);
        let buffer =
            ExchangeSendBuffer::create(vec![exchange], ExchangeSendBufferConfig::default(), &rt)
                .unwrap();

        // Each enqueue finds a free slot until all of them are in flight.
        for len in 1..=MAX_FRAMES_IN_FLIGHT + 2 {
            buffer
                .enqueue_items(0, vec![make_flight_data(len)])
                .await
                .unwrap();
        }
        for len in 1..=MAX_FRAMES_IN_FLIGHT {
            assert_eq!(send_rx.recv().await.unwrap().data_body.len(), len);
        }
        assert!(send_rx.try_recv().is_err());

        // A response frees one slot, which carries both queued items in one frame.
        pong_tx.send(Ok(FlightData::default())).await.unwrap();
        let frame = tokio::time::timeout(Duration::from_secs(2), send_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(is_batch(&frame));
        let lens: Vec<_> = split_batch_flight_data(frame)
            .into_iter()
            .map(|item| item.data_body.len())
            .collect();
        assert_eq!(lens, vec![
            MAX_FRAMES_IN_FLIGHT + 1,
            MAX_FRAMES_IN_FLIGHT + 2
        ]);

        for _ in 0..MAX_FRAMES_IN_FLIGHT {
            pong_tx.send(Ok(FlightData::default())).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(send_rx.try_recv().is_err());
        assert_eq!(buffer.semaphore.available_permits(), buffer.max_permits);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_bytes_in_flight_hold_back_frames_until_acknowledged() {
        let rt = test_runtime();
        let (exchange, send_rx, pong_tx) = create_mock_exchange_with_slots(1, MAX_FRAMES_IN_FLIGHT);
        // Frames of at most 100 bytes, so 100 * MAX_FRAMES_IN_FLIGHT bytes in flight.
        let buffer = ExchangeSendBuffer::create(
            vec![exchange],
            ExchangeSendBufferConfig {
                max_batch_bytes: 100,
                ..Default::default()
            },
            &rt,
        )
        .unwrap();
        let max_in_flight = 100 * MAX_FRAMES_IN_FLIGHT;

        // An oversized item goes out alone while nothing is in flight.
        buffer
            .enqueue_items(0, vec![make_flight_data(max_in_flight - 100)])
            .await
            .unwrap();
        buffer
            .enqueue_items(0, vec![make_flight_data(60)])
            .await
            .unwrap();
        assert_eq!(
            send_rx.recv().await.unwrap().data_body.len(),
            max_in_flight - 100
        );
        assert_eq!(send_rx.recv().await.unwrap().data_body.len(), 60);

        // Free slots remain, but this item would exceed the bytes in flight.
        buffer
            .enqueue_items(0, vec![make_flight_data(50)])
            .await
            .unwrap();
        assert!(send_rx.try_recv().is_err());

        // Acknowledging the oversized frame makes room for it.
        pong_tx.send(Ok(FlightData::default())).await.unwrap();
        let frame = tokio::time::timeout(Duration::from_secs(2), send_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame.data_body.len(), 50);

        for _ in 0..2 {
            pong_tx.send(Ok(FlightData::default())).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(buffer.semaphore.available_permits(), buffer.max_permits);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_profile_records_bytes_and_time_held_back() {
        let profile = Arc::new(Profile::create(
            0,
            "HashSendSink".to_string(),
            Some(0),
            None,
            None,
            Arc::new(String::new()),
            Arc::new(vec![]),
            None,
        ));
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.profile = Some(profile.clone());
        let _guard = ThreadTracker::tracking(payload);

        let rt = test_runtime();
        let (exchange, send_rx, pong_tx) = create_mock_exchange_with_slots(1, MAX_FRAMES_IN_FLIGHT);
        let buffer =
            ExchangeSendBuffer::create(vec![exchange], ExchangeSendBufferConfig::default(), &rt)
                .unwrap();

        // Every slot carries a frame, and two items wait behind them.
        for len in 1..=MAX_FRAMES_IN_FLIGHT + 2 {
            buffer
                .enqueue_items(0, vec![make_flight_data(len)])
                .await
                .unwrap();
        }
        for _ in 0..MAX_FRAMES_IN_FLIGHT {
            send_rx.recv().await.unwrap();
        }
        let wait = Duration::from_millis(20);
        tokio::time::sleep(wait).await;

        // The first response sends both waiting items in one frame; the network
        // callback records into the profile of the processor that enqueued them.
        for _ in 0..MAX_FRAMES_IN_FLIGHT + 1 {
            pong_tx.send(Ok(FlightData::default())).await.unwrap();
        }
        tokio::time::timeout(Duration::from_secs(2), send_rx.recv())
            .await
            .unwrap()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let load = |name| profile.load_profile(name);
        assert_eq!(
            load(ProfileStatisticsName::ExchangeBytes),
            (1..=MAX_FRAMES_IN_FLIGHT + 2).sum::<usize>()
        );
        assert_eq!(load(ProfileStatisticsName::ExchangeSendQueueWaitTime), 0);
        assert!(load(ProfileStatisticsName::ExchangeHeldBackTime) >= wait.as_nanos() as usize);
    }
}
