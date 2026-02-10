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

//! Network inbound channel for do_exchange server side.
//!
//! Architecture: per tid, multiple network connections share sub-queues.
//! The processor prioritizes consuming data from the connection with the
//! highest memory usage to prevent congestion.
//!
//! ```text
//! Connection A (quota_a) ──▶ SubQueue_A ──┐
//! Connection B (quota_b) ──▶ SubQueue_B ──┼──▶ Processor (picks max quota)
//! Connection C (quota_c) ──▶ SubQueue_C ──┘
//! ```
//!
//! Thread safety:
//! - No multi-atomic coordination. Each atomic is used independently.
//! - `ConcurrentQueue`'s built-in close mechanism manages lifecycle.
//! - Processor-side notification uses `event_listener::Event` with
//!   try-listen-retry pattern (same as async_channel).
//! - Network-side backpressure uses `event_listener::Event` with
//!   try-listen-retry pattern.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use arrow_flight::FlightData;
use concurrent_queue::ConcurrentQueue;
use concurrent_queue::PopError;
use databend_common_exception::ErrorCode;
use databend_common_expression::DataBlock;
use event_listener::Event;
use futures_util::future::BoxFuture;
use parking_lot::RwLock;

use super::inbound_quota::ConnectionQuota;
use super::inbound_quota::SubQueue;

/// A single tid's inbound channel. Contains per-connection sub-queues.
///
/// Single consumer (one processor per tid). Multiple producers (network connections).
pub struct NetworkInboundChannel {
    /// Per-connection sub-queues.
    /// Write: when connection arrives (rare, write lock).
    /// Read: when processor pops (frequent, read lock).
    sub_queues: RwLock<Vec<Arc<SubQueue>>>,

    /// Processor-side notification event.
    /// Processor creates a listener when queue is empty.
    /// Network side notifies after push.
    /// Uses try-listen-retry pattern for correctness.
    recv_ops: Event,
}

impl NetworkInboundChannel {
    pub fn create() -> Self {
        Self {
            recv_ops: Event::new(),
            sub_queues: RwLock::new(Vec::new()),
        }
    }

    /// Add a sub-queue for a new connection. Called from NetworkInboundSender::new.
    pub fn add_sub_queue(&self, sq: Arc<SubQueue>) {
        self.sub_queues.write().push(sq);
    }

    fn deserialize(&self, _flight_data: FlightData) -> Result<DataBlock, ErrorCode> {
        // TODO:
        unimplemented!()
    }

    /// Non-blocking pop. Prioritizes the connection with highest `quota.current_bytes`.
    ///
    /// Single consumer guarantee: `is_empty()=false` → `pop()` succeeds
    /// (no other consumer can steal the item).
    pub fn try_pop(&self) -> Result<Result<DataBlock, ErrorCode>, PopError> {
        let sub_queues = self.sub_queues.read();

        loop {
            // Find the non-empty sub-queue whose connection has the most memory
            let mut best_idx: Option<usize> = None;
            let mut best_bytes: usize = 0;

            for (i, sq) in sub_queues.iter().enumerate() {
                if !sq.queue.is_empty() {
                    let bytes = sq.quota.current_bytes();
                    if best_idx.is_none() || bytes > best_bytes {
                        best_idx = Some(i);
                        best_bytes = bytes;
                    }
                }
            }

            return match best_idx {
                None => Err(PopError::Empty),
                Some(best_idx) => {
                    // Single consumer: is_empty()=false guarantees pop() succeeds
                    // (unless queue was closed between check and pop, which is fine)
                    return match sub_queues[best_idx].queue.pop() {
                        Ok(item) => {
                            sub_queues[best_idx].quota.release(item.size);
                            Ok(self.deserialize(item.data))
                        }
                        Err(PopError::Empty) => {
                            continue;
                        }
                        Err(PopError::Closed) => {
                            if !Self::all_closed(&sub_queues) {
                                continue;
                            }

                            Err(PopError::Closed)
                        }
                    };
                }
            };
        }
    }

    fn all_closed(sub_queues: &[Arc<SubQueue>]) -> bool {
        sub_queues.iter().all(|sq| sq.queue.is_closed())
    }

    /// Close from receiver side (e.g., limit operator finished early).
    pub fn close_by_receiver(&self) {
        let sub_queues = self.sub_queues.read();
        for sq in sub_queues.iter() {
            sq.queue.close();
            sq.quota.send_ops.notify(usize::MAX);
        }

        self.recv_ops.notify(usize::MAX);
    }
}

/// The set of NetworkInboundChannels for one channel_id.
pub struct NetworkInboundChannelSet {
    pub channels: Arc<Vec<Arc<NetworkInboundChannel>>>,
}

impl NetworkInboundChannelSet {
    pub fn new(num_threads: usize) -> Self {
        let channels = (0..num_threads)
            .map(|_| Arc::new(NetworkInboundChannel::create()))
            .collect();
        Self {
            channels: Arc::new(channels),
        }
    }
}

/// Network-side handle. Each do_exchange connection gets one.
///
/// When dropped, closes this connection's sub-queues and notifies processors.
pub struct NetworkInboundSender {
    /// This connection's sub-queue in each tid's NetworkInboundChannel.
    sub_queues: Vec<Arc<SubQueue>>,
    /// Reference to channels for notification.
    channels: Arc<Vec<Arc<NetworkInboundChannel>>>,
}

impl NetworkInboundSender {
    /// Create a new sender for a connection.
    /// Adds a sub-queue to each NetworkInboundChannel for this connection.
    pub fn new(channel_set: &NetworkInboundChannelSet, max_bytes_per_connection: usize) -> Self {
        let quota = Arc::new(ConnectionQuota::new(max_bytes_per_connection));
        let mut sub_queues = Vec::with_capacity(channel_set.channels.len());

        for channel in channel_set.channels.iter() {
            let sq = Arc::new(SubQueue {
                queue: ConcurrentQueue::unbounded(),
                quota: quota.clone(),
            });
            channel.add_sub_queue(sq.clone());
            sub_queues.push(sq);
        }

        Self {
            sub_queues,
            channels: channel_set.channels.clone(),
        }
    }

    /// Add data to the inbound channel.
    ///
    /// Extracts tid from the FlightData, pushes to the appropriate sub-queue,
    /// and waits for backpressure to clear.
    ///
    /// Returns `Err(())` only when ALL receivers are closed (network should disconnect).
    /// If only the target tid's receiver is closed, discards the data and returns `Ok(())`.
    pub async fn add_data(&self, data: FlightData) -> Result<(), ()> {
        let tid = extract_tid(&data);

        match self.sub_queues[tid]
            .send(data, &self.channels[tid].recv_ops)
            .await
        {
            Ok(()) => Ok(()),
            Err(()) => match self.all_receivers_closed() {
                true => Err(()),
                false => Ok(()),
            },
        }
    }

    /// Check if all channels are closed by receivers.
    pub fn all_receivers_closed(&self) -> bool {
        self.sub_queues.iter().all(|q| q.queue.is_closed())
    }
}

impl Drop for NetworkInboundSender {
    fn drop(&mut self) {
        // Close this connection's sub-queues
        for sq in &self.sub_queues {
            sq.queue.close();
            sq.quota.send_ops.notify(usize::MAX);
        }

        // Notify all processors so they can detect finished state
        for channel in self.channels.iter() {
            channel.recv_ops.notify(usize::MAX);
        }
    }
}

/// Trait for receiving data blocks from the network.
pub trait InboundChannel: Send + Sync {
    fn recv(&self) -> BoxFuture<'static, Option<Result<DataBlock, ErrorCode>>>;
    fn close(&self);
}

/// Processor-side handle, bound to a specific tid.
///
/// When dropped, closes the channel from receiver side.
pub struct NetworkInboundReceiver {
    channel: Arc<NetworkInboundChannel>,
}

impl NetworkInboundReceiver {
    pub fn new(channel: Arc<NetworkInboundChannel>) -> Self {
        Self { channel }
    }
}

impl InboundChannel for NetworkInboundReceiver {
    fn recv(&self) -> BoxFuture<'static, Option<Result<DataBlock, ErrorCode>>> {
        Box::pin(RecvFuture {
            channel: self.channel.clone(),
            listener: None,
        })
    }

    fn close(&self) {
        self.channel.close_by_receiver();
    }
}

impl Drop for NetworkInboundReceiver {
    fn drop(&mut self) {
        self.channel.close_by_receiver();
    }
}

/// Future that receives one item from a `NetworkInboundChannel`.
///
/// Encapsulates the try-listen-retry pattern (same as async_channel's `Recv`):
/// 1. Try pop → if data or finished, return Ready
/// 2. If no listener, create one and continue (retry)
/// 3. Poll listener → if Pending, return Pending; if Ready, loop back to 1
///
/// This future is `Unpin` because all fields are either `Arc` or `Option<Pin<Box<...>>>`.
struct RecvFuture {
    channel: Arc<NetworkInboundChannel>,
    listener: Option<Pin<Box<event_listener::EventListener>>>,
}

impl Future for RecvFuture {
    type Output = Option<Result<DataBlock, ErrorCode>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            // Try pop (handles both Data and Finished)
            match this.channel.try_pop() {
                Err(PopError::Empty) => {}
                Err(PopError::Closed) => {
                    return Poll::Ready(None);
                }
                Ok(flight_data) => {
                    this.listener = None;
                    return Poll::Ready(Some(flight_data));
                }
            }

            // Create listener if we don't have one, then retry
            if this.listener.is_none() {
                this.listener = Some(Box::pin(this.channel.recv_ops.listen()));
                // Continue to retry (catches race between try_pop and listen)
                continue;
            }

            // Poll the listener
            match this.listener.as_mut().unwrap().as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(()) => {
                    this.listener = None;
                    // Notified, loop back to try_pop
                }
            }
        }
    }
}

/// Compute the byte size of a FlightData for quota accounting.
pub fn flight_data_size(data: &FlightData) -> usize {
    data.data_body.len()
}

/// Extract tid from FlightData app_metadata (first 2 bytes, little-endian u16).
pub fn extract_tid(data: &FlightData) -> usize {
    if data.app_metadata.len() >= 2 {
        u16::from_le_bytes([data.app_metadata[0], data.app_metadata[1]]) as usize
    } else {
        0
    }
}

/// Strip the tid prefix (first 2 bytes) from FlightData app_metadata.
/// Returns the FlightData in its original format (without tid encoding).
pub fn strip_tid(mut data: FlightData) -> FlightData {
    if data.app_metadata.len() >= 2 {
        data.app_metadata = data.app_metadata.slice(2..);
    }
    data
}
