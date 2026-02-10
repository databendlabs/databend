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
use std::task::Poll;
use std::task::Waker;

use arrow_flight::FlightData;
use databend_common_exception::Result;
use parking_lot::Mutex;
use tonic::Status;

use super::outbound_transport::PingPongCallback;
use super::outbound_transport::PingPongExchange;
use super::outbound_transport::PingPongResponse;

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
            max_batch_bytes: 256 * 1024, // 256KB, same as Doris
        }
    }
}

/// Per-sink channel containing its own pending queue.
struct Channel {
    pending_queue: VecDeque<FlightData>,
}

impl Channel {
    fn new() -> Self {
        Self {
            pending_queue: VecDeque::new(),
        }
    }

    fn remaining(&self) -> usize {
        self.pending_queue.len()
    }

    fn pop_front(&mut self, _max_batch_bytes: usize) -> Option<(usize, FlightData)> {
        self.pending_queue.pop_front().map(|x| (1, x))
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
    fn new(num_channels: usize, exchange: PingPongExchange) -> Self {
        let channels = (0..num_channels).map(|_| Channel::new()).collect();
        Self {
            state: Mutex::new(RemoteInstanceState {
                channels,
                last_error: None,
            }),
            exchange,
        }
    }
}

/// Inner state of ExchangeSinkBuffer, shared with callbacks.
struct ExchangeSinkBufferInner {
    config: ExchangeBufferConfig,

    /// Pre-allocated remote instances, indexed by destination index
    remotes: Vec<Arc<RemoteInstance>>,
    queue_capacity: usize,
    queue_size: AtomicUsize,
    /// Blocked sender wakers
    blocked_wakers: Mutex<Vec<Waker>>,
}

impl ExchangeSinkBufferInner {
    fn try_flush_remote(&self, dest_idx: usize, status: Option<Status>) {
        let remote = &self.remotes[dest_idx];
        let mut state = remote.state.lock();

        if let Some(status) = status {
            state.last_error = Some(status);

            let remaining = state.channels.iter().map(|x| x.remaining()).sum::<usize>();
            self.queue_size.fetch_sub(remaining, Ordering::SeqCst);
            return self.wake_blocked();
        }

        // Find channel with max queue size
        if let Some(channel) = state.channels.iter_mut().max_by_key(|x| x.remaining()) {
            let Some((batch, flight)) = channel.pop_front(self.config.max_batch_bytes) else {
                return remote.exchange.ready_send();
            };

            let Ok(_) = remote.exchange.force_send(flight) else {
                state.last_error = Some(Status::aborted("Exchange closed"));
                return;
            };

            // Sent successfully
            self.queue_size.fetch_sub(batch, Ordering::SeqCst);
            self.wake_blocked();
        }
    }

    fn wake_blocked(&self) {
        if self.queue_size.load(Ordering::SeqCst) <= self.queue_capacity {
            let mut wakers = self.blocked_wakers.lock();
            for waker in wakers.drain(..) {
                waker.wake();
            }
        }
    }
}

/// Callback for handling PingPong responses.
struct SinkBufferCallback {
    buffer: Weak<ExchangeSinkBufferInner>,
    dest_idx: usize,
}

impl PingPongCallback for SinkBufferCallback {
    fn on_response(&self, response: PingPongResponse) {
        let Some(buffer) = self.buffer.upgrade() else {
            return;
        };

        buffer.try_flush_remote(self.dest_idx, response.data.err());
    }
}

/// Exchange sink buffer for controlling backpressure in distributed data exchange.
///
/// This buffer manages multiple remote instances (one per destination node) and uses
/// ping-pong mode to ensure at most one request is in-flight per instance at any time.
///
/// Uses `std::task::Waker` for backpressure signaling.
pub struct ExchangeSinkBuffer {
    inner: Arc<ExchangeSinkBufferInner>,
}

impl ExchangeSinkBuffer {
    /// Create a new ExchangeSinkBuffer.
    ///
    /// - `exchanges`: Pre-created PingPongExchange instances (not yet started).
    /// - `num_channels`: Number of channels per remote instance.
    /// - `config`: Buffer configuration.
    pub fn create(
        exchanges: Vec<PingPongExchange>,
        num_channels: usize,
        config: ExchangeBufferConfig,
    ) -> Result<Self> {
        let queue_capacity = config.queue_capacity_factor * exchanges.len().max(1);

        Ok(Self {
            inner: Arc::new_cyclic(|weak_inner| {
                let mut remotes = Vec::with_capacity(exchanges.len());

                for (dest_idx, exchange) in exchanges.into_iter().enumerate() {
                    let _ = exchange.start(Arc::new(SinkBufferCallback {
                        dest_idx,
                        buffer: weak_inner.clone(),
                    }));

                    remotes.push(Arc::new(RemoteInstance::new(num_channels, exchange)));
                }

                ExchangeSinkBufferInner {
                    config,
                    remotes,
                    queue_capacity,
                    queue_size: AtomicUsize::new(0),
                    blocked_wakers: Mutex::new(Vec::new()),
                }
            }),
        })
    }

    /// Poll to send data to the specified destination.
    ///
    /// - `channel_id`: Channel identifier.
    /// - `dest_idx`: Destination index.
    /// - `data`: Data to send.
    /// - `waker`: Waker to be notified when backpressure is released.
    ///
    /// Returns:
    /// - `Poll::Ready(Ok(()))`: Data accepted.
    /// - `Poll::Ready(Err(...))`: Error from the destination.
    /// - `Poll::Pending`: Backpressure, waker registered.
    pub fn poll_send(
        &self,
        tid: usize,
        dest_idx: usize,
        data: FlightData,
        waker: &Waker,
    ) -> Poll<Result<()>> {
        let remote = &self.inner.remotes[dest_idx];

        // Try to send directly first
        if let Some(data) = remote.exchange.try_send(data)? {
            // Failed to send (in-flight or channel full), queue the data
            let mut state = remote.state.lock();

            // Check for previous error
            if let Some(status) = state.last_error.take() {
                return Poll::Ready(Err(status.into()));
            }

            state.channels[tid].pending_queue.push_back(data);

            // Check backpressure
            if self.inner.queue_size.fetch_add(1, Ordering::SeqCst) >= self.inner.queue_capacity {
                let mut wakers = self.inner.blocked_wakers.lock();
                wakers.push(waker.clone());
                return Poll::Pending;
            }
        }

        Poll::Ready(Ok(()))
    }
}
