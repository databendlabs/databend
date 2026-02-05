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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use arrow_flight::FlightData;
use concurrent_queue::ConcurrentQueue;
use databend_common_exception::Result;
use databend_common_pipeline::core::ExecutorWaker;
use parking_lot::Mutex;
use petgraph::stable_graph::NodeIndex;
use tonic::Status;

use super::PingPongCallback;
use super::PingPongExchange;
use super::PingPongResponse;

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

/// Wake target identity for backpressure wake-up.
pub struct WakeTarget {
    pub pid: NodeIndex,
    pub worker_id: usize,
    blocked: AtomicBool,
}

impl WakeTarget {
    pub fn new(pid: NodeIndex, worker_id: usize) -> Arc<Self> {
        Arc::new(Self {
            pid,
            worker_id,
            blocked: AtomicBool::new(false),
        })
    }

    pub fn is_blocked(&self) -> bool {
        self.blocked.load(Ordering::SeqCst)
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
        // let mut batch = Vec::new();
        // let mut batch_size = 0usize;

        self.pending_queue.pop_front().map(|x| (1, x))
        // while let Some(data) = self.pending_queue.front() {
        //     let data_size = data.data_body.len();
        //
        //     if batch_size + data_size > max_batch_bytes && !batch.is_empty() {
        //         break;
        //     }
        //     batch.push(self.pending_queue.pop_front().unwrap());
        //     batch_size += data_size;
        // }
        //
        // Some((1, batch[0]))
    }
}

/// Mutable state within RemoteInstance, protected by its own lock.
struct RemoteInstanceState {
    /// Pre-allocated channels, indexed by channel_id
    channels: Vec<Channel>,
    /// Last error from the exchange, returned on next add_block call
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
    waker: Arc<ExecutorWaker>,
    /// Use ConcurrentQueue for lock-free wake target collection
    blocked_targets: ConcurrentQueue<Arc<WakeTarget>>,
}

impl ExchangeSinkBufferInner {
    fn try_flush_remote(&self, dest_idx: usize, status: Option<Status>) {
        let remote = &self.remotes[dest_idx];
        let mut state = remote.state.lock();

        if let Some(status) = status {
            state.last_error = Some(status);

            let remaining = state.channels.iter().map(|x| x.remaining()).sum::<usize>();
            self.queue_size.fetch_sub(remaining, Ordering::SeqCst);
            return self.ready_queue();
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
            self.ready_queue();
        }
    }

    fn ready_queue(&self) {
        if self.queue_size.load(Ordering::SeqCst) <= self.queue_capacity {
            while let Ok(target) = self.blocked_targets.pop() {
                target.blocked.store(false, Ordering::SeqCst);
                let _ = self.waker.wake(target.pid, target.worker_id);
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
pub struct ExchangeSinkBuffer {
    inner: Arc<ExchangeSinkBufferInner>,
}

impl ExchangeSinkBuffer {
    /// Create a new ExchangeSinkBuffer.
    ///
    /// - `exchanges`: Pre-created PingPongExchange instances (not yet started).
    /// - `num_channels`: Number of channels per remote instance.
    /// - `waker`: ExecutorWaker for waking up blocked processors.
    /// - `config`: Buffer configuration.
    pub fn create(
        exchanges: Vec<PingPongExchange>,
        num_channels: usize,
        waker: Arc<ExecutorWaker>,
        config: ExchangeBufferConfig,
    ) -> Result<Self> {
        let queue_capacity = config.queue_capacity_factor * exchanges.len().max(1);

        Ok(Self {
            inner: Arc::new_cyclic(|weak_inner| {
                let mut remotes = Vec::with_capacity(exchanges.len());

                for (dest_idx, exchange) in exchanges.into_iter().enumerate() {
                    // Note: start() may fail, but we handle that after
                    let _ = exchange.start(Arc::new(SinkBufferCallback {
                        dest_idx,
                        buffer: weak_inner.clone(),
                    }));

                    remotes.push(Arc::new(RemoteInstance::new(num_channels, exchange)));
                }

                ExchangeSinkBufferInner {
                    waker,
                    config,
                    remotes,
                    queue_capacity,
                    queue_size: AtomicUsize::new(0),
                    blocked_targets: ConcurrentQueue::unbounded(),
                }
            }),
        })
    }

    /// Add a block to the specified destination's queue.
    ///
    /// - `wake_target`: Wake target identity for backpressure wake-up.
    /// - `channel_id`: Channel identifier (typically same as processor ID index).
    /// - `dest_idx`: Destination index.
    /// - `data`: Data to send.
    ///
    /// Returns `Ok(true)` if can continue sending, `Ok(false)` if backpressure is triggered.
    /// Returns error if the destination has a previous error.
    pub fn add_block(
        &self,
        wake_target: &Arc<WakeTarget>,
        channel_id: usize,
        dest_idx: usize,
        data: FlightData,
    ) -> Result<bool> {
        let remote = &self.inner.remotes[dest_idx];

        // Try to send directly first
        if let Some(data) = remote.exchange.try_send(data)? {
            // Failed to send (in-flight or channel full), queue the data
            let mut state = remote.state.lock();

            // Check for previous error, return it instead of queueing
            if let Some(status) = state.last_error.take() {
                return Err(status.into());
            }

            state.channels[channel_id].pending_queue.push_back(data);

            // Check backpressure
            if self.inner.queue_size.fetch_add(1, Ordering::SeqCst) >= self.inner.queue_capacity {
                // Only add to queue if not already blocked (avoid duplicates)
                if !wake_target.blocked.swap(true, Ordering::SeqCst) {
                    let _ = self.inner.blocked_targets.push(wake_target.clone());
                }

                return Ok(false);
            }
        }

        Ok(true)
    }
}
