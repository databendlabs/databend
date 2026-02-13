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

//! Local in-memory channel pair for same-node exchanges.
//!
//! Passes `DataBlock`s directly through shared concurrent queues with zero
//! serialization. Backpressure is shared across all channels via a single
//! `AtomicUsize` tracking total bytes in flight.
//!
//! ```text
//! Writer 0 ──▶ queue[0] ──▶ Reader 0
//! Writer 1 ──▶ queue[1] ──▶ Reader 1
//!              └── total_bytes (shared backpressure) ──┘
//! ```

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::task::Waker;

use concurrent_queue::ConcurrentQueue;
use concurrent_queue::PopError;
use concurrent_queue::PushError;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use event_listener::Event;
use parking_lot::Mutex;

use super::inbound_channel::InboundChannel;
use super::outbound_channel::OutboundChannel;

struct SizedBlock {
    block: DataBlock,
    size: usize,
}

/// Shared state across all local channels in one exchange.
struct LocalChannelSharedState {
    /// One unbounded queue per channel. Backpressure is on total_bytes, not queue capacity.
    queues: Vec<ConcurrentQueue<SizedBlock>>,
    /// Total bytes across ALL channels (shared backpressure).
    total_bytes: AtomicUsize,
    /// Memory limit for backpressure.
    max_bytes: usize,
    /// Per-channel event: notifies receiver when data is pushed.
    recv_events: Vec<Event>,
    /// Blocked sender wakers (replaces send_event).
    blocked_wakers: Arc<Mutex<Vec<Waker>>>,
}

impl LocalChannelSharedState {
    fn wake_blocked(&self) {
        if self.total_bytes.load(Ordering::Acquire) <= self.max_bytes {
            let mut wakers = self.blocked_wakers.lock();
            for waker in wakers.drain(..) {
                waker.wake();
            }
        }
    }
}

/// Outbound side of a local channel, bound to one `channel_idx`.
///
/// Dropping closes the queue and notifies the receiver.
pub struct LocalOutboundChannel {
    channel_idx: usize,
    state: Arc<LocalChannelSharedState>,
}

#[async_trait::async_trait]
impl OutboundChannel for LocalOutboundChannel {
    fn close(&self) {
        self.state.queues[self.channel_idx].close();
        self.state.recv_events[self.channel_idx].notify(usize::MAX);
    }

    fn is_closed(&self) -> bool {
        self.state.queues[self.channel_idx].is_closed()
    }

    async fn add_block(&self, block: DataBlock) -> Result<()> {
        let size = block.memory_size();
        let item = SizedBlock { block, size };

        match self.state.queues[self.channel_idx].push(item) {
            Ok(()) => {}
            Err(PushError::Closed(_)) => {
                return Err(ErrorCode::AbortedQuery("Local channel closed"));
            }
            Err(PushError::Full(_)) => unreachable!("unbounded queue"),
        }

        self.state.total_bytes.fetch_add(size, Ordering::AcqRel);
        self.state.recv_events[self.channel_idx].notify_additional(1);

        // Backpressure: if over limit, register waker and pend once
        if self.state.total_bytes.load(Ordering::Acquire) > self.state.max_bytes {
            let mut registered = false;
            let blocked_wakers = self.state.blocked_wakers.clone();
            std::future::poll_fn(|cx| {
                if registered {
                    return Poll::Ready(());
                }
                registered = true;
                blocked_wakers.lock().push(cx.waker().clone());
                Poll::Pending
            })
            .await;
        }

        Ok(())
    }
}

impl Drop for LocalOutboundChannel {
    fn drop(&mut self) {
        self.state.queues[self.channel_idx].close();
        self.state.recv_events[self.channel_idx].notify(usize::MAX);
    }
}

/// Inbound side of a local channel, bound to one `channel_idx`.
///
/// Dropping closes the queue and notifies senders.
pub struct LocalInboundChannel {
    channel_idx: usize,
    state: Arc<LocalChannelSharedState>,
}

#[async_trait::async_trait]
impl InboundChannel for LocalInboundChannel {
    fn close(&self) {
        self.state.queues[self.channel_idx].close();
        // Wake blocked senders so they can detect closed state
        let mut wakers = self.state.blocked_wakers.lock();
        for waker in wakers.drain(..) {
            waker.wake();
        }
        self.state.recv_events[self.channel_idx].notify(usize::MAX);
    }

    fn is_closed(&self) -> bool {
        self.state.queues[self.channel_idx].is_closed()
    }

    async fn recv(&self) -> std::result::Result<Option<DataBlock>, ErrorCode> {
        loop {
            match self.state.queues[self.channel_idx].pop() {
                Ok(item) => {
                    self.state
                        .total_bytes
                        .fetch_sub(item.size, Ordering::AcqRel);
                    self.state.wake_blocked();
                    return Ok(Some(item.block));
                }
                Err(PopError::Closed) => return Ok(None),
                Err(PopError::Empty) => {}
            }

            let listener = self.state.recv_events[self.channel_idx].listen();

            // Retry after creating listener (catch race between pop and listen)
            match self.state.queues[self.channel_idx].pop() {
                Ok(item) => {
                    self.state
                        .total_bytes
                        .fetch_sub(item.size, Ordering::AcqRel);
                    self.state.wake_blocked();
                    return Ok(Some(item.block));
                }
                Err(PopError::Closed) => return Ok(None),
                Err(PopError::Empty) => {}
            }

            listener.await;
        }
    }
}

impl Drop for LocalInboundChannel {
    fn drop(&mut self) {
        self.close();
    }
}

/// Create matched pairs of local outbound/inbound channels.
///
/// All channels share a single memory quota (`max_bytes`). Senders block when
/// the total bytes across all queues exceeds the limit; receivers releasing
/// data unblocks them.
#[allow(clippy::type_complexity)]
pub fn create_local_channels(
    num_channels: usize,
    max_bytes: usize,
) -> (Vec<Arc<dyn OutboundChannel>>, Vec<Arc<dyn InboundChannel>>) {
    let state = Arc::new(LocalChannelSharedState {
        queues: (0..num_channels)
            .map(|_| ConcurrentQueue::unbounded())
            .collect(),
        total_bytes: AtomicUsize::new(0),
        max_bytes,
        recv_events: (0..num_channels).map(|_| Event::new()).collect(),
        blocked_wakers: Arc::new(Mutex::new(Vec::new())),
    });

    let outbound: Vec<Arc<dyn OutboundChannel>> = (0..num_channels)
        .map(|i| {
            Arc::new(LocalOutboundChannel {
                channel_idx: i,
                state: state.clone(),
            }) as Arc<dyn OutboundChannel>
        })
        .collect();

    let inbound: Vec<Arc<dyn InboundChannel>> = (0..num_channels)
        .map(|i| {
            Arc::new(LocalInboundChannel {
                channel_idx: i,
                state: state.clone(),
            }) as Arc<dyn InboundChannel>
        })
        .collect();

    (outbound, inbound)
}
