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

//! Per-connection memory quota and backpressure primitives.
//!
//! Each network connection gets a `ConnectionQuota` that tracks bytes in flight.
//! `SubQueue` is the per-connection, per-tid queue. `SendFuture` implements
//! the try-listen-retry pattern for pushing data with backpressure.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use arrow_flight::FlightData;
use concurrent_queue::ConcurrentQueue;
use concurrent_queue::PushError;
use event_listener::Event;

use super::inbound_channel::flight_data_size;

/// Item stored in a per-connection sub-queue.
pub struct QueueItem {
    pub data: FlightData,
    /// Size in bytes, computed at push time. Must match quota accounting.
    pub size: usize,
}

/// Per-connection sub-queue within a NetworkInboundChannel.
pub struct SubQueue {
    /// Unbounded concurrent queue. Backpressure is on ConnectionQuota, not here.
    pub queue: ConcurrentQueue<QueueItem>,
    /// The owning connection's quota (for priority comparison and release).
    pub quota: Arc<ConnectionQuota>,
}

impl SubQueue {
    /// Send data to this sub-queue with backpressure.
    ///
    /// Returns a `SendFuture` that pushes data, notifies the processor,
    /// and waits for backpressure to clear. Follows async_channel's
    /// try-listen-retry pattern.
    pub fn send<'a>(&'a self, data: FlightData, recv_event: &'a Event) -> SendFuture<'a> {
        let size = flight_data_size(&data);
        SendFuture {
            queue: &self.queue,
            quota: &self.quota,
            recv_event,
            item: Some(QueueItem { data, size }),
            size,
            listener: None,
        }
    }
}

/// Future that sends one item to a `SubQueue` with backpressure.
///
/// Encapsulates the try-listen-retry pattern (same as async_channel's `Send`):
/// 1. Push data to queue, account bytes in quota, notify processor
/// 2. Check backpressure (quota under limit) → if yes, return Ready
/// 3. If no listener, create one and continue (retry)
/// 4. Poll listener → if Pending, return Pending; if Ready, loop back to 2
pub struct SendFuture<'a> {
    queue: &'a ConcurrentQueue<QueueItem>,
    quota: &'a ConnectionQuota,
    recv_event: &'a Event,
    item: Option<QueueItem>,
    size: usize,
    listener: Option<Pin<Box<event_listener::EventListener>>>,
}

impl Future for SendFuture<'_> {
    /// `Ok(())` = data sent and backpressure cleared.
    /// `Err(())` = queue closed (receiver dropped).
    type Output = Result<(), ()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            // Phase 1: Push data if not yet pushed
            if let Some(item) = this.item.take() {
                match this.queue.push(item) {
                    Ok(()) => {
                        this.quota.add_bytes(this.size);
                        this.recv_event.notify_additional(1);
                    }
                    Err(PushError::Closed(_)) => return Poll::Ready(Err(())),
                    Err(PushError::Full(_)) => unreachable!("unbounded queue"),
                }
            }

            // Phase 2: Backpressure - try-listen-retry
            if this.quota.current_bytes.load(Ordering::Acquire) <= this.quota.max_bytes {
                return Poll::Ready(Ok(()));
            }

            if this.listener.is_none() {
                this.listener = Some(Box::pin(this.quota.send_ops.listen()));
                continue;
            }

            match this.listener.as_mut().unwrap().as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(()) => {
                    this.listener = None;
                }
            }
        }
    }
}

/// Per-connection memory quota. Controls backpressure per network connection.
///
/// Only one atomic (`current_bytes`). No multi-atomic coordination.
/// Uses `event_listener::Event` for release notification (network side is async).
pub struct ConnectionQuota {
    /// Total bytes from this connection sitting in all tid queues (not yet consumed).
    pub(super) current_bytes: AtomicUsize,
    /// Memory limit per connection.
    pub(super) max_bytes: usize,
    /// Notified when processor consumes data (releases bytes).
    pub(super) send_ops: Event,
}

impl ConnectionQuota {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            current_bytes: AtomicUsize::new(0),
            max_bytes,
            send_ops: Event::new(),
        }
    }

    /// Add bytes to the quota (called at push time, before entering queue).
    pub fn add_bytes(&self, size: usize) {
        self.current_bytes.fetch_add(size, Ordering::AcqRel);
    }

    /// Release bytes from the quota (called when processor consumes data).
    /// Unconditionally notifies to avoid multi-atomic coordination.
    pub fn release(&self, size: usize) {
        self.current_bytes.fetch_sub(size, Ordering::AcqRel);
        self.send_ops.notify_additional(1);
    }

    /// Current bytes in flight from this connection.
    pub fn current_bytes(&self) -> usize {
        self.current_bytes.load(Ordering::Relaxed)
    }
}
