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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use arrow_flight::FlightData;
use async_channel::Receiver;
use async_channel::Sender;
use tokio::sync::Semaphore;

use super::super::wire_format::extract_tid;
use super::super::wire_format::flight_data_size;
use super::super::wire_format::is_batch;
use super::super::wire_format::split_batch_flight_data;
use super::exchange_queue::ExchangeReceiveChannels;
use super::exchange_queue::QueueItem;
use super::exchange_queue::RemoteQueueItem;

/// Routes one do_exchange connection into the exchange's per-thread channels.
///
/// Registers one producer per channel; dropping the last producer closes that
/// channel so its consumer can observe EOF after draining queued items.
pub struct ExchangeFrameRouter {
    /// This connection's sub-queue in each tid's ExchangeReceiveChannel.
    sub_queues: Vec<Arc<ConnectionReceiveQueue>>,
}

impl ExchangeFrameRouter {
    /// Bind this connection to the receive channels and share its byte quota
    /// across all destination threads.
    pub fn new(channel_set: &ExchangeReceiveChannels, max_bytes_per_connection: usize) -> Self {
        let semaphore = Arc::new(Semaphore::new(max_bytes_per_connection));
        let mut sub_queues = Vec::with_capacity(channel_set.channels.len());

        for channel in channel_set.channels.iter() {
            channel.sender_count.fetch_add(1, Ordering::AcqRel);

            let sub_queue = Arc::new(ConnectionReceiveQueue {
                max_bytes_per_connection,
                sender: channel.sender.clone(),
                receiver: channel.receiver.clone(),
                semaphore: semaphore.clone(),
                sender_count: channel.sender_count.clone(),
            });

            sub_queues.push(sub_queue);
        }

        Self { sub_queues }
    }

    /// Add data to the inbound channel.
    ///
    /// Extracts tid from the FlightData, pushes to the appropriate sub-queue,
    /// and waits for backpressure to clear.
    ///
    /// Returns `Err(())` only when ALL receivers are closed (network should disconnect).
    /// If only the target tid's receiver is closed, discards the data and returns `Ok(())`.
    #[allow(
        clippy::result_unit_err,
        reason = "Callers only need a disconnect signal"
    )]
    pub async fn add_data(&self, data: FlightData) -> Result<(), ()> {
        if is_batch(&data) {
            return self.add_batch_data(data).await;
        }

        let tid = extract_tid(&data);

        match self.sub_queues[tid].add_data(data).await {
            Ok(()) => Ok(()),
            Err(()) => match self.all_receivers_closed() {
                true => Err(()),
                false => Ok(()),
            },
        }
    }

    async fn add_batch_data(&self, data: FlightData) -> Result<(), ()> {
        let items = split_batch_flight_data(data);
        for item in items {
            let tid = extract_tid(&item);
            match self.sub_queues[tid].add_data(item).await {
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

    /// Check if all channels are closed by receivers.
    pub fn all_receivers_closed(&self) -> bool {
        self.sub_queues.iter().all(|q| q.sender.is_closed())
    }
}

impl Drop for ExchangeFrameRouter {
    fn drop(&mut self) {
        for sub_queue in &self.sub_queues {
            if sub_queue.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
                sub_queue.sender.close();
            }
        }
    }
}

/// A connection's handle to one thread channel, sharing quota with its siblings.
pub struct ConnectionReceiveQueue {
    /// The owning connection's quota (for priority comparison and release).
    pub semaphore: Arc<Semaphore>,

    pub max_bytes_per_connection: usize,

    pub sender: Sender<QueueItem>,

    pub receiver: Receiver<QueueItem>,

    pub sender_count: Arc<AtomicUsize>,
}

impl ConnectionReceiveQueue {
    #[allow(
        clippy::result_unit_err,
        reason = "Callers only need a disconnect signal"
    )]
    pub async fn add_data(&self, data: FlightData) -> Result<(), ()> {
        let size = flight_data_size(&data);
        let size = std::cmp::min(size, self.max_bytes_per_connection);

        let semaphore = self.semaphore.clone();
        if let Ok(x) = semaphore.try_acquire_many_owned(size as u32) {
            let item = RemoteQueueItem::create(data, x);

            if let Err(cause) = self.sender.try_send(item) {
                if cause.is_full() {
                    log::error!("Logical error, inbound quota queue is full");
                }

                return Err(());
            }

            return Ok(());
        }

        let semaphore = self.semaphore.clone();
        let Ok(x) = semaphore.acquire_many_owned(size as u32).await else {
            log::error!("Logical error, inbound quota semaphore is closed.");
            return Err(());
        };

        let item = RemoteQueueItem::create(data, x);
        if let Err(cause) = self.sender.try_send(item) {
            if cause.is_full() {
                log::error!("Logical error, inbound quota queue is full");
            }
            return Err(());
        }

        Ok(())
    }
}
