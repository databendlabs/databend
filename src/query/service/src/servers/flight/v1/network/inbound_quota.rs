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

use arrow_flight::FlightData;
use async_channel::Receiver;
use async_channel::Sender;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

use super::inbound_channel::flight_data_size;
use crate::servers::flight::v1::network::local_channel::LocalQueueItem;

/// Item stored in a per-connection sub-queue.
pub struct RemoteQueueItem {
    pub data: FlightData,
    owned_semaphore_permit: OwnedSemaphorePermit,
}

pub enum QueueItem {
    LocalData(LocalQueueItem),
    RemoteData(RemoteQueueItem),
}

impl RemoteQueueItem {
    pub fn create(data: FlightData, owned_semaphore_permit: OwnedSemaphorePermit) -> QueueItem {
        QueueItem::RemoteData(RemoteQueueItem {
            data,
            owned_semaphore_permit,
        })
    }

    pub fn into_data(self) -> FlightData {
        drop(self.owned_semaphore_permit);
        self.data
    }
}

/// Per-connection sub-queue within a NetworkInboundChannel.
pub struct SubQueue {
    /// The owning connection's quota (for priority comparison and release).
    pub semaphore: Arc<Semaphore>,

    pub max_bytes_per_connection: usize,

    pub sender: Sender<QueueItem>,

    pub receiver: Receiver<QueueItem>,

    pub sender_count: Arc<AtomicUsize>,
}

impl SubQueue {
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
