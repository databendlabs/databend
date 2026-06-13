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

use async_channel::Sender;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

use crate::servers::flight::v1::network::NetworkInboundChannelSet;
use crate::servers::flight::v1::network::OutboundChannel;
use crate::servers::flight::v1::network::inbound_quota::QueueItem;

pub struct LocalQueueItem {
    data: DataBlock,
    owned_semaphore_permit: OwnedSemaphorePermit,
}

impl LocalQueueItem {
    pub fn create(data: DataBlock, owned_semaphore_permit: OwnedSemaphorePermit) -> QueueItem {
        QueueItem::LocalData(Self {
            data,
            owned_semaphore_permit,
        })
    }

    pub fn into_data(self) -> DataBlock {
        drop(self.owned_semaphore_permit);
        self.data
    }
}

pub struct LocalOutboundChannel {
    semaphore: Arc<Semaphore>,
    sender: Sender<QueueItem>,
    sender_count: Arc<AtomicUsize>,
    max_bytes_local_channel: usize,
}

#[async_trait::async_trait]
impl OutboundChannel for LocalOutboundChannel {
    fn close(&self) {}

    fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    async fn add_block(&self, block: DataBlock) -> Result<()> {
        let size = block.memory_size();
        let size = std::cmp::min(size, self.max_bytes_local_channel);

        let semaphore = self.semaphore.clone();
        if let Ok(x) = semaphore.try_acquire_many_owned(size as u32) {
            let item = LocalQueueItem::create(block, x);

            if let Err(cause) = self.sender.try_send(item) {
                if cause.is_full() {
                    unreachable!("Logical error, local channel quota queue is full");
                }
            }

            return Ok(());
        }

        let semaphore = self.semaphore.clone();
        let Ok(x) = semaphore.acquire_many_owned(size as u32).await else {
            return Err(ErrorCode::Internal(
                "Logical error, inbound quota semaphore is closed.",
            ));
        };

        let item = LocalQueueItem::create(block, x);
        if let Err(cause) = self.sender.try_send(item) {
            if cause.is_full() {
                unreachable!("Logical error, local channel quota queue is full");
            }
        }

        Ok(())
    }
}

impl Drop for LocalOutboundChannel {
    fn drop(&mut self) {
        if self.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.sender.close();
        }
    }
}

/// Max bytes for local channel backpressure (20 MB).
pub const LOCAL_CHANNEL_MAX_BYTES: usize = 20 * 1024 * 1024;

pub fn create_local_channels(
    channel_set: &NetworkInboundChannelSet,
) -> Vec<Arc<dyn OutboundChannel>> {
    let semaphore = Arc::new(Semaphore::new(LOCAL_CHANNEL_MAX_BYTES));

    let mut outbound = Vec::<Arc<dyn OutboundChannel>>::with_capacity(channel_set.channels.len());
    for channel in channel_set.channels.iter() {
        channel.sender_count.fetch_add(1, Ordering::AcqRel);

        outbound.push(Arc::new(LocalOutboundChannel {
            semaphore: semaphore.clone(),
            sender: channel.sender.clone(),
            sender_count: channel.sender_count.clone(),
            max_bytes_local_channel: LOCAL_CHANNEL_MAX_BYTES,
        }));
    }

    outbound
}
