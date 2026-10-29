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
use arrow_schema::Schema as ArrowSchema;
use async_channel::Receiver;
use async_channel::Sender;
use databend_common_exception::ErrorCode;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use tokio::sync::OwnedSemaphorePermit;

use super::super::wire_format::deserialize_flight_data;
use super::super::wire_format::strip_tid;

/// Per-thread receive channels, retaining both endpoints while connecting
/// an exchange's producers and consumers.
///
/// The coordinator releases this collection once all pipelines are built. Runtime
/// producers own only senders, and consumers own only receivers; the final sender
/// drop closes the queue without a separate producer count.
pub struct ExchangeReceiveChannels {
    pub senders: Vec<Sender<QueueItem>>,
    pub receivers: Vec<Receiver<QueueItem>>,
}

impl ExchangeReceiveChannels {
    pub fn new(num_threads: usize) -> Self {
        let (senders, receivers) = (0..num_threads).map(|_| async_channel::unbounded()).unzip();
        Self { senders, receivers }
    }
}

/// Received encoded data holding receive quota until dequeued for decoding.
pub struct RemoteQueueItem {
    pub data: FlightData,
    owned_semaphore_permit: OwnedSemaphorePermit,
}

pub enum QueueItem {
    LocalData(LocalQueueItem),
    RemoteData(RemoteQueueItem),
}

impl RemoteQueueItem {
    pub fn new(data: FlightData, owned_semaphore_permit: OwnedSemaphorePermit) -> Self {
        Self {
            data,
            owned_semaphore_permit,
        }
    }

    pub fn into_data(self) -> FlightData {
        drop(self.owned_semaphore_permit);
        self.data
    }
}

pub struct LocalQueueItem {
    data: DataBlock,
    owned_semaphore_permit: OwnedSemaphorePermit,
}

impl LocalQueueItem {
    pub fn new(data: DataBlock, owned_semaphore_permit: OwnedSemaphorePermit) -> Self {
        Self {
            data,
            owned_semaphore_permit,
        }
    }

    pub fn into_data(self) -> DataBlock {
        drop(self.owned_semaphore_permit);
        self.data
    }
}

/// Receives from an exchange queue, returning local blocks directly and decoding
/// remote items into blocks on the thread polling `recv_block`.
pub struct ExchangeQueueReceiver {
    receiver: Receiver<QueueItem>,
    schema: DataSchemaRef,
    arrow_schema: Arc<ArrowSchema>,
}

impl ExchangeQueueReceiver {
    pub fn create(schema: &DataSchemaRef, receiver: Receiver<QueueItem>) -> Arc<Self> {
        Arc::new(Self {
            receiver,
            arrow_schema: Arc::new(ArrowSchema::from(schema.as_ref())),
            schema: schema.clone(),
        })
    }

    /// Stop producers and discard queued items, releasing their quota.
    pub fn close(&self) {
        self.receiver.close();

        while self.receiver.try_recv().is_ok() {}
    }

    /// Whether no queued blocks remain and no more can arrive.
    pub fn is_closed(&self) -> bool {
        self.receiver.is_empty() && self.receiver.is_closed()
    }

    /// Wait for the next block, returning `None` after the queue is drained and closed.
    pub async fn recv_block(&self) -> Result<Option<DataBlock>, ErrorCode> {
        match self.receiver.recv().await {
            Err(_) => Ok(None),
            Ok(QueueItem::LocalData(v)) => Ok(Some(v.into_data())),
            Ok(QueueItem::RemoteData(r)) => {
                let flight_data = strip_tid(r.into_data());
                Ok(Some(deserialize_flight_data(
                    flight_data,
                    &self.schema,
                    &self.arrow_schema,
                )?))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::DataSchema;
    use futures::FutureExt;

    use super::*;
    use crate::servers::flight::v1::network::inbound::ExchangeFrameRouter;
    use crate::servers::flight::v1::network::outbound::BlockSender;
    use crate::servers::flight::v1::network::outbound::create_local_block_senders;

    #[test]
    fn test_receiver_drains_blocks_then_observes_last_sender_drop() {
        futures::executor::block_on(async {
            let queues = ExchangeReceiveChannels::new(1);
            let senders = create_local_block_senders(&queues);
            let receiver = ExchangeQueueReceiver::create(
                &Arc::new(DataSchema::empty()),
                queues.receivers[0].clone(),
            );
            drop(queues);

            senders[0].send_block(DataBlock::empty()).await.unwrap();
            drop(senders);

            assert!(!receiver.is_closed());
            assert!(
                receiver
                    .recv_block()
                    .now_or_never()
                    .unwrap()
                    .unwrap()
                    .is_some()
            );
            assert!(receiver.is_closed());
            assert!(
                receiver
                    .recv_block()
                    .now_or_never()
                    .unwrap()
                    .unwrap()
                    .is_none()
            );
        });
    }

    #[test]
    fn test_runtime_producers_do_not_keep_receiver_alive() {
        futures::executor::block_on(async {
            let queues = ExchangeReceiveChannels::new(1);
            let router = ExchangeFrameRouter::new(&queues, 1024);
            let senders = create_local_block_senders(&queues);
            let receiver = ExchangeQueueReceiver::create(
                &Arc::new(DataSchema::empty()),
                queues.receivers[0].clone(),
            );
            drop(queues);
            drop(receiver);

            assert!(senders[0].is_closed());
            assert!(
                router
                    .route_frame(arrow_flight::FlightData::default())
                    .await
                    .is_err()
            );
        });
    }
}
