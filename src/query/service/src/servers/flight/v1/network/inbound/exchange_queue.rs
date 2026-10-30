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

impl Drop for ExchangeReceiveChannels {
    /// A query aborted before its pipelines took the receivers leaves this
    /// collection as their last owner: release what was already queued for them.
    fn drop(&mut self) {
        for receiver in &self.receivers {
            if receiver.receiver_count() == 1 {
                close_queue(receiver);
            }
        }
    }
}

/// Stop producers and discard queued items, releasing their quota.
///
/// Producers wait for the quota of queued items, and keep the queue alive while
/// they wait, so a queue left by its last receiver must be drained explicitly.
fn close_queue(receiver: &Receiver<QueueItem>) {
    receiver.close();

    while receiver.try_recv().is_ok() {}
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
        close_queue(&self.receiver);
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

impl Drop for ExchangeQueueReceiver {
    /// An aborted pipeline drops its receivers without closing them. Their queued
    /// items would keep the quota a connection's router waits for: the router would
    /// never answer again, and its sender would wait for that answer forever.
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use std::task::Poll;

    use databend_common_expression::DataSchema;
    use databend_common_expression::FromData;
    use databend_common_expression::types::UInt64Type;
    use futures::FutureExt;

    use super::*;
    use crate::servers::flight::v1::network::inbound::ExchangeFrameRouter;
    use crate::servers::flight::v1::network::outbound::BlockSender;
    use crate::servers::flight::v1::network::outbound::LOCAL_CHANNEL_MAX_BYTES;
    use crate::servers::flight::v1::network::outbound::create_local_block_senders;
    use crate::servers::flight::v1::network::wire_format::flight_data_size;

    fn receiver(queues: &ExchangeReceiveChannels) -> Arc<ExchangeQueueReceiver> {
        ExchangeQueueReceiver::create(&Arc::new(DataSchema::empty()), queues.receivers[0].clone())
    }

    /// A router waiting for connection quota held by queued items must be released
    /// when the queue loses its last receiver without `close`, as on abort: either
    /// a pipeline's receiver, or the collection when no pipeline took it yet.
    #[test]
    fn test_dropped_receiver_releases_connection_quota() {
        futures::executor::block_on(async {
            for pipeline_built in [true, false] {
                let frame = || FlightData {
                    app_metadata: 0u16.to_le_bytes().to_vec().into(),
                    data_body: vec![42; 8].into(),
                    ..FlightData::default()
                };
                let queues = ExchangeReceiveChannels::new(1);
                let router = ExchangeFrameRouter::new(&queues, flight_data_size(&frame()));
                let receiver = pipeline_built.then(|| receiver(&queues));
                let queues = match pipeline_built {
                    true => {
                        drop(queues);
                        None
                    }
                    false => Some(queues),
                };

                router.route_frame(frame()).await.unwrap();
                let mut pending = Box::pin(router.route_frame(frame()));
                assert!(futures::poll!(pending.as_mut()).is_pending());

                drop(receiver);
                drop(queues);
                assert!(matches!(
                    futures::poll!(pending.as_mut()),
                    Poll::Ready(Err(()))
                ));
            }
        });
    }

    /// The same for a local sender waiting for the node's local quota.
    #[test]
    fn test_dropped_receiver_releases_local_quota() {
        futures::executor::block_on(async {
            let queues = ExchangeReceiveChannels::new(1);
            let senders = create_local_block_senders(&queues);
            let receiver = receiver(&queues);
            drop(queues);

            let block = |rows: usize| {
                DataBlock::new_from_columns(vec![UInt64Type::from_data(vec![0u64; rows])])
            };
            // Takes the whole local quota.
            senders[0]
                .send_block(block(LOCAL_CHANNEL_MAX_BYTES / 8))
                .await
                .unwrap();
            let mut pending = senders[0].send_block(block(1));
            assert!(futures::poll!(pending.as_mut()).is_pending());

            drop(receiver);
            assert!(matches!(
                futures::poll!(pending.as_mut()),
                Poll::Ready(Ok(()))
            ));
            assert!(senders[0].is_closed());
        });
    }

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
