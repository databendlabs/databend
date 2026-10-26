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
use arrow_schema::Schema as ArrowSchema;
use async_channel::Receiver;
use async_channel::Sender;
use databend_common_exception::ErrorCode;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use tokio::sync::OwnedSemaphorePermit;

use super::super::wire_format::deserialize_flight_data;
use super::super::wire_format::strip_tid;

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
    pub fn new(data: FlightData, owned_semaphore_permit: OwnedSemaphorePermit) -> Self {
        Self {
            data,
            owned_semaphore_permit,
        }
    }

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

pub struct ExchangeReceiveChannel {
    pub sender: Sender<QueueItem>,
    pub receiver: Receiver<QueueItem>,

    pub sender_count: Arc<AtomicUsize>,
}

impl ExchangeReceiveChannel {
    pub fn create() -> Self {
        let (tx, rx) = async_channel::unbounded();
        Self {
            sender: tx,
            receiver: rx,
            sender_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub async fn recv_raw(&self) -> Option<QueueItem> {
        if let Ok(item) = self.receiver.try_recv() {
            return Some(item);
        }

        self.receiver.recv().await.ok()
    }
}

/// Shared producer and consumer endpoints for every thread in one exchange.
pub struct ExchangeReceiveChannels {
    pub channels: Arc<Vec<Arc<ExchangeReceiveChannel>>>,
}

impl ExchangeReceiveChannels {
    pub fn new(num_threads: usize) -> Self {
        let channels = (0..num_threads)
            .map(|_| Arc::new(ExchangeReceiveChannel::create()))
            .collect();
        Self {
            channels: Arc::new(channels),
        }
    }

    pub fn create_receiver(
        &self,
        t_idx: usize,
        schema: &DataSchemaRef,
    ) -> Arc<dyn ExchangeReceiver> {
        ExchangeQueueReceiver::create(schema, self.channels[t_idx].clone())
    }
}

/// Consume blocks delivered locally or received as encoded Flight items.
#[async_trait::async_trait]
pub trait ExchangeReceiver: Send + Sync {
    fn close(&self);

    fn is_closed(&self) -> bool;

    async fn recv(&self) -> Result<Option<DataBlock>, ErrorCode>;
}

pub struct ExchangeQueueReceiver {
    channel: Arc<ExchangeReceiveChannel>,
    schema: DataSchemaRef,
    arrow_schema: Arc<ArrowSchema>,
}

impl ExchangeQueueReceiver {
    pub fn create(
        schema: &DataSchemaRef,
        channel: Arc<ExchangeReceiveChannel>,
    ) -> Arc<dyn ExchangeReceiver> {
        Arc::new(Self {
            channel,
            arrow_schema: Arc::new(ArrowSchema::from(schema.as_ref())),
            schema: schema.clone(),
        })
    }
}

#[async_trait::async_trait]
impl ExchangeReceiver for ExchangeQueueReceiver {
    fn close(&self) {
        self.channel.receiver.close();

        while self.channel.receiver.try_recv().is_ok() {}
    }

    fn is_closed(&self) -> bool {
        self.channel.receiver.is_empty() && self.channel.receiver.is_closed()
    }

    async fn recv(&self) -> Result<Option<DataBlock>, ErrorCode> {
        match self.channel.recv_raw().await {
            None => Ok(None),
            Some(QueueItem::LocalData(v)) => Ok(Some(v.into_data())),
            Some(QueueItem::RemoteData(r)) => {
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
