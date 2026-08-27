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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use arrow_flight::FlightData;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_schema::Schema as ArrowSchema;
use async_channel::Receiver;
use async_channel::Sender;
use databend_common_base::base::WatchNotify;
use databend_common_exception::ErrorCode;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_io::prelude::BinaryRead;
use databend_common_io::prelude::bincode_deserialize_from_stream;
use futures::FutureExt;
use futures::future::BoxFuture;
use parking_lot::Mutex;
use tokio::sync::Semaphore;

use super::inbound_quota::QueueItem;
use super::inbound_quota::SubQueue;
use crate::servers::flight::v1::transport::DeliveryOutcome;
use crate::servers::flight::v1::transport::InboundDelivery;
use crate::servers::flight::v1::transport::take_lane;

pub struct ExchangePacketReceiver {
    pub sender: Sender<QueueItem>,
    pub receiver: Receiver<QueueItem>,

    pub sender_count: Arc<AtomicUsize>,
    pub(crate) closed_notified: Arc<WatchNotify>,
    /// The first terminal error from any logical source feeding this channel.
    /// After queued data drains, a closed channel returns this cause, or clean EOF when it is `None`.
    pub close_cause: Arc<Mutex<Option<ErrorCode>>>,
}

impl ExchangePacketReceiver {
    pub fn create() -> Self {
        let (tx, rx) = async_channel::unbounded();
        Self {
            sender: tx,
            receiver: rx,
            sender_count: Arc::new(AtomicUsize::new(0)),
            closed_notified: Arc::new(WatchNotify::new()),
            close_cause: Arc::new(Mutex::new(None)),
        }
    }

    pub fn close(&self) {
        self.receiver.close();
        while self.receiver.try_recv().is_ok() {}
        self.closed_notified.notify_waiters();
    }

    pub async fn recv_raw(&self) -> Result<Option<QueueItem>, ErrorCode> {
        if let Ok(item) = self.receiver.try_recv() {
            return Ok(Some(item));
        }

        match self.receiver.recv().await {
            Ok(item) => Ok(Some(item)),
            Err(_) => match self.close_cause.lock().clone() {
                Some(cause) => Err(cause),
                None => Ok(None),
            },
        }
    }
}

/// The receivers for one exchange channel id.
pub struct ExchangePacketReceiverSet {
    pub receivers: Arc<Vec<Arc<ExchangePacketReceiver>>>,
}

impl ExchangePacketReceiverSet {
    pub fn new(num_threads: usize) -> Self {
        let receivers = (0..num_threads)
            .map(|_| Arc::new(ExchangePacketReceiver::create()))
            .collect();
        Self {
            receivers: Arc::new(receivers),
        }
    }

    pub fn create_receiver(&self, t_idx: usize, schema: &DataSchemaRef) -> Arc<dyn InboundChannel> {
        NetworkInboundReceiver::create(schema, self.receivers[t_idx].clone())
    }
}

/// Network-side handle. Each do_exchange connection gets one.
///
/// Routes incoming payloads into the per-tid sub-queues of one channel and releases them once the
/// connection ends. Both transports share this: the existing Flight path drops the handle when its
/// stream ends, while New Flight drives it through [`InboundDelivery`] so a terminal error
/// can be propagated to waiting processors.
pub struct NetworkInboundSender {
    destinations: Vec<InboundDestination>,
    /// Guards `release` so a `terminate` followed by `Drop` decrements `sender_count` only once.
    released: AtomicBool,
}

struct InboundDestination {
    /// This connection's sub-queue in one tid's ExchangePacketReceiver.
    queue: Arc<SubQueue>,
    close_cause: Arc<Mutex<Option<ErrorCode>>>,
    closed_notified: Arc<WatchNotify>,
}

/// Whether a payload reached a sub-queue, or every consumer on the channel is gone.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InboundOutcome {
    Accepted,
    AllReceiversClosed,
}

impl NetworkInboundSender {
    /// Create a new sender for a connection.
    /// Adds a sub-queue to each ExchangePacketReceiver for this connection.
    pub fn new(channel_set: &ExchangePacketReceiverSet, max_bytes_per_connection: usize) -> Self {
        let semaphore = Arc::new(Semaphore::new(max_bytes_per_connection));
        let destinations = channel_set
            .receivers
            .iter()
            .map(|channel| {
                channel.sender_count.fetch_add(1, Ordering::AcqRel);
                InboundDestination {
                    queue: Arc::new(SubQueue {
                        max_bytes_per_connection,
                        sender: channel.sender.clone(),
                        semaphore: semaphore.clone(),
                        sender_count: channel.sender_count.clone(),
                    }),
                    close_cause: channel.close_cause.clone(),
                    closed_notified: channel.closed_notified.clone(),
                }
            })
            .collect();

        Self {
            destinations,
            released: AtomicBool::new(false),
        }
    }

    /// Add data to the inbound channel.
    ///
    /// Extracts tid from the FlightData, pushes to the appropriate sub-queue,
    /// and waits for backpressure to clear.
    ///
    /// Returns `Err(())` only when ALL receivers are closed (network should disconnect).
    /// If only the target tid's receiver is closed, discards the data and returns `Ok(())`.
    pub async fn add_data(&self, data: FlightData) -> Result<(), ()> {
        // The existing Flight path has no way to report a protocol error back to the peer, so a
        // malformed tid closes the connection just like an exhausted set of receivers.
        let (lane, data) = take_lane(data).map_err(|_| ())?;
        match self.deliver_one(lane, data).await {
            Ok(InboundOutcome::Accepted) => Ok(()),
            Ok(InboundOutcome::AllReceiversClosed) | Err(_) => Err(()),
        }
    }

    async fn deliver_one(
        &self,
        lane: usize,
        data: FlightData,
    ) -> Result<InboundOutcome, ErrorCode> {
        let Some(destination) = self.destinations.get(lane) else {
            return Err(ErrorCode::BadBytes(format!(
                "do_exchange thread id {} is out of range for {} channels",
                lane,
                self.destinations.len()
            )));
        };

        match destination.queue.add_data(data).await {
            Ok(()) => Ok(InboundOutcome::Accepted),
            Err(()) if self.all_receivers_closed() => Ok(InboundOutcome::AllReceiversClosed),
            // Only this tid's consumer is gone, so drop the payload and keep the connection.
            Err(()) => Ok(InboundOutcome::Accepted),
        }
    }

    /// Check if all channels are closed by receivers.
    pub fn all_receivers_closed(&self) -> bool {
        self.destinations
            .iter()
            .all(|destination| destination.queue.sender.is_closed())
    }

    /// Releases every sub-queue once. `cause` fails the downstream receivers instead of letting
    /// them observe a clean end of stream.
    fn release(&self, cause: Option<ErrorCode>) {
        if self.released.swap(true, Ordering::AcqRel) {
            return;
        }

        for destination in &self.destinations {
            if let Some(cause) = &cause {
                let mut close_cause = destination.close_cause.lock();
                if close_cause.is_none() {
                    *close_cause = Some(cause.clone());
                }
                drop(close_cause);
                destination.queue.sender.close();
                destination.queue.semaphore.close();
            }

            if destination
                .queue
                .sender_count
                .fetch_sub(1, Ordering::AcqRel)
                == 1
            {
                destination.queue.sender.close();
            }
        }
    }
}

#[async_trait::async_trait]
impl InboundDelivery for NetworkInboundSender {
    async fn deliver(
        &self,
        lane: usize,
        data: FlightData,
    ) -> std::result::Result<DeliveryOutcome, ErrorCode> {
        Ok(match self.deliver_one(lane, data).await? {
            InboundOutcome::Accepted => DeliveryOutcome::Accepted,
            InboundOutcome::AllReceiversClosed => DeliveryOutcome::ConsumerClosed,
        })
    }

    fn is_closed(&self) -> bool {
        self.all_receivers_closed()
    }

    fn consumer_closed(&self) -> Option<BoxFuture<'static, ()>> {
        let notifications = self
            .destinations
            .iter()
            .map(|destination| destination.closed_notified.clone())
            .collect::<Vec<_>>();

        Some(
            async move {
                futures::future::join_all(
                    notifications
                        .into_iter()
                        .map(|notification| async move { notification.notified().await }),
                )
                .await;
            }
            .boxed(),
        )
    }

    fn terminate(&self, cause: Option<ErrorCode>) {
        self.release(cause);
    }
}

impl Drop for NetworkInboundSender {
    fn drop(&mut self) {
        self.release(None);
    }
}

/// Trait for receiving data blocks from the network.
#[async_trait::async_trait]
pub trait InboundChannel: Send + Sync {
    fn close(&self);

    fn is_closed(&self) -> bool;

    async fn recv(&self) -> Result<Option<DataBlock>, ErrorCode>;
}

pub struct NetworkInboundReceiver {
    channel: Arc<ExchangePacketReceiver>,
    schema: DataSchemaRef,
    arrow_schema: Arc<ArrowSchema>,
}

impl NetworkInboundReceiver {
    pub fn create(
        schema: &DataSchemaRef,
        channel: Arc<ExchangePacketReceiver>,
    ) -> Arc<dyn InboundChannel> {
        Arc::new(Self {
            channel,
            arrow_schema: Arc::new(ArrowSchema::from(schema.as_ref())),
            schema: schema.clone(),
        })
    }
}

#[async_trait::async_trait]
impl InboundChannel for NetworkInboundReceiver {
    fn close(&self) {
        self.channel.close();
    }

    fn is_closed(&self) -> bool {
        self.channel.receiver.is_empty() && self.channel.receiver.is_closed()
    }

    async fn recv(&self) -> Result<Option<DataBlock>, ErrorCode> {
        match self.channel.recv_raw().await? {
            None => Ok(None),
            Some(QueueItem::LocalData(v)) => Ok(Some(v.into_data())),
            Some(QueueItem::RemoteData(r)) => Ok(Some(deserialize_flight_data(
                r.into_data(),
                &self.schema,
                &self.arrow_schema,
            )?)),
        }
    }
}

/// Compute the byte size of a FlightData for quota accounting.
pub fn flight_data_size(data: &FlightData) -> usize {
    data.data_body.len()
}

/// Deserialize a transport-neutral FlightData back into a DataBlock.
///
/// Format of `app_metadata`:
/// - Fragment (last byte 0x01): `[row_count: u32][block_meta: bincode][0x01]`
/// - Dictionary (last byte 0x05): dictionary IPC data (currently unsupported)
pub(crate) fn deserialize_flight_data(
    flight_data: FlightData,
    schema: &DataSchemaRef,
    arrow_schema: &Arc<ArrowSchema>,
) -> Result<DataBlock, ErrorCode> {
    let meta_bytes = &flight_data.app_metadata;
    if meta_bytes.is_empty() {
        return Err(ErrorCode::BadBytes("empty app_metadata in FlightData"));
    }

    let marker = meta_bytes[meta_bytes.len() - 1];
    if marker == 0x05 {
        return Err(ErrorCode::Unimplemented(
            "dictionary FlightData not yet supported in broadcast exchange",
        ));
    }

    if marker != 0x01 {
        return Err(ErrorCode::BadBytes(format!(
            "unknown FlightData marker: 0x{:02x}",
            marker
        )));
    }

    // Parse metadata (excluding the trailing 0x01 marker)
    let meta = &meta_bytes[..meta_bytes.len() - 1];
    const ROW_HEADER_SIZE: usize = std::mem::size_of::<u32>();

    let mut cursor = &meta[..ROW_HEADER_SIZE];
    let row_count: u32 = cursor
        .read_scalar()
        .map_err(|e| ErrorCode::BadBytes(format!("failed to read row_count: {}", e)))?;

    let mut remaining = &meta[ROW_HEADER_SIZE..];
    let block_meta: Option<databend_common_expression::BlockMetaInfoPtr> =
        bincode_deserialize_from_stream(&mut remaining)
            .map_err(|e| ErrorCode::BadBytes(format!("failed to deserialize block_meta: {}", e)))?;

    if row_count == 0 {
        return Ok(DataBlock::new_with_meta(vec![], 0, block_meta));
    }

    let mut schema = schema.clone();
    let mut arrow_schema = arrow_schema.clone();

    if let Some(meta) = &block_meta {
        if let Some(dynamic_schema) = meta.override_block_schema() {
            arrow_schema = Arc::new(ArrowSchema::from(dynamic_schema.as_ref()));
            schema = dynamic_schema;
        }
    }

    let batch = flight_data_to_arrow_batch(&flight_data, arrow_schema, &HashMap::new())
        .map_err(|e| ErrorCode::BadBytes(format!("failed to decode arrow batch: {}", e)))?;

    let block = DataBlock::from_record_batch(&schema, &batch)?;

    if block.num_columns() == 0 {
        return Ok(DataBlock::new_with_meta(
            vec![],
            row_count as usize,
            block_meta,
        ));
    }

    block.add_meta(block_meta)
}
