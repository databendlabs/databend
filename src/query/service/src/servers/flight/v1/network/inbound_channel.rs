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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use arrow_flight::FlightData;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_schema::Schema as ArrowSchema;
use async_channel::Receiver;
use async_channel::Sender;
use databend_common_exception::ErrorCode;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_io::prelude::BinaryRead;
use databend_common_io::prelude::bincode_deserialize_from_stream;
use tokio::sync::Semaphore;

use super::inbound_quota::QueueItem;
use super::inbound_quota::SubQueue;

pub struct NetworkInboundChannel {
    pub sender: Sender<QueueItem>,
    pub receiver: Receiver<QueueItem>,

    pub sender_count: Arc<AtomicUsize>,
}

impl NetworkInboundChannel {
    pub fn create() -> Self {
        let (tx, rx) = async_channel::unbounded();
        Self {
            sender: tx,
            receiver: rx,
            sender_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn deserialize(&self, flight_data: FlightData) -> Result<DataBlock, ErrorCode> {
        deserialize_flight_data(flight_data)
    }

    pub async fn recv(&self) -> Result<Option<DataBlock>, ErrorCode> {
        if let Ok(item) = self.receiver.try_recv() {
            return Ok(Some(match item {
                QueueItem::LocalData(v) => v.into_data(),
                QueueItem::RemoteData(r) => self.deserialize(strip_tid(r.into_data()))?,
            }));
        }

        match self.receiver.recv().await {
            Err(_) => Ok(None),
            Ok(item) => Ok(Some(match item {
                QueueItem::LocalData(v) => v.into_data(),
                QueueItem::RemoteData(r) => self.deserialize(strip_tid(r.into_data()))?,
            })),
        }
    }
}

/// The set of NetworkInboundChannels for one channel_id.
pub struct NetworkInboundChannelSet {
    pub channels: Arc<Vec<Arc<NetworkInboundChannel>>>,
}

impl NetworkInboundChannelSet {
    pub fn new(num_threads: usize) -> Self {
        let channels = (0..num_threads)
            .map(|_| Arc::new(NetworkInboundChannel::create()))
            .collect();
        Self {
            channels: Arc::new(channels),
        }
    }

    pub fn create_receiver(&self, thread_idx: usize) -> Arc<dyn InboundChannel> {
        NetworkInboundReceiver::create(self.channels[thread_idx].clone())
    }
}

/// Network-side handle. Each do_exchange connection gets one.
///
/// When dropped, closes this connection's sub-queues and notifies processors.
pub struct NetworkInboundSender {
    /// This connection's sub-queue in each tid's NetworkInboundChannel.
    sub_queues: Vec<Arc<SubQueue>>,
}

impl NetworkInboundSender {
    /// Create a new sender for a connection.
    /// Adds a sub-queue to each NetworkInboundChannel for this connection.
    pub fn new(channel_set: &NetworkInboundChannelSet, max_bytes_per_connection: usize) -> Self {
        let semaphore = Arc::new(Semaphore::new(max_bytes_per_connection));
        let mut sub_queues = Vec::with_capacity(channel_set.channels.len());

        for channel in channel_set.channels.iter() {
            channel.sender_count.fetch_add(1, Ordering::AcqRel);

            let sub_queue = Arc::new(SubQueue {
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
    pub async fn add_data(&self, data: FlightData) -> Result<(), ()> {
        let tid = extract_tid(&data);

        match self.sub_queues[tid].add_data(data).await {
            Ok(()) => Ok(()),
            Err(()) => match self.all_receivers_closed() {
                true => Err(()),
                false => Ok(()),
            },
        }
    }

    /// Check if all channels are closed by receivers.
    pub fn all_receivers_closed(&self) -> bool {
        self.sub_queues.iter().all(|q| q.sender.is_closed())
    }
}

impl Drop for NetworkInboundSender {
    fn drop(&mut self) {
        for sub_queue in &self.sub_queues {
            if sub_queue.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
                sub_queue.sender.close();
            }
        }
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
    channel: Arc<NetworkInboundChannel>,
}

impl NetworkInboundReceiver {
    pub fn create(channel: Arc<NetworkInboundChannel>) -> Arc<dyn InboundChannel> {
        Arc::new(Self { channel })
    }
}

#[async_trait::async_trait]
impl InboundChannel for NetworkInboundReceiver {
    fn close(&self) {
        self.channel.receiver.close();

        while self.channel.receiver.try_recv().is_ok() {}
    }

    fn is_closed(&self) -> bool {
        self.channel.receiver.is_closed()
    }

    async fn recv(&self) -> Result<Option<DataBlock>, ErrorCode> {
        self.channel.recv().await
    }
}

/// Compute the byte size of a FlightData for quota accounting.
pub fn flight_data_size(data: &FlightData) -> usize {
    data.data_body.len()
}

/// Extract tid from FlightData app_metadata (first 2 bytes, little-endian u16).
pub fn extract_tid(data: &FlightData) -> usize {
    if data.app_metadata.len() >= 2 {
        u16::from_le_bytes([data.app_metadata[0], data.app_metadata[1]]) as usize
    } else {
        0
    }
}

/// Strip the tid prefix (first 2 bytes) from FlightData app_metadata.
/// Returns the FlightData in its original format (without tid encoding).
pub fn strip_tid(mut data: FlightData) -> FlightData {
    if data.app_metadata.len() >= 2 {
        data.app_metadata = data.app_metadata.slice(2..);
    }
    data
}

/// Deserialize a FlightData (after tid stripping) back into a DataBlock.
///
/// Format of `app_metadata`:
/// - Fragment (last byte 0x01): `[row_count: u32][block_meta: bincode][schema: bincode][0x01]`
/// - Dictionary (last byte 0x05): dictionary IPC data (currently unsupported)
pub(crate) fn deserialize_flight_data(flight_data: FlightData) -> Result<DataBlock, ErrorCode> {
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

    let schema: DataSchema = bincode_deserialize_from_stream(&mut remaining)
        .map_err(|e| ErrorCode::BadBytes(format!("failed to deserialize schema: {}", e)))?;

    if row_count == 0 {
        return Ok(DataBlock::new_with_meta(vec![], 0, block_meta));
    }

    let arrow_schema = Arc::new(ArrowSchema::from(&schema));
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
