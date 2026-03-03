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
use bytes::Buf;
use bytes::BufMut;
use bytes::BytesMut;
use databend_common_exception::ErrorCode;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
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

    pub async fn recv_raw(&self) -> Option<QueueItem> {
        if let Ok(item) = self.receiver.try_recv() {
            return Some(item);
        }

        self.receiver.recv().await.ok()
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

    pub fn create_receiver(&self, t_idx: usize, schema: &DataSchemaRef) -> Arc<dyn InboundChannel> {
        NetworkInboundReceiver::create(schema, self.channels[t_idx].clone())
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
    schema: DataSchemaRef,
    arrow_schema: Arc<ArrowSchema>,
}

impl NetworkInboundReceiver {
    pub fn create(
        schema: &DataSchemaRef,
        channel: Arc<NetworkInboundChannel>,
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

/// Detect a batch FlightData by checking for the BATCH_MARKER (0x02) as the last byte.
fn is_batch(data: &FlightData) -> bool {
    const BATCH_MARKER: u8 = 0x02;
    data.app_metadata.len() >= 5 && data.app_metadata[data.app_metadata.len() - 1] == BATCH_MARKER
}

/// Split a batch FlightData back into individual FlightData items.
/// Uses zero-copy `Bytes::split_to()` for the large data_header and data_body fields.
fn split_batch_flight_data(data: FlightData) -> Vec<FlightData> {
    let meta = &data.app_metadata;
    let tid_bytes: [u8; 2] = [meta[0], meta[1]];
    let num_items = u16::from_le_bytes([meta[2], meta[3]]) as usize;

    let mut buf = data.data_body; // Bytes implements Buf
    let mut items = Vec::with_capacity(num_items);

    for _ in 0..num_items {
        let meta_len = buf.get_u32_le() as usize;
        let inner_meta = buf.split_to(meta_len);

        let header_len = buf.get_u32_le() as usize;
        let data_header = buf.split_to(header_len);

        let body_len = buf.get_u32_le() as usize;
        let data_body = buf.split_to(body_len);

        // Only app_metadata needs a small copy to prepend tid
        let mut app_metadata = BytesMut::with_capacity(2 + meta_len);
        app_metadata.put_slice(&tid_bytes);
        app_metadata.extend_from_slice(&inner_meta);

        items.push(FlightData {
            flight_descriptor: None,
            app_metadata: app_metadata.freeze(),
            data_header,
            data_body,
        });
    }

    items
}

/// Deserialize a FlightData (after tid stripping) back into a DataBlock.
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

#[cfg(test)]
mod tests {
    use arrow_flight::FlightData;
    use bytes::BufMut;
    use bytes::Bytes;
    use bytes::BytesMut;

    use super::*;

    const BATCH_MARKER: u8 = 0x02;

    /// Build a FlightData with tid prefix in app_metadata.
    fn make_item(tid: u16, inner_meta: &[u8], header: &[u8], body: &[u8]) -> FlightData {
        let mut app_metadata = BytesMut::with_capacity(2 + inner_meta.len());
        app_metadata.put_u16_le(tid);
        app_metadata.put_slice(inner_meta);
        FlightData {
            flight_descriptor: None,
            app_metadata: app_metadata.freeze(),
            data_header: Bytes::copy_from_slice(header),
            data_body: Bytes::copy_from_slice(body),
        }
    }

    /// Build a batch FlightData by hand (mirrors merge_flight_data_batch logic).
    fn build_batch(tid: u16, items: &[FlightData]) -> FlightData {
        let mut app_metadata = BytesMut::with_capacity(5);
        app_metadata.put_u16_le(tid);
        app_metadata.put_u16_le(items.len() as u16);
        app_metadata.put_u8(BATCH_MARKER);

        let mut body = BytesMut::new();
        for item in items {
            let inner_meta = &item.app_metadata[2..];
            body.put_u32_le(inner_meta.len() as u32);
            body.put_slice(inner_meta);
            body.put_u32_le(item.data_header.len() as u32);
            body.put_slice(&item.data_header);
            body.put_u32_le(item.data_body.len() as u32);
            body.put_slice(&item.data_body);
        }

        FlightData {
            flight_descriptor: None,
            app_metadata: app_metadata.freeze(),
            data_header: Bytes::new(),
            data_body: body.freeze(),
        }
    }

    #[test]
    fn test_is_batch_detection() {
        // A proper batch: 5 bytes with BATCH_MARKER at end
        let batch = build_batch(0, &[make_item(0, &[0x01], &[], &[1, 2, 3])]);
        assert!(is_batch(&batch));

        // A normal single item (3 bytes, marker 0x01)
        let single = make_item(0, &[0x01], &[], &[1, 2, 3]);
        assert!(!is_batch(&single));

        // Too short to be a batch
        let short = FlightData {
            app_metadata: Bytes::from_static(&[0x00, 0x00, 0x02]),
            ..Default::default()
        };
        assert!(!is_batch(&short));
    }

    #[test]
    fn test_split_batch_roundtrip() {
        let items = vec![
            make_item(7, &[0xAA, 0x01], &[10, 20], &[1, 2, 3, 4, 5]),
            make_item(7, &[0xBB, 0x01], &[], &[6, 7, 8]),
            make_item(7, &[0xCC, 0x01], &[30], &[]),
        ];

        let batch = build_batch(7, &items);
        assert!(is_batch(&batch));

        let split = split_batch_flight_data(batch);
        assert_eq!(split.len(), 3);

        for (original, restored) in items.iter().zip(split.iter()) {
            assert_eq!(restored.app_metadata, original.app_metadata);
            assert_eq!(restored.data_header, original.data_header);
            assert_eq!(restored.data_body, original.data_body);
        }
    }

    #[test]
    fn test_split_batch_single_item() {
        let items = vec![make_item(0, &[0x01], &[1, 2], &[3, 4, 5])];
        let batch = build_batch(0, &items);
        let split = split_batch_flight_data(batch);
        assert_eq!(split.len(), 1);
        assert_eq!(split[0].app_metadata, items[0].app_metadata);
        assert_eq!(split[0].data_header, items[0].data_header);
        assert_eq!(split[0].data_body, items[0].data_body);
    }

    #[test]
    fn test_split_preserves_tid() {
        let tid: u16 = 42;
        let items = vec![
            make_item(tid, &[0x01], &[], &[1]),
            make_item(tid, &[0x02], &[], &[2]),
        ];
        let batch = build_batch(tid, &items);
        let split = split_batch_flight_data(batch);

        for item in &split {
            let restored_tid = u16::from_le_bytes([item.app_metadata[0], item.app_metadata[1]]);
            assert_eq!(restored_tid, tid);
        }
    }
}
