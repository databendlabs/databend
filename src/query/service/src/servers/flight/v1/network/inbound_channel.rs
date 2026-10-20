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
use super::outbound_buffer::BATCH_MARKER;
use super::outbound_channel::LZ4_BODY_MARKER;
use super::outbound_channel::ZSTD_BODY_MARKER;

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

/// Detect a batch frame built by `merge_flight_data_batch`.
fn is_batch(data: &FlightData) -> bool {
    data.app_metadata.len() == 5 && data.app_metadata[4] == BATCH_MARKER
}

/// Split a batch FlightData back into individual FlightData items.
///
/// Each item carries its own tid prefix in app_metadata. Items of one frame may go
/// to different threads' queues, where zero-copy slices would keep the whole frame
/// alive until the slowest thread consumes its item. So when a frame serves more
/// than one thread, each item is copied into its own allocation.
pub(crate) fn split_batch_flight_data(data: FlightData) -> Vec<FlightData> {
    let meta = &data.app_metadata;
    let num_items = u32::from_le_bytes([meta[0], meta[1], meta[2], meta[3]]) as usize;

    let mut buf = data.data_body; // Bytes implements Buf
    let mut items = Vec::with_capacity(num_items);

    for _ in 0..num_items {
        let meta_len = buf.get_u32_le() as usize;
        let app_metadata = buf.split_to(meta_len);

        let header_len = buf.get_u32_le() as usize;
        let data_header = buf.split_to(header_len);

        let body_len = buf.get_u32_le() as usize;
        let data_body = buf.split_to(body_len);

        items.push(FlightData {
            flight_descriptor: None,
            app_metadata,
            data_header,
            data_body,
        });
    }

    let single_thread = items
        .windows(2)
        .all(|x| extract_tid(&x[0]) == extract_tid(&x[1]));

    match single_thread {
        true => items,
        false => items.into_iter().map(copy_flight_data).collect(),
    }
}

/// Copy a FlightData into one allocation owned by it alone.
fn copy_flight_data(data: FlightData) -> FlightData {
    let meta_len = data.app_metadata.len();
    let header_len = data.data_header.len();

    let mut owned = BytesMut::with_capacity(meta_len + header_len + data.data_body.len());
    owned.put_slice(&data.app_metadata);
    owned.put_slice(&data.data_header);
    owned.put_slice(&data.data_body);
    let mut owned = owned.freeze();

    FlightData {
        flight_descriptor: None,
        app_metadata: owned.split_to(meta_len),
        data_header: owned.split_to(header_len),
        data_body: owned,
    }
}

/// Deserialize a FlightData (after tid stripping) back into a DataBlock.
///
/// Format of `app_metadata`:
/// - Fragment (last byte 0x01): `[row_count: u32][block_meta: bincode][0x01]`
/// - Fragment with a compressed body (last byte 0x06 for LZ4, 0x07 for ZSTD): same
///   metadata as 0x01, see [`decode_compressed_body`]
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

    if !matches!(marker, 0x01 | LZ4_BODY_MARKER | ZSTD_BODY_MARKER) {
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

    let batch = match marker {
        0x01 => flight_data_to_arrow_batch(&flight_data, arrow_schema, &HashMap::new())
            .map_err(|e| ErrorCode::BadBytes(format!("failed to decode arrow batch: {}", e)))?,
        _ => decode_compressed_body(&flight_data, marker, arrow_schema)?,
    };

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

/// Decode a record batch whose whole IPC body is compressed as one block
/// ([`LZ4_BODY_MARKER`] or [`ZSTD_BODY_MARKER`]).
///
/// The decompressed body is handed to the IPC reader without another copy, and
/// is owned by this batch alone.
fn decode_compressed_body(
    flight_data: &FlightData,
    marker: u8,
    arrow_schema: Arc<ArrowSchema>,
) -> Result<arrow_array::RecordBatch, ErrorCode> {
    let data = &flight_data.data_body;
    let body = match marker {
        LZ4_BODY_MARKER => lz4_flex::block::decompress_size_prepended(data)
            .map_err(|e| ErrorCode::BadBytes(format!("failed to decompress arrow batch: {}", e)))?,
        _ => {
            let Some((size, frame)) = data.split_first_chunk::<4>() else {
                return Err(ErrorCode::BadBytes("truncated ZSTD arrow batch"));
            };
            zstd::bulk::decompress(frame, u32::from_le_bytes(*size) as usize).map_err(|e| {
                ErrorCode::BadBytes(format!("failed to decompress arrow batch: {}", e))
            })?
        }
    };

    let message = arrow_ipc::root_as_message(&flight_data.data_header)
        .map_err(|e| ErrorCode::BadBytes(format!("invalid arrow message header: {:?}", e)))?;
    let batch = message
        .header_as_record_batch()
        .ok_or_else(|| ErrorCode::BadBytes("arrow message is not a record batch"))?;

    arrow_ipc::reader::read_record_batch(
        &arrow_buffer::Buffer::from_vec(body),
        batch,
        arrow_schema,
        &HashMap::new(),
        None,
        &message.version(),
    )
    .map_err(|e| ErrorCode::BadBytes(format!("failed to decode arrow batch: {}", e)))
}

#[cfg(test)]
mod tests {
    use arrow_flight::FlightData;
    use bytes::BufMut;
    use bytes::Bytes;
    use bytes::BytesMut;

    use super::*;
    use crate::servers::flight::v1::network::outbound_buffer::merge_flight_data_batch;

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

    #[test]
    fn test_is_batch_detection() {
        let batch = merge_flight_data_batch(vec![
            make_item(0, &[0x01], &[], &[1, 2, 3]),
            make_item(1, &[0x01], &[], &[4]),
        ]);
        assert!(is_batch(&batch));

        // A normal single item (3 bytes, marker 0x01)
        let single = make_item(0, &[0x01], &[], &[1, 2, 3]);
        assert!(!is_batch(&single));

        // A single item whose metadata happens to end with 0x02
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
            make_item(3, &[0xBB, 0x01], &[], &[6, 7, 8]),
            make_item(9, &[0xCC, 0x01], &[30], &[]),
        ];

        let batch = merge_flight_data_batch(items.clone());
        assert!(is_batch(&batch));

        let split = split_batch_flight_data(batch);
        assert_eq!(split.len(), 3);

        for (original, restored) in items.iter().zip(split.iter()) {
            assert_eq!(restored.app_metadata, original.app_metadata);
            assert_eq!(restored.data_header, original.data_header);
            assert_eq!(restored.data_body, original.data_body);
        }

        let tids = split.iter().map(extract_tid).collect::<Vec<_>>();
        assert_eq!(tids, vec![7, 3, 9]);
    }

    /// Items for different threads must not keep the frame alive, while items of a
    /// single-thread frame keep sharing it.
    #[test]
    fn test_split_batch_copies_only_multi_thread_frames() {
        let frame_items = |tids: &[u16]| {
            tids.iter()
                .map(|tid| make_item(*tid, &[0x01], &[1, 2], &[*tid as u8; 1024]))
                .collect::<Vec<_>>()
        };

        for (tids, shared) in [(&[0u16, 1, 2, 3][..], false), (&[5u16, 5, 5][..], true)] {
            let batch = merge_flight_data_batch(frame_items(tids));
            let frame = batch.data_body.clone();
            let frame_range = frame.as_ptr_range();

            for item in split_batch_flight_data(batch) {
                assert_eq!(frame_range.contains(&item.data_body.as_ptr()), shared);
            }
        }
    }
}
