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

//! Wire format of the FlightData exchanged between nodes.
//!
//! An item is one FlightData for one thread of the destination node. Its
//! `app_metadata` is `[tid: u16][row_count: u32][block_meta: bincode][marker: u8]`:
//! - [`FRAGMENT_MARKER`]: Arrow IPC body, buffers optionally compressed by Arrow.
//! - [`LZ4_BODY_MARKER`] / [`ZSTD_BODY_MARKER`]: the whole IPC body compressed at once.
//! - [`DICTIONARY_MARKER`]: a dictionary batch, not supported by the receiver.
//!
//! Items for one node may be packed into a batch frame, see [`merge_flight_data_batch`].

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::RecordBatchOptions;
use arrow_flight::FlightData;
use arrow_flight::FlightDescriptor;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_ipc::CompressionType;
use arrow_ipc::writer::CompressionContext;
use arrow_ipc::writer::DictionaryTracker;
use arrow_ipc::writer::IpcDataGenerator;
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::Schema as ArrowSchema;
use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_io::prelude::BinaryRead;
use databend_common_io::prelude::BinaryWrite;
use databend_common_io::prelude::bincode_deserialize_from_stream;
use databend_common_io::prelude::bincode_serialize_into_buf;
use databend_common_settings::FlightCompression;

/// Marker of a block whose IPC body is sent as Arrow encodes it.
pub(crate) const FRAGMENT_MARKER: u8 = 0x01;

/// Marker of a batch frame, see [`merge_flight_data_batch`].
pub(crate) const BATCH_MARKER: u8 = 0x02;

/// Marker of a dictionary batch.
pub(crate) const DICTIONARY_MARKER: u8 = 0x05;

/// Marker of a block whose whole IPC body is one size-prepended LZ4 block.
/// Its metadata is the same as a plain fragment ([`FRAGMENT_MARKER`]).
pub(crate) const LZ4_BODY_MARKER: u8 = 0x06;

/// Marker of a block whose whole IPC body is one ZSTD frame, prepended by the
/// uncompressed size as a little-endian u32. Its metadata is the same as a plain
/// fragment ([`FRAGMENT_MARKER`]).
pub(crate) const ZSTD_BODY_MARKER: u8 = 0x07;

// ---------------------------------------------------------------------------
// Item size and thread routing
// ---------------------------------------------------------------------------

/// Bytes a FlightData holds, charged against send and receive quotas. Metadata and
/// header count too, so items with a small or empty body still take quota.
pub(crate) fn flight_data_size(data: &FlightData) -> usize {
    data.app_metadata.len() + data.data_header.len() + data.data_body.len()
}

/// Extract tid from FlightData app_metadata (first 2 bytes, little-endian u16).
pub(crate) fn extract_tid(data: &FlightData) -> usize {
    if data.app_metadata.len() >= 2 {
        u16::from_le_bytes([data.app_metadata[0], data.app_metadata[1]]) as usize
    } else {
        0
    }
}

/// Strip the tid prefix (first 2 bytes) from FlightData app_metadata.
/// Returns the FlightData in its original format (without tid encoding).
pub(crate) fn strip_tid(mut data: FlightData) -> FlightData {
    if data.app_metadata.len() >= 2 {
        data.app_metadata = data.app_metadata.slice(2..);
    }
    data
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

fn flight_compression(compression: Option<FlightCompression>) -> Option<CompressionType> {
    match compression {
        None => None,
        Some(FlightCompression::Lz4) => Some(CompressionType::LZ4_FRAME),
        Some(FlightCompression::Zstd) => Some(CompressionType::ZSTD),
    }
}

pub(crate) fn make_ipc_options(compression: Option<FlightCompression>) -> Result<IpcWriteOptions> {
    Ok(IpcWriteOptions::default().try_with_compression(flight_compression(compression))?)
}

fn encode_batch(
    batch: &RecordBatch,
    ipc_options: &IpcWriteOptions,
) -> Result<(Vec<FlightData>, FlightData)> {
    let data_gen = IpcDataGenerator::default();
    let mut dictionary_tracker = DictionaryTracker::new(false);
    let mut compression_context = CompressionContext::default();
    let (encoded_dictionaries, encoded_batch) = data_gen.encode(
        batch,
        &mut dictionary_tracker,
        ipc_options,
        &mut compression_context,
    )?;
    let dictionaries: Vec<FlightData> = encoded_dictionaries.into_iter().map(Into::into).collect();
    let batch_data: FlightData = encoded_batch.into();
    Ok((dictionaries, batch_data))
}

fn serialize_to_batches(
    block: DataBlock,
    ipc_options: &IpcWriteOptions,
) -> Result<(Vec<FlightData>, Vec<FlightData>)> {
    if block.is_empty() {
        let empty_batch = RecordBatch::try_new_with_options(
            Arc::new(ArrowSchema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(0)),
        )?;
        let (dicts, batch) = encode_batch(&empty_batch, ipc_options)?;
        Ok((dicts, vec![batch]))
    } else {
        let schema = block.infer_schema();
        let arrow_schema = ArrowSchema::from(&schema);
        let batch = block.to_record_batch_with_dataschema(&schema)?;
        let _ = &arrow_schema; // used for schema inference, batch carries it
        let (dicts, batch_data) = encode_batch(&batch, ipc_options)?;
        Ok((dicts, vec![batch_data]))
    }
}

/// Serialize a DataBlock into a list of FlightData (dictionaries + fragments).
/// The `descriptor` is attached to each FlightData for routing.
pub(crate) fn serialize_block(
    block: DataBlock,
    ipc_options: &IpcWriteOptions,
    descriptor: Option<FlightDescriptor>,
) -> Result<Vec<FlightData>> {
    if block.is_empty() && block.get_meta().is_none() {
        return Ok(vec![]);
    }

    // Build metadata (row count + block meta) before moving block
    let mut meta = vec![];
    meta.write_scalar_own(block.num_rows() as u32)?;
    bincode_serialize_into_buf(&mut meta, &block.get_meta())?;

    let (dict_data, value_data) = serialize_to_batches(block, ipc_options)?;

    let meta_bytes: Bytes = meta.into();
    let mut result = Vec::with_capacity(dict_data.len() + value_data.len());

    for mut dict in dict_data {
        let mut app_metadata = dict.app_metadata.to_vec();
        app_metadata.push(DICTIONARY_MARKER);
        dict.app_metadata = app_metadata.into();
        dict.flight_descriptor = descriptor.clone();
        result.push(dict);
    }

    for value in value_data {
        let mut metadata = meta_bytes.to_vec();
        metadata.push(FRAGMENT_MARKER);
        result.push(FlightData {
            app_metadata: metadata.into(),
            data_body: value.data_body,
            data_header: value.data_header,
            flight_descriptor: descriptor.clone(),
        });
    }

    Ok(result)
}

/// Serialize a block for thread `tid` of a remote node: the tid is prefixed to the
/// app_metadata of every FlightData so the receiver can route it.
pub(crate) fn serialize_block_for_thread(
    block: DataBlock,
    tid: usize,
    ipc_options: &IpcWriteOptions,
) -> Result<Vec<FlightData>> {
    let tid_prefix = (tid as u16).to_le_bytes();
    let flight_data_list = serialize_block(block, ipc_options, None)?;

    Ok(flight_data_list
        .into_iter()
        .map(|flight_data| {
            let mut metadata = tid_prefix.to_vec();
            metadata.extend_from_slice(&flight_data.app_metadata);
            FlightData {
                app_metadata: metadata.into(),
                ..flight_data
            }
        })
        .collect())
}

/// Encode `block` for thread `tid` of a remote node, in the same format as
/// [`serialize_block_for_thread`], with `arrow_schema` built once by the caller.
///
/// With `compression`, the whole IPC body is compressed at once instead of each
/// buffer: a per-buffer compressor costs a fresh encoder and its buffers, which
/// dominates when the blocks sent to each thread are small.
pub(crate) fn encode_block_for_thread(
    tid: usize,
    block: DataBlock,
    arrow_schema: &Arc<ArrowSchema>,
    ipc_options: &IpcWriteOptions,
    compression: Option<FlightCompression>,
) -> Result<FlightData> {
    let mut app_metadata = Vec::with_capacity(16);
    app_metadata.extend_from_slice(&(tid as u16).to_le_bytes());
    app_metadata.write_scalar_own(block.num_rows() as u32)?;
    bincode_serialize_into_buf(&mut app_metadata, &block.get_meta())?;

    // A block whose meta overrides the schema is decoded with it, see `deserialize_flight_data`.
    let block_schema = block
        .get_meta()
        .and_then(|meta| meta.override_block_schema())
        .map(|schema| Arc::new(ArrowSchema::from(schema.as_ref())));
    let arrow_schema = block_schema.as_ref().unwrap_or(arrow_schema);
    let batch = block.to_record_batch_with_arrow_schema(arrow_schema)?;
    let (dictionaries, mut data) = encode_batch(&batch, ipc_options)?;
    if !dictionaries.is_empty() {
        return Err(ErrorCode::Unimplemented(
            "dictionary encoded columns are not supported in hash shuffle",
        ));
    }

    match compression {
        Some(compression) => {
            let (body, marker) = compress_body(compression, &data.data_body)?;
            data.data_body = body.into();
            app_metadata.push(marker);
        }
        None => app_metadata.push(FRAGMENT_MARKER),
    }

    Ok(FlightData {
        app_metadata: app_metadata.into(),
        ..data
    })
}

/// Compress a whole IPC body. Returns the compressed body and its marker.
fn compress_body(compression: FlightCompression, body: &[u8]) -> Result<(Vec<u8>, u8)> {
    match compression {
        FlightCompression::Lz4 => Ok((
            lz4_flex::block::compress_prepend_size(body),
            LZ4_BODY_MARKER,
        )),
        FlightCompression::Zstd => {
            let size = u32::try_from(body.len()).map_err(|_| {
                ErrorCode::Internal(format!("IPC body of {} bytes is too large", body.len()))
            })?;
            let compressed = zstd::bulk::compress(body, zstd::DEFAULT_COMPRESSION_LEVEL)?;
            let mut data = Vec::with_capacity(4 + compressed.len());
            data.extend_from_slice(&size.to_le_bytes());
            data.extend_from_slice(&compressed);
            Ok((data, ZSTD_BODY_MARKER))
        }
    }
}

// ---------------------------------------------------------------------------
// Batch frames
// ---------------------------------------------------------------------------

/// Detect a batch frame built by `merge_flight_data_batch`.
pub(crate) fn is_batch(data: &FlightData) -> bool {
    data.app_metadata.len() == 5 && data.app_metadata[4] == BATCH_MARKER
}

/// Pack several FlightData into one batch frame.
///
/// Each item keeps its whole app_metadata, including the tid prefix, so one frame
/// can carry data for any threads of the destination node.
///
/// Frame app_metadata: `[num_items: u32][BATCH_MARKER]`.
/// Frame body, per item: `[meta_len: u32][meta][header_len: u32][header][body_len: u32][body]`.
pub(crate) fn merge_flight_data_batch(items: Vec<FlightData>) -> FlightData {
    let mut app_metadata = BytesMut::with_capacity(5);
    app_metadata.put_u32_le(items.len() as u32);
    app_metadata.put_u8(BATCH_MARKER);

    let estimated: usize = items
        .iter()
        .map(|i| 12 + i.app_metadata.len() + i.data_header.len() + i.data_body.len())
        .sum();

    let mut body = BytesMut::with_capacity(estimated);
    for item in items {
        body.put_u32_le(item.app_metadata.len() as u32);
        body.put_slice(&item.app_metadata);
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

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

/// Deserialize a FlightData (after tid stripping) back into a DataBlock.
///
/// Format of `app_metadata`:
/// - Fragment ([`FRAGMENT_MARKER`]): `[row_count: u32][block_meta: bincode][marker]`
/// - Fragment with a compressed body ([`LZ4_BODY_MARKER`], [`ZSTD_BODY_MARKER`]):
///   same metadata as a fragment, see [`decode_compressed_body`]
/// - Dictionary ([`DICTIONARY_MARKER`]): dictionary IPC data (currently unsupported)
pub(crate) fn deserialize_flight_data(
    flight_data: FlightData,
    schema: &DataSchemaRef,
    arrow_schema: &Arc<ArrowSchema>,
) -> Result<DataBlock> {
    let meta_bytes = &flight_data.app_metadata;
    if meta_bytes.is_empty() {
        return Err(ErrorCode::BadBytes("empty app_metadata in FlightData"));
    }

    let marker = meta_bytes[meta_bytes.len() - 1];
    if marker == DICTIONARY_MARKER {
        return Err(ErrorCode::Unimplemented(
            "dictionary FlightData not yet supported in broadcast exchange",
        ));
    }

    if !matches!(marker, FRAGMENT_MARKER | LZ4_BODY_MARKER | ZSTD_BODY_MARKER) {
        return Err(ErrorCode::BadBytes(format!(
            "unknown FlightData marker: 0x{:02x}",
            marker
        )));
    }

    // Parse metadata (excluding the trailing marker)
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
        FRAGMENT_MARKER => flight_data_to_arrow_batch(&flight_data, arrow_schema, &HashMap::new())
            .map_err(|e| ErrorCode::BadBytes(format!("failed to decode arrow batch: {}", e)))?,
        _ => decode_compressed_body(&flight_data, marker, arrow_schema)?,
    };

    let block = DataBlock::from_record_batch_keep_null_const(&schema, &batch)?;

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
) -> Result<RecordBatch> {
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
    use super::*;

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

    /// An item retained by a slow thread must not keep the entire frame alive.
    #[test]
    fn test_split_batch_does_not_retain_multi_thread_frame() {
        let frame_items = |tids: &[u16]| {
            tids.iter()
                .map(|tid| make_item(*tid, &[0x01], &[1, 2], &[*tid as u8; 1024]))
                .collect::<Vec<_>>()
        };

        let batch = merge_flight_data_batch(frame_items(&[0, 1, 2, 3]));
        let frame = batch.data_body.clone();
        let frame_range = frame.as_ptr_range();

        for item in split_batch_flight_data(batch) {
            assert!(!frame_range.contains(&item.data_body.as_ptr()));
        }
    }
}
