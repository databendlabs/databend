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

use std::io::Write;

use arrow_array::RecordBatch;
use arrow_ipc::BodyCompressionArgs;
use arrow_ipc::BodyCompressionMethod;
use arrow_ipc::Buffer;
use arrow_ipc::CompressionType;
use arrow_ipc::KeyValue;
use arrow_ipc::KeyValueArgs;
use arrow_ipc::MessageArgs;
use arrow_ipc::MessageHeader;
use arrow_ipc::RecordBatchArgs;
use arrow_ipc::root_as_message;
use arrow_ipc::writer::CompressionContext;
use arrow_ipc::writer::DictionaryTracker;
use arrow_ipc::writer::EncodedData;
use arrow_ipc::writer::IpcDataGenerator;
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::ArrowError;
use databend_common_settings::FlightCompression;
use flatbuffers::FlatBufferBuilder;

const IPC_ALIGNMENT: usize = 64;

/// Exchange IPC options. LZ4 is applied after Arrow has encoded the body so that
/// buffers use the native frame encoder without changing the Arrow wire format.
#[derive(Clone)]
pub struct FlightIpcOptions {
    options: IpcWriteOptions,
    native_lz4: bool,
}

impl FlightIpcOptions {
    pub fn new(compression: Option<FlightCompression>) -> Result<Self, ArrowError> {
        let codec = match compression {
            Some(FlightCompression::Zstd) => Some(CompressionType::ZSTD),
            _ => None,
        };
        Ok(Self {
            options: IpcWriteOptions::default().try_with_compression(codec)?,
            native_lz4: matches!(compression, Some(FlightCompression::Lz4)),
        })
    }

    pub fn arrow_options(&self) -> &IpcWriteOptions {
        &self.options
    }

    pub(crate) fn encode(
        &self,
        batch: &RecordBatch,
        dictionary_tracker: &mut DictionaryTracker,
        compression_context: &mut CompressionContext,
    ) -> Result<(Vec<EncodedData>, EncodedData), ArrowError> {
        let (dictionaries, batch) = IpcDataGenerator::default().encode(
            batch,
            dictionary_tracker,
            &self.options,
            compression_context,
        )?;
        // Dictionary messages remain valid uncompressed IPC messages. Compression
        // is described per message, not by the schema or the surrounding stream.
        let batch = if self.native_lz4 {
            compress_record_batch_lz4(batch)?
        } else {
            batch
        };
        Ok((dictionaries, batch))
    }
}

fn ipc_error(message: impl Into<String>) -> ArrowError {
    ArrowError::IpcError(message.into())
}

fn compress_buffer(input: &[u8], output: &mut Vec<u8>) -> Result<usize, ArrowError> {
    if input.is_empty() {
        return Ok(0);
    }
    let start = output.len();
    let len = i64::try_from(input.len())
        .map_err(|e| ipc_error(format!("IPC buffer is too large: {e}")))?;
    output.extend_from_slice(&len.to_le_bytes());
    let block_size = if input.len() > 256 * 1024 {
        lz4::BlockSize::Max4MB
    } else if input.len() > 64 * 1024 {
        lz4::BlockSize::Max256KB
    } else {
        lz4::BlockSize::Max64KB
    };
    let mut encoder = lz4::EncoderBuilder::new()
        .block_size(block_size)
        .block_checksum(lz4::liblz4::BlockChecksum::NoBlockChecksum)
        .checksum(lz4::ContentChecksum::NoChecksum)
        .build(&mut *output)?;
    encoder.write_all(input)?;
    let (_, result) = encoder.finish();
    result?;

    // Match Arrow's decision, including the size prefix in the comparison.
    if output.len() - start > input.len() {
        output.truncate(start);
        output.extend_from_slice(&(-1_i64).to_le_bytes());
        output.extend_from_slice(input);
    }
    Ok(output.len() - start)
}

fn compress_record_batch_lz4(encoded: EncodedData) -> Result<EncodedData, ArrowError> {
    let message = root_as_message(&encoded.ipc_message)
        .map_err(|e| ipc_error(format!("invalid generated IPC message: {e}")))?;
    let batch = message
        .header_as_record_batch()
        .ok_or_else(|| ipc_error("generated IPC message is not a record batch"))?;
    if batch.compression().is_some() {
        return Err(ipc_error("generated IPC batch is already compressed"));
    }
    let source_buffers = batch
        .buffers()
        .ok_or_else(|| ipc_error("generated IPC batch has no buffers"))?;
    let mut arrow_data = Vec::with_capacity(encoded.arrow_data.len());
    let mut buffers = Vec::with_capacity(source_buffers.len());
    for source in source_buffers {
        let offset = usize::try_from(source.offset())
            .map_err(|e| ipc_error(format!("invalid IPC buffer offset: {e}")))?;
        let len = usize::try_from(source.length())
            .map_err(|e| ipc_error(format!("invalid IPC buffer length: {e}")))?;
        let end = offset
            .checked_add(len)
            .ok_or_else(|| ipc_error("IPC buffer range overflow"))?;
        let source = encoded
            .arrow_data
            .get(offset..end)
            .ok_or_else(|| ipc_error("IPC buffer is outside the message body"))?;
        let offset = i64::try_from(arrow_data.len())
            .map_err(|e| ipc_error(format!("IPC body is too large: {e}")))?;
        let len = i64::try_from(compress_buffer(source, &mut arrow_data)?)
            .map_err(|e| ipc_error(format!("compressed IPC buffer is too large: {e}")))?;
        buffers.push(Buffer::new(offset, len));
        let padding = (IPC_ALIGNMENT - arrow_data.len() % IPC_ALIGNMENT) % IPC_ALIGNMENT;
        let padded_len = arrow_data
            .len()
            .checked_add(padding)
            .ok_or_else(|| ipc_error("IPC body alignment overflow"))?;
        arrow_data.resize(padded_len, 0);
    }

    let mut builder = FlatBufferBuilder::new();
    let nodes = batch
        .nodes()
        .map(|nodes| builder.create_vector_from_iter(nodes.iter().copied()));
    let buffers = Some(builder.create_vector(&buffers));
    let counts = batch
        .variadicBufferCounts()
        .map(|counts| builder.create_vector_from_iter(counts.iter()));
    let compression = Some(arrow_ipc::BodyCompression::create(
        &mut builder,
        &BodyCompressionArgs {
            codec: CompressionType::LZ4_FRAME,
            method: BodyCompressionMethod::BUFFER,
        },
    ));
    let header = arrow_ipc::RecordBatch::create(&mut builder, &RecordBatchArgs {
        length: batch.length(),
        nodes,
        buffers,
        compression,
        variadicBufferCounts: counts,
    })
    .as_union_value();
    let custom_metadata = message.custom_metadata().map(|metadata| {
        let entries = metadata
            .iter()
            .map(|entry| {
                let key = entry.key().map(|key| builder.create_string(key));
                let value = entry.value().map(|value| builder.create_string(value));
                KeyValue::create(&mut builder, &KeyValueArgs { key, value })
            })
            .collect::<Vec<_>>();
        builder.create_vector(&entries)
    });
    let body_len = i64::try_from(arrow_data.len())
        .map_err(|e| ipc_error(format!("IPC body is too large: {e}")))?;
    let message = arrow_ipc::Message::create(&mut builder, &MessageArgs {
        version: message.version(),
        header_type: MessageHeader::RecordBatch,
        header: Some(header),
        bodyLength: body_len,
        custom_metadata,
    });
    builder.finish(message, None);
    Ok(EncodedData {
        ipc_message: builder.finished_data().to_vec(),
        arrow_data,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Read;
    use std::sync::Arc;

    use arrow_array::RecordBatchOptions;
    use arrow_flight::FlightData;
    use arrow_flight::utils::flight_data_to_arrow_batch;
    use arrow_ipc::FieldNode;
    use arrow_schema::Schema;

    use super::*;

    #[test]
    fn test_buffer_frames_and_raw_fallback() {
        // Exercise each frame block-size choice, and multiple blocks per frame.
        for size in [
            0,
            1,
            8,
            64 * 1024,
            64 * 1024 + 1,
            256 * 1024 + 1,
            5 * 1024 * 1024,
        ] {
            let input = vec![7; size];
            let mut output = vec![2, 4];
            let written = compress_buffer(&input, &mut output).unwrap();
            assert_eq!(&output[..2], &[2, 4]);
            assert_eq!(written, output.len() - 2);
            if size == 0 {
                assert_eq!(written, 0);
                continue;
            }
            let prefix = i64::from_le_bytes(output[2..10].try_into().unwrap());
            if size < 64 {
                assert_eq!(prefix, -1);
                assert_eq!(&output[10..], input);
            } else {
                assert_eq!(prefix, size as i64);
                let mut decoded = Vec::new();
                lz4_flex::frame::FrameDecoder::new(&output[10..])
                    .read_to_end(&mut decoded)
                    .unwrap();
                assert_eq!(decoded, input);
            }
        }
    }

    fn generated_message(buffers: &[Buffer], body: Vec<u8>, compressed: bool) -> EncodedData {
        let mut builder = FlatBufferBuilder::new();
        let nodes = Some(builder.create_vector(&[FieldNode::new(0, 0)]));
        let buffers = Some(builder.create_vector(buffers));
        let compression = compressed.then(|| {
            arrow_ipc::BodyCompression::create(&mut builder, &BodyCompressionArgs::default())
        });
        let batch = arrow_ipc::RecordBatch::create(&mut builder, &RecordBatchArgs {
            nodes,
            buffers,
            compression,
            ..Default::default()
        });
        let key = Some(builder.create_string("tag"));
        let value = Some(builder.create_string("leaf"));
        let entry = KeyValue::create(&mut builder, &KeyValueArgs { key, value });
        let custom_metadata = Some(builder.create_vector(&[entry]));
        let message = arrow_ipc::Message::create(&mut builder, &MessageArgs {
            version: arrow_ipc::MetadataVersion::V5,
            header_type: MessageHeader::RecordBatch,
            header: Some(batch.as_union_value()),
            bodyLength: body.len() as i64,
            custom_metadata,
        });
        builder.finish(message, None);
        EncodedData {
            ipc_message: builder.finished_data().to_vec(),
            arrow_data: body,
        }
    }

    #[test]
    fn test_buffer_offsets_lengths_padding_and_metadata() {
        let encoded = generated_message(
            &[Buffer::new(0, 0), Buffer::new(0, 1), Buffer::new(1, 512)],
            vec![9; 513],
            false,
        );
        let encoded = compress_record_batch_lz4(encoded).unwrap();
        let message = root_as_message(&encoded.ipc_message).unwrap();
        assert_eq!(message.bodyLength() as usize, encoded.arrow_data.len());
        let entry = message.custom_metadata().unwrap().get(0);
        assert_eq!(entry.key(), Some("tag"));
        assert_eq!(entry.value(), Some("leaf"));
        let buffers = message.header_as_record_batch().unwrap().buffers().unwrap();
        assert_eq!(buffers.get(0), &Buffer::new(0, 0));
        assert_eq!(buffers.get(1), &Buffer::new(0, 9));
        assert_eq!(buffers.get(2).offset(), 64);
        for (index, buffer) in buffers.iter().enumerate() {
            assert_eq!(buffer.offset() % 64, 0);
            let end = (buffer.offset() + buffer.length()) as usize;
            let next = if index + 1 == buffers.len() {
                encoded.arrow_data.len()
            } else {
                buffers.get(index + 1).offset() as usize
            };
            assert!(encoded.arrow_data[end..next].iter().all(|byte| *byte == 0));
        }
    }

    #[test]
    fn test_invalid_generated_messages_return_errors() {
        for buffer in [
            Buffer::new(-1, 0),
            Buffer::new(0, -1),
            Buffer::new(1, 1),
            Buffer::new(i64::MAX, i64::MAX),
        ] {
            assert!(
                compress_record_batch_lz4(generated_message(&[buffer], vec![], false)).is_err()
            );
        }
        assert!(compress_record_batch_lz4(generated_message(&[], vec![], true)).is_err());
        assert!(
            compress_record_batch_lz4(EncodedData {
                ipc_message: vec![0],
                arrow_data: vec![],
            })
            .is_err()
        );
    }

    #[test]
    fn test_nullable_nested_and_variadic_buffers_with_stock_reader() {
        use arrow_array::ArrayRef;
        use arrow_array::ListArray;
        use arrow_array::builder::StringViewBuilder;
        use arrow_array::types::Int32Type;

        let mut strings = StringViewBuilder::new().with_fixed_block_size(128);
        for index in 0..12 {
            if index % 3 == 0 {
                strings.append_null();
            } else {
                strings.append_value("moss".repeat(20));
            }
        }
        let strings = strings.finish();
        assert!(strings.data_buffers().len() > 1);
        let lists = ListArray::from_iter_primitive::<Int32Type, _, _>((0..12).map(|index| {
            match index % 3 {
                0 => None,
                1 => Some(vec![]),
                _ => Some(vec![Some(2), None]),
            }
        }));
        let batch = RecordBatch::try_from_iter(vec![
            ("label", Arc::new(strings) as ArrayRef),
            ("values", Arc::new(lists) as ArrayRef),
        ])
        .unwrap();
        let options = FlightIpcOptions::new(Some(FlightCompression::Lz4)).unwrap();
        let (_, encoded) = options
            .encode(
                &batch,
                &mut DictionaryTracker::new(false),
                &mut CompressionContext::default(),
            )
            .unwrap();
        let data: FlightData = encoded.into();
        let message = root_as_message(&data.data_header).unwrap();
        let counts = message
            .header_as_record_batch()
            .unwrap()
            .variadicBufferCounts()
            .unwrap();
        assert!(counts.get(0) > 1);
        let decoded = flight_data_to_arrow_batch(&data, batch.schema(), &HashMap::new()).unwrap();
        assert_eq!(decoded, batch);
        assert_eq!(decoded.column(0).null_count(), 4);
    }

    #[test]
    fn test_dictionary_messages_remain_readable() {
        use arrow_array::DictionaryArray;
        use arrow_array::types::Int8Type;

        let dictionary =
            DictionaryArray::<Int8Type>::from_iter([Some("oak"), None, Some("elm"), Some("oak")]);
        let batch = RecordBatch::try_from_iter(vec![(
            "kind",
            Arc::new(dictionary) as arrow_array::ArrayRef,
        )])
        .unwrap();
        let options = FlightIpcOptions::new(Some(FlightCompression::Lz4)).unwrap();
        let (dictionaries, encoded) = options
            .encode(
                &batch,
                &mut DictionaryTracker::new(false),
                &mut CompressionContext::default(),
            )
            .unwrap();
        assert_eq!(dictionaries.len(), 1);
        let mut by_id = HashMap::new();
        for dictionary in dictionaries {
            let message = root_as_message(&dictionary.ipc_message).unwrap();
            arrow_ipc::reader::read_dictionary(
                &arrow_buffer::Buffer::from_vec(dictionary.arrow_data),
                message.header_as_dictionary_batch().unwrap(),
                batch.schema().as_ref(),
                &mut by_id,
                &message.version(),
            )
            .unwrap();
        }
        let data: FlightData = encoded.into();
        assert_eq!(
            flight_data_to_arrow_batch(&data, batch.schema(), &by_id).unwrap(),
            batch
        );
    }

    #[test]
    fn test_empty_record_batch_with_stock_reader() {
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::try_new_with_options(
            schema.clone(),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(0)),
        )
        .unwrap();
        let options = FlightIpcOptions::new(Some(FlightCompression::Lz4)).unwrap();
        let (dictionaries, encoded) = options
            .encode(
                &batch,
                &mut DictionaryTracker::new(false),
                &mut CompressionContext::default(),
            )
            .unwrap();
        assert!(dictionaries.is_empty());
        assert!(encoded.arrow_data.is_empty());
        let data: FlightData = encoded.into();
        assert_eq!(
            flight_data_to_arrow_batch(&data, schema, &HashMap::new()).unwrap(),
            batch
        );
    }
}
