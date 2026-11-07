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

use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_ipc::BodyCompressionMethod;
use arrow_ipc::CompressionType;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::StringType;
use databend_common_settings::FlightCompression;
use databend_query::servers::flight::v1::exchange::serde::ExchangeSerializeMeta;
use databend_query::servers::flight::v1::exchange::serde::serialize_block;
use databend_query::servers::flight::v1::ipc_compression::FlightIpcOptions;
use databend_query::servers::flight::v1::packets::DataPacket;

#[test]
fn test_exchange_ipc_codecs_with_stock_reader() {
    let block = DataBlock::new_from_columns(vec![
        StringType::from_data(vec!["reed".repeat(40); 96]),
        Int32Type::from_data(vec![3; 96]),
    ]);
    let schema = block.infer_schema();
    let expected = block
        .clone()
        .to_record_batch_with_dataschema(&schema)
        .unwrap();
    for (compression, codec) in [
        (
            Some(FlightCompression::Lz4),
            Some(CompressionType::LZ4_FRAME),
        ),
        (Some(FlightCompression::Zstd), Some(CompressionType::ZSTD)),
        (None, None),
    ] {
        let options = FlightIpcOptions::new(compression).unwrap();
        let serialized = serialize_block(0, block.clone(), &options).unwrap();
        let meta =
            ExchangeSerializeMeta::downcast_ref_from(serialized.get_meta().unwrap()).unwrap();
        assert_eq!(meta.packet.len(), 1);
        let DataPacket::FragmentData(fragment) = &meta.packet[0] else {
            panic!("expected record batch");
        };
        let data = &fragment.data;
        let message = arrow_ipc::root_as_message(&data.data_header).unwrap();
        let batch = message.header_as_record_batch().unwrap();
        assert_eq!(message.bodyLength() as usize, data.data_body.len());
        assert_eq!(batch.compression().map(|c| c.codec()), codec);
        if let Some(compression) = batch.compression() {
            assert_eq!(compression.method(), BodyCompressionMethod::BUFFER);
        }
        if matches!(codec, Some(CompressionType::LZ4_FRAME)) {
            let buffers = batch.buffers().unwrap();
            assert!(buffers.iter().any(|buffer| buffer.length() > 0));
            for buffer in buffers {
                assert_eq!(buffer.offset() % 64, 0);
            }
            // String views require the variadic data-buffer counts to survive
            // the metadata rebuild, including when more than one field exists.
            assert!(batch.variadicBufferCounts().is_some());
        }
        let actual = flight_data_to_arrow_batch(data, expected.schema(), &HashMap::new()).unwrap();
        assert_eq!(actual, expected);
    }
}

#[test]
fn test_empty_exchange_still_emits_no_packets() {
    let options = FlightIpcOptions::new(Some(FlightCompression::Lz4)).unwrap();
    let serialized = serialize_block(0, DataBlock::empty(), &options).unwrap();
    let meta = ExchangeSerializeMeta::downcast_ref_from(serialized.get_meta().unwrap()).unwrap();
    assert!(meta.packet.is_empty());
}
