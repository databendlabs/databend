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

use arrow_schema::Schema as ArrowSchema;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_io::prelude::BinaryRead;
use databend_common_io::prelude::bincode_deserialize_from_slice;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_pipeline_transforms::processors::AccumulatingTransformer;

use crate::pipelines::processors::transforms::aggregator::AggregateSerdeMeta;
use crate::servers::flight::v1::exchange::serde::ExchangeDeserializeMeta;
use crate::servers::flight::v1::exchange::serde::deserialize_block;
use crate::servers::flight::v1::network::ExchangeDataCodec;
use crate::servers::flight::v1::packets::DataPacket;
use crate::servers::flight::v1::packets::FragmentData;

pub struct TransformDeserializer {
    schema: DataSchemaRef,
    arrow_schema: Arc<ArrowSchema>,
    codec: Arc<dyn ExchangeDataCodec>,
}

impl TransformDeserializer {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        schema: &DataSchemaRef,
        codec: Arc<dyn ExchangeDataCodec>,
    ) -> Result<ProcessorPtr> {
        let arrow_schema = ArrowSchema::from(schema.as_ref());

        Ok(ProcessorPtr::create(AccumulatingTransformer::create(
            input,
            output,
            TransformDeserializer {
                arrow_schema: Arc::new(arrow_schema),
                schema: schema.clone(),
                codec,
            },
        )))
    }

    fn recv_data(
        &self,
        dict: Vec<DataPacket>,
        fragment_data: FragmentData,
    ) -> Result<Vec<DataBlock>> {
        const ROW_HEADER_SIZE: usize = std::mem::size_of::<u32>();

        let meta = bincode_deserialize_from_slice(&fragment_data.get_meta()[ROW_HEADER_SIZE..])
            .map_err(|_| ErrorCode::BadBytes("block meta deserialize error when exchange"))?;

        let mut row_count_meta = &fragment_data.get_meta()[..ROW_HEADER_SIZE];
        let row_count: u32 = row_count_meta.read_scalar()?;

        if row_count == 0 {
            return Ok(vec![DataBlock::new_with_meta(vec![], 0, meta)]);
        }

        let is_aggregate = meta
            .as_ref()
            .and_then(AggregateSerdeMeta::downcast_ref_from)
            .is_some();
        let dynamic_schema = if is_aggregate {
            meta.as_ref().and_then(|meta| meta.override_block_schema())
        } else {
            None
        };
        let (schema, arrow_schema) = match dynamic_schema {
            Some(schema) => {
                let arrow_schema = Arc::new(ArrowSchema::from(schema.as_ref()));
                (schema, arrow_schema)
            }
            None => (self.schema.clone(), self.arrow_schema.clone()),
        };
        let block = deserialize_block(dict, fragment_data, &schema, arrow_schema)?;
        let block = if block.num_columns() == 0 {
            DataBlock::new_with_meta(vec![], row_count as usize, meta)
        } else {
            block.add_meta(meta)?
        };

        if is_aggregate {
            Ok(self.codec.decode(block)?.into_iter().collect())
        } else {
            Ok(vec![block])
        }
    }
}

impl TransformDeserializer {
    fn transform_exchange_meta(
        &mut self,
        mut meta: ExchangeDeserializeMeta,
    ) -> Result<Vec<DataBlock>> {
        match meta.packet.pop().unwrap() {
            DataPacket::FragmentData(v) => self.recv_data(meta.packet, v),
            DataPacket::ErrorCode(err) => Err(err),
            _ => unreachable!(),
        }
    }
}

impl AccumulatingTransform for TransformDeserializer {
    const NAME: &'static str = "TransformDeserializer";

    fn transform(&mut self, mut data_block: DataBlock) -> Result<Vec<DataBlock>> {
        if let Some(block_meta_ref) = data_block.get_meta() {
            if ExchangeDeserializeMeta::downcast_ref_from(block_meta_ref).is_some() {
                let block_meta = data_block.take_meta().unwrap();
                let Some(meta) = ExchangeDeserializeMeta::downcast_from(block_meta) else {
                    unreachable!("block_meta_ref is ExchangeDeserializeMeta");
                };

                if data_block.num_rows() != 0 {
                    return Err(ErrorCode::Internal("DataBlockMeta has rows"));
                }

                return self.transform_exchange_meta(meta);
            }
        }

        Ok(vec![data_block])
    }
}

pub type TransformAggregateDeserializer = TransformDeserializer;

#[cfg(test)]
mod tests {
    use arrow_flight::FlightData;
    use arrow_ipc::writer::IpcWriteOptions;
    use databend_common_expression::FromData;
    use databend_common_expression::types::Int64Type;
    use parquet::file::metadata::RowGroupMetaData;
    use parquet::schema::types::SchemaDescriptor;
    use parquet::schema::types::Type;

    use super::*;
    use crate::pipelines::processors::transforms::aggregator::AggregateExchangeDataCodec;
    use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
    use crate::pipelines::processors::transforms::aggregator::PartitionedData;
    use crate::pipelines::processors::transforms::aggregator::SpilledPayload;
    use crate::pipelines::processors::transforms::aggregator::aggregate_exchange_codec::tests::params;
    use crate::pipelines::processors::transforms::aggregator::aggregate_exchange_codec::tests::payload_block;
    use crate::servers::flight::v1::exchange::serde::ExchangeSerializeMeta;
    use crate::servers::flight::v1::exchange::serde::serialize_block;

    fn decoder() -> TransformDeserializer {
        let params = params();
        let schema = params.spill_schema();
        TransformDeserializer {
            arrow_schema: Arc::new(ArrowSchema::from(schema.as_ref())),
            schema,
            codec: AggregateExchangeDataCodec::create(params),
        }
    }

    fn packet_round_trip(block: DataBlock) -> Result<Vec<DataBlock>> {
        let mut serialized = serialize_block(0, block, &IpcWriteOptions::default())?;
        let meta = serialized
            .take_meta()
            .and_then(ExchangeSerializeMeta::downcast_from)
            .unwrap();
        // The Flight envelope appends the packet tag consumed by get_meta().
        let packets = meta
            .packet
            .into_iter()
            .map(|packet| DataPacket::try_from(FlightData::try_from(packet)?))
            .collect::<Result<Vec<_>>>()?;
        decoder().transform(DataBlock::empty_with_meta(ExchangeDeserializeMeta::create(
            packets,
        )))
    }

    #[test]
    fn test_legacy_packets_decode_aggregate_and_ordinary_blocks() -> Result<()> {
        let transport = AggregateExchangeDataCodec::create(params())
            .encode(payload_block(vec![11, 22]))?
            .unwrap();
        let mut restored = packet_round_trip(transport)?;
        let Some(AggregateMeta::Serialized(payload)) = restored[0]
            .take_meta()
            .and_then(AggregateMeta::downcast_from)
        else {
            panic!("payload was not decoded")
        };
        assert_eq!((payload.bucket, payload.max_partition_count), (7, 8));
        assert_eq!(
            payload.data_block.columns(),
            DataBlock::new_from_columns(vec![Int64Type::from_data(vec![11, 22])]).columns()
        );

        let ordinary = DataBlock::new_from_columns(vec![Int64Type::from_data(vec![3, 4])]);
        let restored = packet_round_trip(ordinary.clone())?;
        assert_eq!(restored[0].columns(), ordinary.columns());
        let local = decoder().transform(payload_block(vec![5]))?;
        assert!(matches!(
            local[0]
                .get_meta()
                .and_then(AggregateMeta::downcast_ref_from),
            Some(AggregateMeta::AggregatePayload(_))
        ));
        Ok(())
    }

    #[test]
    fn test_legacy_packets_preserve_zero_row_partition_and_spill_schema() -> Result<()> {
        let transport = DataBlock::new_from_columns(vec![Int64Type::from_data(vec![10, 20])])
            .add_meta(Some(AggregateSerdeMeta::create_partitioned_payload(
                vec![4, 9],
                vec![0, 2],
                false,
            )))?;
        let mut restored = packet_round_trip(transport)?;
        let Some(AggregateMeta::Partitioned {
            data: PartitionedData::Serialized(payloads),
            ..
        }) = restored[0]
            .take_meta()
            .and_then(AggregateMeta::downcast_from)
        else {
            panic!("partitioned payload was not decoded")
        };
        assert_eq!(payloads.len(), 2);
        assert_eq!(payloads[0].bucket, 4);
        assert_eq!(payloads[0].data_block.num_columns(), 0);
        assert_eq!(payloads[1].data_block.num_rows(), 2);

        let schema = Type::group_type_builder("schema").build().unwrap();
        let row_group =
            RowGroupMetaData::builder(Arc::new(SchemaDescriptor::new(Arc::new(schema))))
                .set_num_rows(17)
                .build()
                .unwrap();
        let block = DataBlock::empty_with_meta(AggregateMeta::create_partitioned(
            None,
            PartitionedData::BucketSpilled(vec![SpilledPayload {
                bucket: 6,
                location: "memory://spill".to_string(),
                row_group,
            }]),
        ));
        let transport = AggregateExchangeDataCodec::create(params())
            .encode(block)?
            .unwrap();
        let mut restored = packet_round_trip(transport)?;
        let Some(AggregateMeta::Partitioned {
            data: PartitionedData::BucketSpilled(payloads),
            ..
        }) = restored[0]
            .take_meta()
            .and_then(AggregateMeta::downcast_from)
        else {
            panic!("spill reference was not decoded")
        };
        assert_eq!(payloads[0].bucket, 6);
        assert_eq!(payloads[0].location, "memory://spill");
        assert_eq!(payloads[0].row_group.num_rows(), 17);
        Ok(())
    }
}
