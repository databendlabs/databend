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
use databend_common_expression::types::AccessType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_io::prelude::bincode_deserialize_from_slice;
use databend_common_io::prelude::BinaryRead;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::processors::BlockMetaTransform;
use databend_common_pipeline_transforms::processors::BlockMetaTransformer;
use databend_common_pipeline_transforms::processors::UnknownMode;

use crate::pipelines::processors::transforms::aggregator::exchange_defines;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregateSerdeMeta;
use crate::pipelines::processors::transforms::aggregator::BucketSpilledPayload;
use crate::pipelines::processors::transforms::aggregator::BUCKET_TYPE;
use crate::servers::flight::v1::exchange::serde::deserialize_block;
use crate::servers::flight::v1::exchange::serde::ExchangeDeserializeMeta;
use crate::servers::flight::v1::packets::DataPacket;
use crate::servers::flight::v1::packets::FragmentData;

pub struct TransformDeserializer {
    schema: DataSchemaRef,
    arrow_schema: Arc<ArrowSchema>,
}

impl TransformDeserializer {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        schema: &DataSchemaRef,
    ) -> Result<ProcessorPtr> {
        let arrow_schema = ArrowSchema::from(schema.as_ref());

        Ok(ProcessorPtr::create(BlockMetaTransformer::create(
            input,
            output,
            TransformDeserializer {
                arrow_schema: Arc::new(arrow_schema),
                schema: schema.clone(),
            },
        )))
    }

    fn recv_data(&self, dict: Vec<DataPacket>, fragment_data: FragmentData) -> Result<DataBlock> {
        const ROW_HEADER_SIZE: usize = std::mem::size_of::<u32>();

        let meta = bincode_deserialize_from_slice(&fragment_data.get_meta()[ROW_HEADER_SIZE..])
            .map_err(|_| ErrorCode::BadBytes("block meta deserialize error when exchange"))?;

        let mut row_count_meta = &fragment_data.get_meta()[..ROW_HEADER_SIZE];
        let row_count: u32 = row_count_meta.read_scalar()?;

        if row_count == 0 {
            return Ok(DataBlock::new_with_meta(vec![], 0, meta));
        }

        let Some(meta) = meta
            .as_ref()
            .and_then(AggregateSerdeMeta::downcast_ref_from)
        else {
            let data_block =
                deserialize_block(dict, fragment_data, &self.schema, self.arrow_schema.clone())?;
            return match data_block.num_columns() == 0 {
                true => Ok(DataBlock::new_with_meta(vec![], row_count as usize, meta)),
                false => data_block.add_meta(meta),
            };
        };

        match meta.typ {
            BUCKET_TYPE => {
                let mut block = deserialize_block(
                    dict,
                    fragment_data,
                    &self.schema,
                    self.arrow_schema.clone(),
                )?;

                if meta.is_empty {
                    block = block.slice(0..0);
                }

                Ok(DataBlock::empty_with_meta(
                    AggregateMeta::create_serialized(meta.bucket, block, meta.max_partition_count),
                ))
            }
            _ => {
                let data_schema = Arc::new(exchange_defines::spilled_schema());
                let arrow_schema = Arc::new(exchange_defines::spilled_arrow_schema());
                let data_block =
                    deserialize_block(dict, fragment_data, &data_schema, arrow_schema.clone())?;

                let columns = data_block
                    .columns()
                    .iter()
                    .map(|c| c.as_column().unwrap().clone())
                    .collect::<Vec<_>>();

                let buckets = NumberType::<i64>::try_downcast_column(&columns[0]).unwrap();
                let data_range_start = NumberType::<u64>::try_downcast_column(&columns[1]).unwrap();
                let data_range_end = NumberType::<u64>::try_downcast_column(&columns[2]).unwrap();
                let columns_layout =
                    ArrayType::<UInt64Type>::try_downcast_column(&columns[3]).unwrap();

                let columns_layout_data = columns_layout.values().as_slice();
                let columns_layout_offsets = columns_layout.offsets();

                let mut buckets_payload = Vec::with_capacity(data_block.num_rows());
                for index in 0..data_block.num_rows() {
                    unsafe {
                        buckets_payload.push(BucketSpilledPayload {
                            bucket: *buckets.get_unchecked(index) as isize,
                            location: meta.location.clone().unwrap(),
                            data_range: *data_range_start.get_unchecked(index)
                                ..*data_range_end.get_unchecked(index),
                            columns_layout: columns_layout_data[columns_layout_offsets[index]
                                as usize
                                ..columns_layout_offsets[index + 1] as usize]
                                .to_vec(),
                            max_partition_count: meta.max_partition_count,
                        });
                    }
                }

                Ok(DataBlock::empty_with_meta(AggregateMeta::create_spilled(
                    buckets_payload,
                )))
            }
        }
    }
}

impl BlockMetaTransform<ExchangeDeserializeMeta> for TransformDeserializer {
    const UNKNOWN_MODE: UnknownMode = UnknownMode::Pass;
    const NAME: &'static str = "TransformDeserializer";

    fn transform(&mut self, mut meta: ExchangeDeserializeMeta) -> Result<Vec<DataBlock>> {
        match meta.packet.pop().unwrap() {
            DataPacket::FragmentData(v) => Ok(vec![self.recv_data(meta.packet, v)?]),
            DataPacket::ErrorCode(err) => Err(err),
            _ => unreachable!(),
        }
    }
}

pub type TransformAggregateDeserializer = TransformDeserializer;
