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
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_io::prelude::bincode_deserialize_from_slice;
use databend_common_io::prelude::BinaryRead;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_pipeline_transforms::processors::AccumulatingTransformer;
use databend_common_storages_parquet::deserialize_row_group_meta_from_bytes;

use crate::pipelines::processors::transforms::aggregator::exchange_defines;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregateSerdeMeta;
use crate::pipelines::processors::transforms::aggregator::BucketSpilledPayload;
use crate::pipelines::processors::transforms::aggregator::NewSpilledPayload;
use crate::pipelines::processors::transforms::aggregator::SerializedPayload;
use crate::pipelines::processors::transforms::aggregator::BUCKET_TYPE;
use crate::pipelines::processors::transforms::aggregator::NEW_SPILLED_TYPE;
use crate::pipelines::processors::transforms::aggregator::PARTITIONED_AGGREGATE_TYPE;
use crate::pipelines::processors::transforms::aggregator::SPILLED_TYPE;
use crate::servers::flight::v1::exchange::serde::deserialize_block;
use crate::servers::flight::v1::exchange::serde::ExchangeDeserializeMeta;
use crate::servers::flight::v1::exchange::ExchangeShuffleMeta;
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

        Ok(ProcessorPtr::create(AccumulatingTransformer::create(
            input,
            output,
            TransformDeserializer {
                arrow_schema: Arc::new(arrow_schema),
                schema: schema.clone(),
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

        let Some(meta) = meta
            .as_ref()
            .and_then(AggregateSerdeMeta::downcast_ref_from)
        else {
            let data_block =
                deserialize_block(dict, fragment_data, &self.schema, self.arrow_schema.clone())?;
            return match data_block.num_columns() == 0 {
                true => Ok(vec![DataBlock::new_with_meta(
                    vec![],
                    row_count as usize,
                    meta,
                )]),
                false => Ok(vec![data_block.add_meta(meta)?]),
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

                Ok(vec![DataBlock::empty_with_meta(
                    AggregateMeta::create_serialized(meta.bucket, block, meta.max_partition_count),
                )])
            }
            PARTITIONED_AGGREGATE_TYPE => {
                let data_block = deserialize_block(
                    dict,
                    fragment_data,
                    &self.schema,
                    self.arrow_schema.clone(),
                )?;

                if meta.is_empty {
                    return Ok(vec![]);
                }

                if meta.buckets.len() != meta.payload_row_counts.len() {
                    return Err(ErrorCode::Internal(
                        "Invalid partitioned aggregate serde meta".to_string(),
                    ));
                }

                let mut offset = 0;
                let mut metas = Vec::with_capacity(meta.buckets.len());
                for (bucket, rows) in meta.buckets.iter().zip(meta.payload_row_counts.iter()) {
                    let rows = *rows;
                    let start = offset;
                    offset += rows;
                    if offset > data_block.num_rows() {
                        return Err(ErrorCode::Internal(
                            "Partitioned aggregate payload rows exceed block rows".to_string(),
                        ));
                    }

                    let payload_block = if rows == 0 {
                        DataBlock::empty()
                    } else {
                        data_block.slice(start..offset)
                    };

                    metas.push(AggregateMeta::Serialized(SerializedPayload {
                        bucket: *bucket,
                        data_block: payload_block,
                        max_partition_count: 0,
                    }));
                }

                if offset != data_block.num_rows() {
                    return Err(ErrorCode::Internal(
                        "Partitioned aggregate payload rows do not match block rows".to_string(),
                    ));
                }
                Ok(vec![DataBlock::empty_with_meta(
                    AggregateMeta::create_partitioned(None, metas),
                )])
            }
            SPILLED_TYPE => {
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

                Ok(vec![DataBlock::empty_with_meta(
                    AggregateMeta::create_spilled(buckets_payload),
                )])
            }
            NEW_SPILLED_TYPE => {
                let data_schema = Arc::new(exchange_defines::new_spilled_schema());
                let arrow_schema = Arc::new(exchange_defines::new_spilled_arrow_schema());
                let data_block =
                    deserialize_block(dict, fragment_data, &data_schema, arrow_schema.clone())?;

                let columns = data_block
                    .columns()
                    .iter()
                    .map(|c| c.as_column().unwrap().clone())
                    .collect::<Vec<_>>();

                let buckets = NumberType::<i64>::try_downcast_column(&columns[0]).unwrap();
                let locations = StringType::try_downcast_column(&columns[1]).unwrap();
                let row_groups = BinaryType::try_downcast_column(&columns[2]).unwrap();

                let mut spilled_payloads = Vec::with_capacity(data_block.num_rows());
                for index in 0..data_block.num_rows() {
                    unsafe {
                        let bucket = *buckets.get_unchecked(index) as isize;
                        let location = locations.value_unchecked(index).to_string();
                        let row_group_bytes = row_groups.index_unchecked(index);
                        let row_group = deserialize_row_group_meta_from_bytes(row_group_bytes)?;

                        spilled_payloads.push(NewSpilledPayload {
                            bucket,
                            location,
                            row_group,
                        });
                    }
                }

                let shuffle_bucket = meta.shuffle_bucket;
                if shuffle_bucket == -1 {
                    let mut blocks = Vec::with_capacity(spilled_payloads.len());
                    for payload in spilled_payloads {
                        let meta = AggregateMeta::create_new_bucket_spilled(payload);
                        blocks.push(DataBlock::empty_with_meta(meta));
                    }
                    return Ok(blocks);
                } else {
                    let dispatch_blocks = AggregateMeta::create_new_spilled_blocks(
                        shuffle_bucket as usize,
                        spilled_payloads,
                    );

                    return Ok(vec![DataBlock::empty_with_meta(
                        ExchangeShuffleMeta::create(dispatch_blocks),
                    )]);
                }
            }
            other => Err(ErrorCode::Internal(format!(
                "Unknown aggregate serde meta type {other}"
            ))),
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

            if let Some(agg_meta) = AggregateMeta::downcast_ref_from(block_meta_ref) {
                if matches!(agg_meta, AggregateMeta::NewSpilled(_)) {
                    let block_meta = data_block.take_meta().unwrap();
                    if let Some(AggregateMeta::NewSpilled(payloads)) =
                        AggregateMeta::downcast_from(block_meta)
                    {
                        let mut blocks = Vec::with_capacity(payloads.len());
                        for payload in payloads {
                            let meta = AggregateMeta::create_new_bucket_spilled(payload);
                            blocks.push(DataBlock::empty_with_meta(meta));
                        }
                        return Ok(blocks);
                    }
                }
            }

            let block_meta = data_block.take_meta().unwrap();
            return Ok(vec![data_block.add_meta(Some(block_meta))?]);
        }

        Ok(vec![data_block])
    }
}

pub type TransformAggregateDeserializer = TransformDeserializer;
