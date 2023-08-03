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

use std::marker::PhantomData;
use std::sync::Arc;

use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::flight::default_ipc_fields;
use common_arrow::arrow::io::flight::deserialize_batch;
use common_arrow::arrow::io::flight::deserialize_dictionary;
use common_arrow::arrow::io::ipc::read::Dictionaries;
use common_arrow::arrow::io::ipc::IpcSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::ArrayType;
use common_expression::types::NumberType;
use common_expression::types::UInt64Type;
use common_expression::types::ValueType;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_io::prelude::BinaryRead;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::BlockMetaTransform;
use common_pipeline_transforms::processors::transforms::BlockMetaTransformer;
use common_pipeline_transforms::processors::transforms::UnknownMode;

use crate::api::DataPacket;
use crate::api::ExchangeDeserializeMeta;
use crate::api::FragmentData;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::BucketSpilledPayload;
use crate::pipelines::processors::transforms::aggregator::serde::exchange_defines;
use crate::pipelines::processors::transforms::aggregator::serde::serde_meta::AggregateSerdeMeta;
use crate::pipelines::processors::transforms::aggregator::serde::BUCKET_TYPE;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;

pub struct TransformDeserializer<Method: HashMethodBounds, V: Send + Sync + 'static> {
    schema: DataSchemaRef,
    ipc_schema: IpcSchema,
    arrow_schema: Arc<ArrowSchema>,
    _phantom: PhantomData<(Method, V)>,
}

impl<Method: HashMethodBounds, V: Send + Sync + 'static> TransformDeserializer<Method, V> {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        schema: &DataSchemaRef,
    ) -> Result<ProcessorPtr> {
        let arrow_schema = Arc::new(schema.to_arrow());
        let ipc_fields = default_ipc_fields(&arrow_schema.fields);
        let ipc_schema = IpcSchema {
            fields: ipc_fields,
            is_little_endian: true,
        };

        Ok(ProcessorPtr::create(BlockMetaTransformer::create(
            input,
            output,
            TransformDeserializer::<Method, V> {
                ipc_schema,
                arrow_schema,
                schema: schema.clone(),
                _phantom: Default::default(),
            },
        )))
    }

    fn recv_data(&self, dict: Vec<DataPacket>, fragment_data: FragmentData) -> Result<DataBlock> {
        const ROW_HEADER_SIZE: usize = std::mem::size_of::<u32>();

        let meta = match bincode::deserialize(&fragment_data.get_meta()[ROW_HEADER_SIZE..]) {
            Ok(meta) => Ok(meta),
            Err(_) => Err(ErrorCode::BadBytes(
                "block meta deserialize error when exchange",
            )),
        }?;

        let mut row_count_meta = &fragment_data.get_meta()[..ROW_HEADER_SIZE];
        let row_count: u32 = row_count_meta.read_scalar()?;

        if row_count == 0 {
            return Ok(DataBlock::new_with_meta(vec![], 0, meta));
        }

        let fields = &self.arrow_schema.fields;
        let schema = &self.ipc_schema;

        let data_block = match &meta {
            None => {
                self.deserialize_data_block(dict, &fragment_data, fields, schema, &self.schema)?
            }
            Some(meta) => match AggregateSerdeMeta::downcast_ref_from(meta) {
                None => {
                    self.deserialize_data_block(dict, &fragment_data, fields, schema, &self.schema)?
                }
                Some(meta) => {
                    return match meta.typ == BUCKET_TYPE {
                        true => Ok(DataBlock::empty_with_meta(
                            AggregateMeta::<Method, V>::create_serialized(
                                meta.bucket,
                                self.deserialize_data_block(
                                    dict,
                                    &fragment_data,
                                    fields,
                                    schema,
                                    &self.schema,
                                )?,
                            ),
                        )),
                        false => {
                            let fields = exchange_defines::spilled_fields();
                            let schema = exchange_defines::spilled_ipc_schema();
                            let data_schema = Arc::new(exchange_defines::spilled_schema());
                            let data_block = self.deserialize_data_block(
                                dict,
                                &fragment_data,
                                fields,
                                schema,
                                &data_schema,
                            )?;

                            let columns = data_block
                                .columns()
                                .iter()
                                .map(|c| c.value.clone().into_column())
                                .try_collect::<Vec<_>>()
                                .unwrap();

                            let buckets =
                                NumberType::<i64>::try_downcast_column(&columns[0]).unwrap();
                            let data_range_start =
                                NumberType::<u64>::try_downcast_column(&columns[1]).unwrap();
                            let data_range_end =
                                NumberType::<u64>::try_downcast_column(&columns[2]).unwrap();
                            let columns_layout =
                                ArrayType::<UInt64Type>::try_downcast_column(&columns[3]).unwrap();

                            let columns_layout_data = columns_layout.values.as_slice();

                            let mut buckets_payload = Vec::with_capacity(data_block.num_rows());
                            for index in 0..data_block.num_rows() {
                                unsafe {
                                    buckets_payload.push(BucketSpilledPayload {
                                        bucket: *buckets.get_unchecked(index) as isize,
                                        location: meta.location.clone().unwrap(),
                                        data_range: *data_range_start.get_unchecked(index)
                                            ..*data_range_end.get_unchecked(index),
                                        columns_layout: columns_layout_data[columns_layout.offsets
                                            [index]
                                            as usize
                                            ..columns_layout.offsets[index + 1] as usize]
                                            .to_vec(),
                                    });
                                }
                            }

                            Ok(DataBlock::empty_with_meta(
                                AggregateMeta::<Method, V>::create_spilled(buckets_payload),
                            ))
                        }
                    };
                }
            },
        };

        match data_block.num_columns() == 0 {
            true => Ok(DataBlock::new_with_meta(vec![], row_count as usize, meta)),
            false => data_block.add_meta(meta),
        }
    }

    fn deserialize_data_block(
        &self,
        dict: Vec<DataPacket>,
        fragment_data: &FragmentData,
        arrow_fields: &[Field],
        ipc_schema: &IpcSchema,
        data_schema: &DataSchemaRef,
    ) -> Result<DataBlock> {
        let mut dictionaries = Dictionaries::new();

        for dict_packet in dict {
            if let DataPacket::Dictionary(flight_data) = dict_packet {
                deserialize_dictionary(&flight_data, arrow_fields, ipc_schema, &mut dictionaries)?;
            }
        }

        let batch =
            deserialize_batch(&fragment_data.data, arrow_fields, ipc_schema, &dictionaries)?;

        DataBlock::from_arrow_chunk(&batch, data_schema)
    }
}

impl<M, V> BlockMetaTransform<ExchangeDeserializeMeta> for TransformDeserializer<M, V>
where
    M: HashMethodBounds,
    V: Send + Sync + 'static,
{
    const UNKNOWN_MODE: UnknownMode = UnknownMode::Pass;
    const NAME: &'static str = "TransformDeserializer";

    fn transform(&mut self, mut meta: ExchangeDeserializeMeta) -> Result<DataBlock> {
        match meta.packet.pop().unwrap() {
            DataPacket::ErrorCode(v) => Err(v),
            DataPacket::Dictionary(_) => unreachable!(),
            DataPacket::FetchProgress => unreachable!(),
            DataPacket::SerializeProgress { .. } => unreachable!(),
            DataPacket::FragmentData(v) => self.recv_data(meta.packet, v),
        }
    }
}

pub type TransformGroupByDeserializer<Method> = TransformDeserializer<Method, ()>;
pub type TransformAggregateDeserializer<Method> = TransformDeserializer<Method, usize>;
