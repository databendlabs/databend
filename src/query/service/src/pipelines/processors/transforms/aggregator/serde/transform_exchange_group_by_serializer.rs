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

use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::time::Instant;

use common_arrow::arrow::io::flight::default_ipc_fields;
use common_arrow::arrow::io::flight::WriteOptions;
use common_arrow::arrow::io::ipc::IpcField;
use common_base::base::GlobalUniqName;
use common_exception::Result;
use common_expression::arrow::serialize_column;
use common_expression::types::ArgType;
use common_expression::types::ArrayType;
use common_expression::types::Int64Type;
use common_expression::types::UInt64Type;
use common_expression::types::ValueType;
use common_expression::BlockEntry;
use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoDowncast;
use common_expression::BlockMetaInfoPtr;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::FromData;
use common_hashtable::HashtableLike;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::Processor;
use common_pipeline_transforms::processors::transforms::BlockMetaTransform;
use common_pipeline_transforms::processors::transforms::BlockMetaTransformer;
use common_pipeline_transforms::processors::transforms::UnknownMode;
use futures_util::future::BoxFuture;
use log::info;
use opendal::Operator;

use crate::api::serialize_block;
use crate::api::ExchangeShuffleMeta;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::HashTablePayload;
use crate::pipelines::processors::transforms::aggregator::serde::exchange_defines;
use crate::pipelines::processors::transforms::aggregator::serde::transform_group_by_serializer::serialize_group_by;
use crate::pipelines::processors::transforms::aggregator::serde::transform_group_by_serializer::SerializeGroupByStream;
use crate::pipelines::processors::transforms::aggregator::serde::transform_group_by_spill_writer::spilling_group_by_payload as local_spilling_group_by_payload;
use crate::pipelines::processors::transforms::aggregator::serde::AggregateSerdeMeta;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::PartitionedHashMethod;
use crate::pipelines::processors::transforms::metrics::metrics_inc_aggregate_spill_data_serialize_milliseconds;
use crate::pipelines::processors::transforms::metrics::metrics_inc_group_by_spill_write_bytes;
use crate::pipelines::processors::transforms::metrics::metrics_inc_group_by_spill_write_count;
use crate::pipelines::processors::transforms::metrics::metrics_inc_group_by_spill_write_milliseconds;

pub struct TransformExchangeGroupBySerializer<Method: HashMethodBounds> {
    method: Method,
    local_pos: usize,
    options: WriteOptions,
    ipc_fields: Vec<IpcField>,

    operator: Operator,
    location_prefix: String,
}

impl<Method: HashMethodBounds> TransformExchangeGroupBySerializer<Method> {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        method: Method,
        operator: Operator,
        location_prefix: String,
        schema: DataSchemaRef,
        local_pos: usize,
    ) -> Box<dyn Processor> {
        let arrow_schema = schema.to_arrow();
        let ipc_fields = default_ipc_fields(&arrow_schema.fields);

        BlockMetaTransformer::create(
            input,
            output,
            TransformExchangeGroupBySerializer::<Method> {
                method,
                operator,
                local_pos,
                ipc_fields,
                location_prefix,
                options: WriteOptions { compression: None },
            },
        )
    }
}

pub enum FlightSerialized {
    DataBlock(DataBlock),
    Future(BoxFuture<'static, Result<DataBlock>>),
}

unsafe impl Sync for FlightSerialized {}

pub struct FlightSerializedMeta {
    pub serialized_blocks: Vec<FlightSerialized>,
}

impl FlightSerializedMeta {
    pub fn create(blocks: Vec<FlightSerialized>) -> BlockMetaInfoPtr {
        Box::new(FlightSerializedMeta {
            serialized_blocks: blocks,
        })
    }
}

impl Debug for FlightSerializedMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlightSerializedMeta").finish()
    }
}

impl serde::Serialize for FlightSerializedMeta {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        unimplemented!("Unimplemented serialize FlightSerializedMeta")
    }
}

impl<'de> serde::Deserialize<'de> for FlightSerializedMeta {
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        unimplemented!("Unimplemented deserialize FlightSerializedMeta")
    }
}

#[typetag::serde(name = "exchange_shuffle")]
impl BlockMetaInfo for FlightSerializedMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals FlightSerializedMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone FlightSerializedMeta")
    }
}

impl<Method: HashMethodBounds> BlockMetaTransform<ExchangeShuffleMeta>
    for TransformExchangeGroupBySerializer<Method>
{
    const UNKNOWN_MODE: UnknownMode = UnknownMode::Error;
    const NAME: &'static str = "TransformExchangeGroupBySerializer";

    fn transform(&mut self, meta: ExchangeShuffleMeta) -> Result<DataBlock> {
        let mut serialized_blocks = Vec::with_capacity(meta.blocks.len());
        for (index, mut block) in meta.blocks.into_iter().enumerate() {
            if block.is_empty() && block.get_meta().is_none() {
                serialized_blocks.push(FlightSerialized::DataBlock(block));
                continue;
            }

            match AggregateMeta::<Method, ()>::downcast_from(block.take_meta().unwrap()) {
                None => unreachable!(),
                Some(AggregateMeta::Spilled(_)) => unreachable!(),
                Some(AggregateMeta::BucketSpilled(_)) => unreachable!(),
                Some(AggregateMeta::Serialized(_)) => unreachable!(),
                Some(AggregateMeta::Partitioned { .. }) => unreachable!(),
                Some(AggregateMeta::Spilling(payload)) => {
                    serialized_blocks.push(FlightSerialized::Future(
                        match index == self.local_pos {
                            true => local_spilling_group_by_payload(
                                self.operator.clone(),
                                &self.method,
                                &self.location_prefix,
                                payload,
                            )?,
                            false => spilling_group_by_payload(
                                self.operator.clone(),
                                &self.method,
                                &self.location_prefix,
                                payload,
                            )?,
                        },
                    ));
                }
                Some(AggregateMeta::HashTable(payload)) => {
                    if index == self.local_pos {
                        serialized_blocks.push(FlightSerialized::DataBlock(block.add_meta(
                            Some(Box::new(AggregateMeta::<Method, ()>::HashTable(payload))),
                        )?));
                        continue;
                    }

                    let mut stream = SerializeGroupByStream::create(&self.method, payload);
                    let bucket = stream.payload.bucket;
                    serialized_blocks.push(FlightSerialized::DataBlock(match stream.next() {
                        None => DataBlock::empty(),
                        Some(data_block) => {
                            serialize_block(bucket, data_block?, &self.ipc_fields, &self.options)?
                        }
                    }));
                }
            };
        }

        Ok(DataBlock::empty_with_meta(FlightSerializedMeta::create(
            serialized_blocks,
        )))
    }
}

fn get_columns(data_block: DataBlock) -> Vec<BlockEntry> {
    data_block.columns().to_vec()
}

fn spilling_group_by_payload<Method: HashMethodBounds>(
    operator: Operator,
    method: &Method,
    location_prefix: &str,
    mut payload: HashTablePayload<PartitionedHashMethod<Method>, ()>,
) -> Result<BoxFuture<'static, Result<DataBlock>>> {
    let unique_name = GlobalUniqName::unique();
    let location = format!("{}/{}", location_prefix, unique_name);

    let mut write_size = 0;
    let mut write_data = Vec::with_capacity(256);
    let mut buckets_column_data = Vec::with_capacity(256);
    let mut data_range_start_column_data = Vec::with_capacity(256);
    let mut data_range_end_column_data = Vec::with_capacity(256);
    let mut columns_layout_column_data = Vec::with_capacity(256);

    for (bucket, inner_table) in payload.cell.hashtable.iter_tables_mut().enumerate() {
        if inner_table.len() == 0 {
            continue;
        }

        let now = Instant::now();
        let data_block = serialize_group_by(method, inner_table)?;

        let old_write_size = write_size;
        let columns = get_columns(data_block);
        let mut columns_data = Vec::with_capacity(columns.len());
        let mut columns_layout = Vec::with_capacity(columns.len());

        for column in columns.into_iter() {
            let column = column.value.as_column().unwrap();
            let column_data = serialize_column(column);
            write_size += column_data.len() as u64;
            columns_layout.push(column_data.len() as u64);
            columns_data.push(column_data);
        }

        // perf
        {
            metrics_inc_aggregate_spill_data_serialize_milliseconds(
                now.elapsed().as_millis() as u64
            );
        }

        write_data.push(columns_data);
        buckets_column_data.push(bucket as i64);
        data_range_end_column_data.push(write_size);
        columns_layout_column_data.push(columns_layout);
        data_range_start_column_data.push(old_write_size);
    }

    Ok(Box::pin(async move {
        let instant = Instant::now();

        if !write_data.is_empty() {
            let mut write_bytes = 0;
            let mut writer = operator.writer(&location).await?;
            for write_bucket_data in write_data.into_iter() {
                for data in write_bucket_data.into_iter() {
                    write_bytes += data.len();
                    writer.write(data).await?;
                }
            }

            writer.close().await?;

            // perf
            {
                metrics_inc_group_by_spill_write_count();
                metrics_inc_group_by_spill_write_bytes(write_bytes as u64);
                metrics_inc_group_by_spill_write_milliseconds(instant.elapsed().as_millis() as u64);
            }

            info!(
                "Write aggregate spill {} successfully, elapsed: {:?}",
                location,
                instant.elapsed()
            );

            let data_block = DataBlock::new_from_columns(vec![
                Int64Type::from_data(buckets_column_data),
                UInt64Type::from_data(data_range_start_column_data),
                UInt64Type::from_data(data_range_end_column_data),
                ArrayType::upcast_column(ArrayType::<UInt64Type>::column_from_iter(
                    columns_layout_column_data
                        .into_iter()
                        .map(|x| UInt64Type::column_from_iter(x.into_iter(), &[])),
                    &[],
                )),
            ]);

            let data_block = data_block.add_meta(Some(AggregateSerdeMeta::create_spilled(
                -1,
                location.clone(),
                0..0,
                vec![],
            )))?;

            let ipc_fields = exchange_defines::spilled_ipc_fields();
            let write_options = exchange_defines::spilled_write_options();
            return serialize_block(-1, data_block, ipc_fields, write_options);
        }

        Ok(DataBlock::empty())
    }))
}
