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
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use common_exception::Result;
use common_expression::{BlockMetaInfoDowncast, DataSchemaRef};
use common_expression::DataBlock;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::Processor;
use futures_util::future::BoxFuture;
use opendal::Operator;
use tracing::info;
use common_arrow::arrow::io::flight::{default_ipc_fields, WriteOptions};
use common_arrow::arrow::io::ipc::IpcField;
use common_base::base::GlobalUniqName;
use common_expression::arrow::serialize_column;
use common_pipeline_transforms::processors::transforms::{AsyncTransform, BlockMetaTransform, BlockMetaTransformer};

use crate::api::{ExchangeShuffleMeta, serialize_block};
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::{AggregateMeta, HashTablePayload};
use crate::pipelines::processors::transforms::group_by::{HashMethodBounds, PartitionedHashMethod};
use crate::pipelines::processors::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::serde::transform_aggregate_serializer::{serialize_aggregate, SerializeAggregateStream};
use crate::pipelines::processors::transforms::aggregator::serde::transform_exchange_group_by_serializer::{FlightSerialized, FlightSerializedMeta};
use crate::pipelines::processors::transforms::metrics::{metrics_inc_aggregate_spill_data_serialize_milliseconds, metrics_inc_aggregate_spill_write_bytes, metrics_inc_aggregate_spill_write_count, metrics_inc_aggregate_spill_write_milliseconds};

pub struct TransformExchangeAggregateSerializer<Method: HashMethodBounds> {
    method: Method,
    local_pos: usize,
    options: WriteOptions,
    ipc_fields: Vec<IpcField>,

    operator: Operator,
    location_prefix: String,
    params: Arc<AggregatorParams>,
}

impl<Method: HashMethodBounds> TransformExchangeAggregateSerializer<Method> {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        method: Method,
        operator: Operator,
        location_prefix: String,
        params: Arc<AggregatorParams>,
        schema: DataSchemaRef,
        local_pos: usize,
    ) -> Box<dyn Processor> {
        let arrow_schema = schema.to_arrow();
        let ipc_fields = default_ipc_fields(&arrow_schema.fields);

        BlockMetaTransformer::create(input, output, TransformExchangeAggregateSerializer::<
            Method,
        > {
            method,
            params,
            operator,
            location_prefix,
            local_pos,
            ipc_fields,
            options: WriteOptions { compression: None },
        })
    }
}

impl<Method: HashMethodBounds> BlockMetaTransform<ExchangeShuffleMeta>
    for TransformExchangeAggregateSerializer<Method>
{
    const NAME: &'static str = "TransformScatterAggregateSpillWriter";

    fn transform(&mut self, meta: ExchangeShuffleMeta) -> Result<DataBlock> {
        let mut serialized_blocks = Vec::with_capacity(meta.blocks.len());
        for (index, mut block) in meta.blocks.into_iter().enumerate() {
            if index == self.local_pos {
                serialized_blocks.push(FlightSerialized::DataBlock(block));
                continue;
            }

            match block
                .take_meta()
                .and_then(AggregateMeta::<Method, usize>::downcast_from)
            {
                None => unreachable!(),
                Some(AggregateMeta::Spilled(_)) => unreachable!(),
                Some(AggregateMeta::Serialized(_)) => unreachable!(),
                Some(AggregateMeta::Partitioned { .. }) => unreachable!(),
                Some(AggregateMeta::Spilling(payload)) => {
                    serialized_blocks.push(FlightSerialized::Future(spilling_aggregate_payload(
                        self.operator.clone(),
                        &self.method,
                        &self.location_prefix,
                        &self.params,
                        payload,
                    )?));
                }
                Some(AggregateMeta::HashTable(payload)) => {
                    let mut stream =
                        SerializeAggregateStream::create(&self.method, &self.params, payload);
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

fn spilling_aggregate_payload<Method: HashMethodBounds>(
    operator: Operator,
    method: &Method,
    location_prefix: &str,
    params: &Arc<AggregatorParams>,
    mut payload: HashTablePayload<PartitionedHashMethod<Method>, usize>,
) -> Result<BoxFuture<'static, Result<DataBlock>>> {
    let unique_name = GlobalUniqName::unique();
    let location = format!("{}/{}", location_prefix, unique_name);

    let mut write_size = 0;
    let mut write_data = Vec::with_capacity(256);
    let mut spilled_blocks = VecDeque::with_capacity(256);
    for (bucket, inner_table) in payload.cell.hashtable.iter_tables_mut().enumerate() {
        if inner_table.len() == 0 {
            spilled_blocks.push_back(DataBlock::empty());
            continue;
        }

        let now = Instant::now();
        let data_block = serialize_aggregate(method, params, inner_table)?;

        let begin = write_size;
        let columns = get_columns(data_block);
        let mut columns_data = Vec::with_capacity(columns.len());
        let mut columns_layout = Vec::with_capacity(columns.len());

        for column in columns.into_iter() {
            let column = column.value.as_column().unwrap();
            let column_data = serialize_column(column);
            write_size += column_data.len() as u64;
            columns_layout.push(column_data.len());
            columns_data.push(column_data);
        }

        // perf
        {
            metrics_inc_aggregate_spill_data_serialize_milliseconds(
                now.elapsed().as_millis() as u64
            );
        }

        write_data.push(columns_data);
        spilled_blocks.push_back(DataBlock::empty_with_meta(
            AggregateMeta::<Method, usize>::create_spilled(
                bucket as isize,
                location.clone(),
                begin..write_size,
                columns_layout,
            ),
        ));
    }

    Ok(Box::pin(async move {
        let instant = Instant::now();

        let mut write_bytes = 0;

        if !write_data.is_empty() {
            let mut writer = operator.writer(&location).await?;
            for write_bucket_data in write_data.into_iter() {
                for data in write_bucket_data.into_iter() {
                    write_bytes += data.len();
                    writer.write(data).await?;
                }
            }

            writer.close().await?;
        }

        // perf
        {
            metrics_inc_aggregate_spill_write_count();
            metrics_inc_aggregate_spill_write_bytes(write_bytes as u64);
            metrics_inc_aggregate_spill_write_milliseconds(instant.elapsed().as_millis() as u64);
        }

        info!(
            "Write aggregate spill {} successfully, elapsed: {:?}",
            location,
            instant.elapsed()
        );

        Ok(())
    }))
}
