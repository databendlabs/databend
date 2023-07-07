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

use common_base::base::GlobalUniqName;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::arrow::serialize_column;
use common_expression::BlockEntry;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::Processor;
use futures_util::future::BoxFuture;
use opendal::Operator;
use tracing::info;

use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::HashTablePayload;
use crate::pipelines::processors::transforms::aggregator::serde::transform_group_by_serializer::serialize_group_by;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::PartitionedHashMethod;
use crate::pipelines::processors::transforms::metrics::metrics_inc_group_by_spill_write_bytes;
use crate::pipelines::processors::transforms::metrics::metrics_inc_group_by_spill_write_count;
use crate::pipelines::processors::transforms::metrics::metrics_inc_group_by_spill_write_milliseconds;

pub struct TransformGroupBySpillWriter<Method: HashMethodBounds> {
    method: Method,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    operator: Operator,
    location_prefix: String,
    spilled_blocks: VecDeque<DataBlock>,
    spilling_meta: Option<AggregateMeta<Method, ()>>,
    spilling_future: Option<BoxFuture<'static, Result<()>>>,
}

impl<Method: HashMethodBounds> TransformGroupBySpillWriter<Method> {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        method: Method,
        operator: Operator,
        location_prefix: String,
    ) -> Box<dyn Processor> {
        Box::new(TransformGroupBySpillWriter::<Method> {
            method,
            input,
            output,
            operator,
            location_prefix,
            spilled_blocks: VecDeque::new(),
            spilling_meta: None,
            spilling_future: None,
        })
    }
}

#[async_trait::async_trait]
impl<Method: HashMethodBounds> Processor for TransformGroupBySpillWriter<Method> {
    fn name(&self) -> String {
        String::from("TransformGroupBySpillWriter")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if self.spilling_future.is_some() {
            self.input.set_not_need_data();
            return Ok(Event::Async);
        }

        if let Some(spilled_meta) = self.spilled_blocks.pop_front() {
            self.output.push_data(Ok(spilled_meta));
            return Ok(Event::NeedConsume);
        }

        if self.spilling_meta.is_some() {
            self.input.set_not_need_data();
            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;

            if let Some(block_meta) = data_block
                .get_meta()
                .and_then(AggregateMeta::<Method, ()>::downcast_ref_from)
            {
                if matches!(block_meta, AggregateMeta::Spilling(_)) {
                    self.input.set_not_need_data();
                    let block_meta = data_block.take_meta().unwrap();
                    self.spilling_meta = AggregateMeta::<Method, ()>::downcast_from(block_meta);
                    return Ok(Event::Sync);
                }
            }

            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(spilling_meta) = self.spilling_meta.take() {
            if let AggregateMeta::Spilling(payload) = spilling_meta {
                let (spilled_blocks, spilling_future) = spilling_group_by_payload(
                    self.operator.clone(),
                    &self.method,
                    &self.location_prefix,
                    payload,
                )?;

                self.spilled_blocks = spilled_blocks;
                self.spilling_future = Some(spilling_future);

                return Ok(());
            }

            return Err(ErrorCode::Internal(
                "TransformGroupBySpillWriter only recv AggregateMeta",
            ));
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let Some(spilling_future) = self.spilling_future.take() {
            return spilling_future.await;
        }

        Ok(())
    }
}

fn get_columns(data_block: DataBlock) -> Vec<BlockEntry> {
    data_block.columns().to_vec()
}

pub fn spilling_group_by_payload<Method: HashMethodBounds>(
    operator: Operator,
    method: &Method,
    location_prefix: &str,
    payload: HashTablePayload<PartitionedHashMethod<Method>, ()>,
) -> Result<(VecDeque<DataBlock>, BoxFuture<'static, Result<()>>)> {
    let unique_name = GlobalUniqName::unique();
    let location = format!("{}/{}", location_prefix, unique_name);

    let mut write_size = 0;
    let mut write_data = Vec::with_capacity(256);
    let mut spilled_blocks = VecDeque::with_capacity(256);
    for (bucket, inner_table) in payload.cell.hashtable.into_iter_tables().enumerate() {
        let data_block = serialize_group_by(method, &inner_table)?;

        let begin = write_size;
        let columns = get_columns(data_block);
        let mut columns_data = Vec::with_capacity(columns.len());
        let mut columns_layout = Vec::with_capacity(columns.len());
        for column in columns.into_iter() {
            let column = column.value.as_column().unwrap();
            let column_data = serialize_column(column);
            write_size += column_data.len();
            columns_layout.push(column_data.len());
            columns_data.push(column_data);
        }

        write_data.push(columns_data);
        spilled_blocks.push(DataBlock::empty_with_meta(
            AggregateMeta::<Method, usize>::create_spilled(
                bucket as isize,
                location.clone(),
                begin..write_size,
                columns_layout,
            ),
        ));
    }

    Ok((
        spilled_blocks,
        Box::pin(async move {
            let instant = Instant::now();

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

            Ok(())
        }),
    ))
}
