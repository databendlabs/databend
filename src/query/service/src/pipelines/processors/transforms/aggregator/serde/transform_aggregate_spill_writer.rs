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
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::base::GlobalUniqName;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::arrow::serialize_column;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::PartitionedPayload;
use databend_common_hashtable::HashtableLike;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use futures_util::future::BoxFuture;
use log::info;
use opendal::Operator;

use crate::pipelines::processors::transforms::aggregator::serialize_aggregate;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::BucketSpilledPayload;
use crate::pipelines::processors::transforms::aggregator::HashTablePayload;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::PartitionedHashMethod;
use crate::sessions::QueryContext;

pub struct TransformAggregateSpillWriter<Method: HashMethodBounds> {
    ctx: Arc<QueryContext>,
    method: Method,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    params: Arc<AggregatorParams>,

    operator: Operator,
    location_prefix: String,
    spilled_block: Option<DataBlock>,
    spilling_meta: Option<AggregateMeta<Method, usize>>,
    spilling_future: Option<BoxFuture<'static, Result<DataBlock>>>,
}

impl<Method: HashMethodBounds> TransformAggregateSpillWriter<Method> {
    pub fn create(
        ctx: Arc<QueryContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        method: Method,
        operator: Operator,
        params: Arc<AggregatorParams>,
        location_prefix: String,
    ) -> Box<dyn Processor> {
        Box::new(TransformAggregateSpillWriter::<Method> {
            ctx,
            method,
            input,
            output,
            params,
            operator,
            location_prefix,
            spilled_block: None,
            spilling_meta: None,
            spilling_future: None,
        })
    }
}

#[async_trait::async_trait]
impl<Method: HashMethodBounds> Processor for TransformAggregateSpillWriter<Method> {
    fn name(&self) -> String {
        String::from("TransformAggregateSpillWriter")
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

        while let Some(spilled_block) = self.spilled_block.take() {
            if !spilled_block.is_empty() || spilled_block.get_meta().is_some() {
                self.output.push_data(Ok(spilled_block));
                return Ok(Event::NeedConsume);
            }
        }

        if self.spilling_meta.is_some() {
            self.input.set_not_need_data();
            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;

            if let Some(block_meta) = data_block
                .get_meta()
                .and_then(AggregateMeta::<Method, usize>::downcast_ref_from)
            {
                if matches!(block_meta, AggregateMeta::Spilling(_)) {
                    self.input.set_not_need_data();
                    let block_meta = data_block.take_meta().unwrap();
                    self.spilling_meta = AggregateMeta::<Method, usize>::downcast_from(block_meta);
                    return Ok(Event::Sync);
                }

                if matches!(block_meta, AggregateMeta::AggregateSpilling(_)) {
                    self.input.set_not_need_data();
                    let block_meta = data_block.take_meta().unwrap();
                    self.spilling_meta = AggregateMeta::<Method, usize>::downcast_from(block_meta);
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
            match spilling_meta {
                AggregateMeta::Spilling(payload) => {
                    self.spilling_future = Some(spilling_aggregate_payload(
                        self.ctx.clone(),
                        self.operator.clone(),
                        &self.method,
                        &self.location_prefix,
                        &self.params,
                        payload,
                    )?);

                    return Ok(());
                }
                AggregateMeta::AggregateSpilling(payload) => {
                    self.spilling_future = Some(agg_spilling_aggregate_payload::<Method>(
                        self.ctx.clone(),
                        self.operator.clone(),
                        &self.location_prefix,
                        payload,
                    )?);

                    return Ok(());
                }

                _ => {
                    return Err(ErrorCode::Internal(""));
                }
            }
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let Some(spilling_future) = self.spilling_future.take() {
            self.spilled_block = Some(spilling_future.await?);
        }

        Ok(())
    }
}

pub fn agg_spilling_aggregate_payload<Method: HashMethodBounds>(
    ctx: Arc<QueryContext>,
    operator: Operator,
    location_prefix: &str,
    partitioned_payload: PartitionedPayload,
) -> Result<BoxFuture<'static, Result<DataBlock>>> {
    let unique_name = GlobalUniqName::unique();
    let location = format!("{}/{}", location_prefix, unique_name);

    let mut write_size = 0;
    let partition_count = partitioned_payload.partition_count();
    let mut write_data = Vec::with_capacity(partition_count);
    let mut spilled_buckets_payloads = Vec::with_capacity(partition_count);
    // Record how many rows are spilled.
    let mut rows = 0;
    for (bucket, payload) in partitioned_payload.payloads.into_iter().enumerate() {
        if payload.len() == 0 {
            continue;
        }

        let data_block = payload.aggregate_flush_all()?;
        rows += data_block.num_rows();

        let begin = write_size;
        let columns = data_block.columns().to_vec();
        let mut columns_data = Vec::with_capacity(columns.len());
        let mut columns_layout = Vec::with_capacity(columns.len());
        for column in columns.into_iter() {
            let column = column.value.as_column().unwrap();
            let column_data = serialize_column(column);
            write_size += column_data.len() as u64;
            columns_layout.push(column_data.len() as u64);
            columns_data.push(column_data);
        }

        write_data.push(columns_data);
        spilled_buckets_payloads.push(BucketSpilledPayload {
            bucket: bucket as isize,
            location: location.clone(),
            data_range: begin..write_size,
            columns_layout,
            max_partition_count: partition_count,
        });
    }

    Ok(Box::pin(async move {
        let instant = Instant::now();

        let mut write_bytes = 0;

        if !write_data.is_empty() {
            let mut writer = operator
                .writer_with(&location)
                .buffer(8 * 1024 * 1024)
                .await?;
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
            Profile::record_usize_profile(ProfileStatisticsName::SpillWriteCount, 1);
            Profile::record_usize_profile(ProfileStatisticsName::SpillWriteBytes, write_bytes);
            Profile::record_usize_profile(
                ProfileStatisticsName::SpillWriteTime,
                instant.elapsed().as_millis() as usize,
            );
        }

        {
            let progress_val = ProgressValues {
                rows,
                bytes: write_bytes,
            };
            ctx.get_aggregate_spill_progress().incr(&progress_val);
        }

        info!(
            "Write aggregate spill {} successfully, elapsed: {:?}",
            location,
            instant.elapsed()
        );

        Ok(DataBlock::empty_with_meta(
            AggregateMeta::<Method, usize>::create_spilled(spilled_buckets_payloads),
        ))
    }))
}

pub fn spilling_aggregate_payload<Method: HashMethodBounds>(
    ctx: Arc<QueryContext>,
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
    let mut spilled_buckets_payloads = Vec::with_capacity(256);
    // Record how many rows are spilled.
    let mut rows = 0;
    for (bucket, inner_table) in payload.cell.hashtable.iter_tables_mut().enumerate() {
        if inner_table.len() == 0 {
            continue;
        }

        let data_block = serialize_aggregate(method, params, inner_table)?;
        rows += data_block.num_rows();

        let begin = write_size;
        let columns = data_block.columns().to_vec();
        let mut columns_data = Vec::with_capacity(columns.len());
        let mut columns_layout = Vec::with_capacity(columns.len());

        for column in columns.into_iter() {
            let column = column.value.as_column().unwrap();
            let column_data = serialize_column(column);
            write_size += column_data.len() as u64;
            columns_layout.push(column_data.len() as u64);
            columns_data.push(column_data);
        }

        write_data.push(columns_data);
        spilled_buckets_payloads.push(BucketSpilledPayload {
            bucket: bucket as isize,
            location: location.clone(),
            data_range: begin..write_size,
            columns_layout,
            max_partition_count: 0,
        });
    }

    Ok(Box::pin(async move {
        let instant = Instant::now();

        let mut write_bytes = 0;

        if !write_data.is_empty() {
            let mut writer = operator
                .writer_with(&location)
                .buffer(8 * 1024 * 1024)
                .await?;
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
            Profile::record_usize_profile(ProfileStatisticsName::SpillWriteCount, 1);
            Profile::record_usize_profile(ProfileStatisticsName::SpillWriteBytes, write_bytes);
            Profile::record_usize_profile(
                ProfileStatisticsName::SpillWriteTime,
                instant.elapsed().as_millis() as usize,
            );
        }

        {
            let progress_val = ProgressValues {
                rows,
                bytes: write_bytes,
            };
            ctx.get_aggregate_spill_progress().incr(&progress_val);
        }

        info!(
            "Write aggregate spill {} successfully, elapsed: {:?}",
            location,
            instant.elapsed()
        );

        Ok(DataBlock::empty_with_meta(
            AggregateMeta::<Method, usize>::create_spilled(spilled_buckets_payloads),
        ))
    }))
}
