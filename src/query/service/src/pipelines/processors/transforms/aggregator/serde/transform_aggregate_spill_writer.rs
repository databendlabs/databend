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
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use futures_util::future::BoxFuture;
use log::info;
use opendal::Operator;

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::BucketSpilledPayload;
use crate::sessions::QueryContext;
use crate::spillers::Spiller;
use crate::spillers::SpillerConfig;
use crate::spillers::SpillerType;

pub struct TransformAggregateSpillWriter {
    ctx: Arc<QueryContext>,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    _params: Arc<AggregatorParams>,

    spiller: Arc<Spiller>,
    spilled_block: Option<DataBlock>,
    spilling_meta: Option<AggregateMeta>,
    spilling_future: Option<BoxFuture<'static, Result<DataBlock>>>,
}

impl TransformAggregateSpillWriter {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        operator: Operator,
        params: Arc<AggregatorParams>,
        location_prefix: String,
    ) -> Result<Box<dyn Processor>> {
        let config = SpillerConfig {
            spiller_type: SpillerType::Aggregation,
            location_prefix,
            disk_spill: None,
            use_parquet: ctx.get_settings().get_spilling_file_format()?.is_parquet(),
        };

        let spiller = Spiller::create(ctx.clone(), operator, config.clone())?;
        Ok(Box::new(TransformAggregateSpillWriter {
            ctx,
            input,
            output,
            _params: params,
            spiller: Arc::new(spiller),
            spilled_block: None,
            spilling_meta: None,
            spilling_future: None,
        }))
    }
}

#[async_trait::async_trait]
impl Processor for TransformAggregateSpillWriter {
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
                .and_then(AggregateMeta::downcast_ref_from)
            {
                if matches!(block_meta, AggregateMeta::AggregateSpilling(_)) {
                    self.input.set_not_need_data();
                    let block_meta = data_block.take_meta().unwrap();
                    self.spilling_meta = AggregateMeta::downcast_from(block_meta);
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
                AggregateMeta::AggregateSpilling(payload) => {
                    self.spilling_future = Some(agg_spilling_aggregate_payload(
                        self.ctx.clone(),
                        self.spiller.clone(),
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

pub fn agg_spilling_aggregate_payload(
    ctx: Arc<QueryContext>,
    spiller: Arc<Spiller>,
    partitioned_payload: PartitionedPayload,
) -> Result<BoxFuture<'static, Result<DataBlock>>> {
    let mut write_size = 0;
    let partition_count = partitioned_payload.partition_count();
    let mut write_data = Vec::with_capacity(partition_count);
    let mut spilled_buckets_payloads = Vec::with_capacity(partition_count);
    // Record how many rows are spilled.
    let mut rows = 0;
    let mut buckets_count = 0;
    let location = spiller.create_unique_location();
    for (bucket, payload) in partitioned_payload.payloads.into_iter().enumerate() {
        if payload.len() == 0 {
            continue;
        }

        let data_block = payload.aggregate_flush_all()?.consume_convert_to_full();
        rows += data_block.num_rows();
        buckets_count += 1;

        let begin = write_size;
        let mut columns_data = Vec::with_capacity(data_block.num_columns());
        let mut columns_layout = Vec::with_capacity(data_block.num_columns());
        for entry in data_block.columns() {
            let column_data = serialize_column(entry.as_column().unwrap());
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
        if !write_data.is_empty() {
            let (location, write_bytes) = spiller
                .spill_stream_aggregate_buffer(Some(location), write_data)
                .await?;
            let elapsed = instant.elapsed();
            // perf
            {
                Profile::record_usize_profile(ProfileStatisticsName::RemoteSpillWriteCount, 1);
                Profile::record_usize_profile(
                    ProfileStatisticsName::RemoteSpillWriteBytes,
                    write_bytes,
                );
                Profile::record_usize_profile(
                    ProfileStatisticsName::RemoteSpillWriteTime,
                    elapsed.as_millis() as usize,
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
                "Write aggregate spill finished(local): (location: {}, bytes: {}, rows: {}, buckets_count: {}, elapsed: {:?})",
                location, write_bytes, rows, buckets_count, elapsed
            );
        }

        Ok(DataBlock::empty_with_meta(AggregateMeta::create_spilled(
            spilled_buckets_payloads,
        )))
    }))
}
