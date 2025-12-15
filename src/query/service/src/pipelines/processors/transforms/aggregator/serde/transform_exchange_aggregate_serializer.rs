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
use std::time::Instant;

use arrow_ipc::CompressionType;
use arrow_ipc::writer::IpcWriteOptions;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::PartitionedPayload;
use databend_common_expression::arrow::serialize_column;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::ReturnType;
use databend_common_expression::types::UInt64Type;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline_transforms::UnknownMode;
use databend_common_pipeline_transforms::processors::BlockMetaTransform;
use databend_common_pipeline_transforms::processors::BlockMetaTransformer;
use databend_common_settings::FlightCompression;
use futures_util::future::BoxFuture;
use log::info;
use opendal::Operator;

use super::SerializePayload;
use crate::pipelines::processors::transforms::aggregator::AggregateSerdeMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::FlightSerialized;
use crate::pipelines::processors::transforms::aggregator::FlightSerializedMeta;
use crate::pipelines::processors::transforms::aggregator::SerializeAggregateStream;
use crate::pipelines::processors::transforms::aggregator::agg_spilling_aggregate_payload as local_agg_spilling_aggregate_payload;
use crate::pipelines::processors::transforms::aggregator::aggregate_exchange_injector::compute_block_number;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::exchange_defines;
use crate::servers::flight::v1::exchange::ExchangeShuffleMeta;
use crate::servers::flight::v1::exchange::serde::serialize_block;
use crate::sessions::QueryContext;
use crate::spillers::Spiller;
use crate::spillers::SpillerConfig;
use crate::spillers::SpillerType;

pub struct TransformExchangeAggregateSerializer {
    ctx: Arc<QueryContext>,
    local_pos: usize,
    options: IpcWriteOptions,

    params: Arc<AggregatorParams>,
    spiller: Arc<Spiller>,
}

impl TransformExchangeAggregateSerializer {
    #[allow(clippy::too_many_arguments)]
    pub fn try_create(
        ctx: Arc<QueryContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,

        operator: Operator,
        location_prefix: String,
        params: Arc<AggregatorParams>,
        compression: Option<FlightCompression>,
        local_pos: usize,
    ) -> Result<Box<dyn Processor>> {
        let compression = match compression {
            None => None,
            Some(compression) => match compression {
                FlightCompression::Lz4 => Some(CompressionType::LZ4_FRAME),
                FlightCompression::Zstd => Some(CompressionType::ZSTD),
            },
        };
        let config = SpillerConfig {
            spiller_type: SpillerType::Aggregation,
            location_prefix,
            disk_spill: None,
            use_parquet: ctx.get_settings().get_spilling_file_format()?.is_parquet(),
        };

        let spiller = Spiller::create(ctx.clone(), operator, config.clone())?;
        Ok(BlockMetaTransformer::create(
            input,
            output,
            TransformExchangeAggregateSerializer {
                ctx,
                params,
                local_pos,
                spiller: spiller.into(),
                options: IpcWriteOptions::default()
                    .try_with_compression(compression)
                    .unwrap(),
            },
        ))
    }
}

impl BlockMetaTransform<ExchangeShuffleMeta> for TransformExchangeAggregateSerializer {
    const UNKNOWN_MODE: UnknownMode = UnknownMode::Pass;
    const NAME: &'static str = "TransformExchangeAggregateSerializer";

    fn transform(&mut self, meta: ExchangeShuffleMeta) -> Result<Vec<DataBlock>> {
        let mut serialized_blocks = Vec::with_capacity(meta.blocks.len());
        for (index, mut block) in meta.blocks.into_iter().enumerate() {
            if block.is_empty() && block.get_meta().is_none() {
                serialized_blocks.push(FlightSerialized::DataBlock(block));
                continue;
            }

            match block.take_meta().and_then(AggregateMeta::downcast_from) {
                Some(AggregateMeta::AggregateSpilling(payload)) => {
                    serialized_blocks.push(FlightSerialized::Future(
                        match index == self.local_pos {
                            true => local_agg_spilling_aggregate_payload(
                                self.ctx.clone(),
                                self.spiller.clone(),
                                payload,
                            )?,
                            false => exchange_agg_spilling_aggregate_payload(
                                self.ctx.clone(),
                                self.spiller.clone(),
                                payload,
                            )?,
                        },
                    ));
                }

                Some(AggregateMeta::AggregatePayload(p)) => {
                    let (bucket, max_partition_count) = (p.bucket, p.max_partition_count);

                    if index == self.local_pos {
                        serialized_blocks.push(FlightSerialized::DataBlock(
                            block.add_meta(Some(Box::new(AggregateMeta::AggregatePayload(p))))?,
                        ));
                        continue;
                    }

                    let block_number = compute_block_number(bucket, max_partition_count)?;
                    let stream = SerializeAggregateStream::create(
                        &self.params,
                        SerializePayload::AggregatePayload(p),
                    );
                    let mut stream_blocks = stream.into_iter().collect::<Result<Vec<_>>>()?;
                    debug_assert!(!stream_blocks.is_empty());
                    let mut c = DataBlock::concat(&stream_blocks)?;
                    if let Some(meta) = stream_blocks[0].take_meta() {
                        c.replace_meta(meta);
                    }
                    let c = serialize_block(block_number, c, &self.options)?;
                    serialized_blocks.push(FlightSerialized::DataBlock(c));
                }

                _ => unreachable!(),
            };
        }

        Ok(vec![DataBlock::empty_with_meta(
            FlightSerializedMeta::create(serialized_blocks),
        )])
    }
}

fn exchange_agg_spilling_aggregate_payload(
    ctx: Arc<QueryContext>,
    spiller: Arc<Spiller>,
    partitioned_payload: PartitionedPayload,
) -> Result<BoxFuture<'static, Result<DataBlock>>> {
    let partition_count = partitioned_payload.partition_count();
    let mut write_size = 0;
    let mut buckets_count = 0;
    let mut write_data = Vec::with_capacity(partition_count);
    let mut buckets_column_data = Vec::with_capacity(partition_count);
    let mut data_range_start_column_data = Vec::with_capacity(partition_count);
    let mut data_range_end_column_data = Vec::with_capacity(partition_count);
    let mut columns_layout_column_data = Vec::with_capacity(partition_count);
    // Record how many rows are spilled.
    let mut rows = 0;

    for (bucket, payload) in partitioned_payload.payloads.into_iter().enumerate() {
        if payload.len() == 0 {
            continue;
        }

        let data_block = payload.aggregate_flush_all()?;
        rows += data_block.num_rows();
        buckets_count += 1;

        let old_write_size = write_size;
        let columns = data_block.columns().to_vec();
        let mut columns_data = Vec::with_capacity(columns.len());
        let mut columns_layout = Vec::with_capacity(columns.len());

        for entry in columns.into_iter() {
            let column = entry.to_column();
            let column_data = serialize_column(&column);
            write_size += column_data.len() as u64;
            columns_layout.push(column_data.len() as u64);
            columns_data.push(column_data);
        }

        write_data.push(columns_data);
        buckets_column_data.push(bucket as i64);
        data_range_end_column_data.push(write_size);
        columns_layout_column_data.push(columns_layout);
        data_range_start_column_data.push(old_write_size);
    }

    Ok(Box::pin(async move {
        if !write_data.is_empty() {
            let instant = Instant::now();
            let (location, write_bytes) = spiller
                .spill_stream_aggregate_buffer(None, write_data)
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
                {
                    let progress_val = ProgressValues {
                        rows,
                        bytes: write_bytes,
                    };
                    ctx.get_aggregate_spill_progress().incr(&progress_val);
                }
            }

            info!(
                "Write aggregate spill finished(exchange): (location: {}, bytes: {}, rows: {}, buckets_count: {}, elapsed: {:?})",
                location, write_bytes, rows, buckets_count, elapsed
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

            let data_block = data_block.add_meta(Some(AggregateSerdeMeta::create_agg_spilled(
                -1,
                location.clone(),
                0..0,
                vec![],
                partition_count,
            )))?;

            let write_options = exchange_defines::spilled_write_options();
            return serialize_block(-1, data_block, &write_options);
        }

        Ok(DataBlock::empty())
    }))
}
