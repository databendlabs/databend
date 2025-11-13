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

use arrow_ipc::writer::IpcWriteOptions;
use arrow_ipc::CompressionType;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::arrow::serialize_column;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::ReturnType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::PartitionedPayload;
use databend_common_expression::MAX_AGGREGATE_HASHTABLE_BUCKETS_NUM;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline_transforms::AccumulatingTransform;
use databend_common_pipeline_transforms::AccumulatingTransformer;
use databend_common_settings::FlightCompression;
use databend_common_storages_parquet::serialize_row_group_meta_to_bytes;
use futures_util::future::BoxFuture;
use log::info;
use opendal::Operator;

use super::SerializePayload;
use crate::pipelines::processors::transforms::aggregator::agg_spilling_aggregate_payload as local_agg_spilling_aggregate_payload;
use crate::pipelines::processors::transforms::aggregator::aggregate_exchange_injector::compute_block_number;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::exchange_defines;
use crate::pipelines::processors::transforms::aggregator::new_aggregate::NewAggregateSpiller;
use crate::pipelines::processors::transforms::aggregator::AggregateSerdeMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::FlightSerialized;
use crate::pipelines::processors::transforms::aggregator::FlightSerializedMeta;
use crate::pipelines::processors::transforms::aggregator::SerializeAggregateStream;
use crate::pipelines::processors::transforms::aggregator::SharedPartitionStream;
use crate::servers::flight::v1::exchange::serde::serialize_block;
use crate::servers::flight::v1::exchange::ExchangeShuffleMeta;
use crate::sessions::QueryContext;
use crate::spillers::Spiller;
use crate::spillers::SpillerConfig;
use crate::spillers::SpillerType;

enum SpillerVer {
    Old(Arc<Spiller>),
    New(Vec<NewAggregateSpiller>),
}

pub struct TransformExchangeAggregateSerializer {
    ctx: Arc<QueryContext>,
    local_pos: usize,
    options: IpcWriteOptions,

    params: Arc<AggregatorParams>,
    spiller: SpillerVer,

    finished: bool,
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
        partition_streams: Vec<SharedPartitionStream>,
    ) -> Result<Box<dyn Processor>> {
        let compression = match compression {
            None => None,
            Some(compression) => match compression {
                FlightCompression::Lz4 => Some(CompressionType::LZ4_FRAME),
                FlightCompression::Zstd => Some(CompressionType::ZSTD),
            },
        };

        let spiller = if params.enable_experiment_aggregate {
            let spillers = partition_streams
                .into_iter()
                .map(|stream| {
                    NewAggregateSpiller::try_create(
                        ctx.clone(),
                        MAX_AGGREGATE_HASHTABLE_BUCKETS_NUM as usize,
                        stream.clone(),
                    )
                })
                .collect::<Result<Vec<NewAggregateSpiller>>>()?;
            SpillerVer::New(spillers)
        } else {
            let config = SpillerConfig {
                spiller_type: SpillerType::Aggregation,
                location_prefix,
                disk_spill: None,
                use_parquet: ctx.get_settings().get_spilling_file_format()?.is_parquet(),
            };

            let spiller = Spiller::create(ctx.clone(), operator, config.clone())?;
            SpillerVer::Old(Arc::new(spiller))
        };

        Ok(AccumulatingTransformer::create(
            input,
            output,
            TransformExchangeAggregateSerializer {
                ctx,
                params,
                local_pos,
                spiller,
                options: IpcWriteOptions::default()
                    .try_with_compression(compression)
                    .unwrap(),
                finished: false,
            },
        ))
    }

    fn finish(&mut self) -> Result<Vec<FlightSerialized>> {
        if self.finished {
            return Ok(vec![]);
        }
        self.finished = true;

        if let SpillerVer::New(spillers) = &mut self.spiller {
            let mut serialized_blocks = vec![];
            let write_options = exchange_defines::spilled_write_options();

            for (index, spiller) in spillers.iter_mut().enumerate() {
                if index == self.local_pos {
                    serialized_blocks.push(Self::finish_local_new_spiller(spiller)?);
                } else {
                    serialized_blocks
                        .push(Self::finish_exchange_new_spiller(spiller, &write_options)?);
                }
            }

            return Ok(serialized_blocks);
        }

        Err(ErrorCode::Internal(
            "Spiller version is not NewAggregateSpiller",
        ))
    }

    fn finish_local_new_spiller(spiller: &mut NewAggregateSpiller) -> Result<FlightSerialized> {
        let spilled_payloads = spiller.spill_finish()?;
        let block = if spilled_payloads.is_empty() {
            DataBlock::empty()
        } else {
            DataBlock::empty_with_meta(AggregateMeta::create_new_spilled(spilled_payloads))
        };
        Ok(FlightSerialized::DataBlock(block))
    }

    fn finish_exchange_new_spiller(
        spiller: &mut NewAggregateSpiller,
        write_options: &IpcWriteOptions,
    ) -> Result<FlightSerialized> {
        let spilled_payloads = spiller.spill_finish()?;
        if spilled_payloads.is_empty() {
            return Ok(FlightSerialized::DataBlock(serialize_block(
                -1,
                DataBlock::empty(),
                write_options,
            )?));
        }

        let mut bucket_column = Vec::with_capacity(spilled_payloads.len());
        let mut row_group_column = Vec::with_capacity(spilled_payloads.len());
        let mut location_column = Vec::with_capacity(spilled_payloads.len());
        for payload in spilled_payloads {
            bucket_column.push(payload.bucket as i64);
            location_column.push(payload.location);
            row_group_column.push(serialize_row_group_meta_to_bytes(&payload.row_group)?);
        }

        let data_block = DataBlock::new_from_columns(vec![
            Int64Type::from_data(bucket_column),
            StringType::from_data(location_column),
            BinaryType::from_data(row_group_column),
        ]);
        let meta = AggregateSerdeMeta::create_new_spilled();
        let data_block = data_block.add_meta(Some(meta))?;
        Ok(FlightSerialized::DataBlock(serialize_block(
            -1,
            data_block,
            write_options,
        )?))
    }
}

impl AccumulatingTransform for TransformExchangeAggregateSerializer {
    const NAME: &'static str = "TransformExchangeAggregateSerializer";

    fn transform(&mut self, mut data: DataBlock) -> Result<Vec<DataBlock>> {
        if let Some(block_meta) = data.take_meta() {
            if ExchangeShuffleMeta::downcast_ref_from(&block_meta).is_some() {
                let meta = ExchangeShuffleMeta::downcast_from(block_meta).unwrap();
                let mut serialized_blocks = Vec::with_capacity(meta.blocks.len());
                let mut spilled_blocks = None;
                for (index, mut block) in meta.blocks.into_iter().enumerate() {
                    if block.is_empty() && block.get_meta().is_none() {
                        serialized_blocks.push(FlightSerialized::DataBlock(block));
                        continue;
                    }

                    match block.take_meta().and_then(AggregateMeta::downcast_from) {
                        Some(AggregateMeta::AggregateSpilling(partitioned_payload)) => {
                            match &mut self.spiller {
                                SpillerVer::Old(spiller) => {
                                    serialized_blocks.push(FlightSerialized::Future(
                                        match index == self.local_pos {
                                            true => local_agg_spilling_aggregate_payload(
                                                self.ctx.clone(),
                                                spiller.clone(),
                                                partitioned_payload,
                                            )?,
                                            false => exchange_agg_spilling_aggregate_payload(
                                                self.ctx.clone(),
                                                spiller.clone(),
                                                partitioned_payload,
                                            )?,
                                        },
                                    ));
                                }
                                SpillerVer::New(spillers) => {
                                    for (bucket, payload) in
                                        partitioned_payload.payloads.into_iter().enumerate()
                                    {
                                        if payload.len() == 0 {
                                            continue;
                                        }

                                        let data_block = payload
                                            .aggregate_flush_all()?
                                            .consume_convert_to_full();
                                        spillers[index].spill(bucket, data_block)?;
                                    }
                                    let block = if index == self.local_pos {
                                        DataBlock::empty()
                                    } else {
                                        serialize_block(-1, DataBlock::empty(), &self.options)?
                                    };
                                    serialized_blocks.push(FlightSerialized::DataBlock(block));
                                }
                            }
                        }

                        Some(AggregateMeta::AggregatePayload(p)) => {
                            // As soon as a non-spilled AggregatePayload shows up we must flush any pending
                            // spill files. AggregatePayload shows that partial stage is over, no more spilling
                            // will happen.
                            if matches!(&self.spiller, SpillerVer::New(_)) {
                                let spilled = self.finish()?;
                                if !spilled.is_empty() {
                                    spilled_blocks = Some(spilled);
                                }
                            }

                            let (bucket, max_partition_count) = (p.bucket, p.max_partition_count);

                            if index == self.local_pos {
                                serialized_blocks.push(FlightSerialized::DataBlock(
                                    block.add_meta(Some(Box::new(
                                        AggregateMeta::AggregatePayload(p),
                                    )))?,
                                ));
                                continue;
                            }

                            let block_number = compute_block_number(bucket, max_partition_count)?;
                            let stream = SerializeAggregateStream::create(
                                &self.params,
                                SerializePayload::AggregatePayload(p),
                            );
                            let mut stream_blocks =
                                stream.into_iter().collect::<Result<Vec<_>>>()?;
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
                return if let Some(spilled) = spilled_blocks {
                    Ok(vec![
                        DataBlock::empty_with_meta(FlightSerializedMeta::create(spilled)),
                        DataBlock::empty_with_meta(FlightSerializedMeta::create(serialized_blocks)),
                    ])
                } else {
                    Ok(vec![DataBlock::empty_with_meta(
                        FlightSerializedMeta::create(serialized_blocks),
                    )])
                };
            }

            data = data.add_meta(Some(block_meta))?;
        }
        Ok(vec![data])
    }

    fn on_finish(&mut self, _output: bool) -> Result<Vec<DataBlock>> {
        // if partial stage spilled all data, no one AggregatePayload shows up,
        // we need to finish spiller here.
        if let SpillerVer::New(_) = &self.spiller {
            let serialized_blocks = self.finish()?;
            if !serialized_blocks.is_empty() {
                return Ok(vec![DataBlock::empty_with_meta(
                    FlightSerializedMeta::create(serialized_blocks),
                )]);
            }
        }

        Ok(vec![])
    }
}

fn exchange_agg_spilling_aggregate_payload(
    ctx: Arc<QueryContext>,
    spiller: Arc<Spiller>,
    partitioned_payload: PartitionedPayload,
) -> Result<BoxFuture<'static, Result<DataBlock>>> {
    let partition_count = partitioned_payload.partition_count();
    let mut write_size = 0;
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
            // perf
            {
                Profile::record_usize_profile(ProfileStatisticsName::RemoteSpillWriteCount, 1);
                Profile::record_usize_profile(
                    ProfileStatisticsName::RemoteSpillWriteBytes,
                    write_bytes,
                );
                Profile::record_usize_profile(
                    ProfileStatisticsName::RemoteSpillWriteTime,
                    instant.elapsed().as_millis() as usize,
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
