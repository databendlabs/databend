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
use std::vec;

use bumpalo::Bump;
use databend_common_catalog::plan::AggIndexMeta;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::HashTableConfig;
use databend_common_expression::MAX_AGGREGATE_HASHTABLE_BUCKETS_NUM;
use databend_common_expression::PartitionedPayload;
use databend_common_expression::Payload;
use databend_common_expression::ProbeState;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::StringType;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_pipeline_transforms::processors::AccumulatingTransformer;
use databend_common_storages_parquet::serialize_row_group_meta_to_bytes;

use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::transforms::aggregator::AggregatePayload;
use crate::pipelines::processors::transforms::aggregator::AggregateSerdeMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::FlightSerialized;
use crate::pipelines::processors::transforms::aggregator::FlightSerializedMeta;
use crate::pipelines::processors::transforms::aggregator::NewAggregateSpiller;
use crate::pipelines::processors::transforms::aggregator::SharedPartitionStream;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::exchange_defines;
use crate::pipelines::processors::transforms::aggregator::scatter_partitioned_payload;
use crate::pipelines::processors::transforms::aggregator::statistics::AggregationStatistics;
use crate::servers::flight::v1::exchange::ExchangeShuffleMeta;
use crate::servers::flight::v1::exchange::serde::serialize_block;
use crate::sessions::QueryContext;

#[allow(clippy::enum_variant_names)]
enum HashTable {
    MovedOut,
    AggregateHashTable(AggregateHashTable),
}

impl Default for HashTable {
    fn default() -> Self {
        Self::MovedOut
    }
}

enum Spiller {
    Standalone(NewAggregateSpiller<SharedPartitionStream>),
    // (local_pos, spillers for all)
    Clusters(usize, Vec<NewAggregateSpiller<SharedPartitionStream>>),
}

impl Spiller {
    pub fn create(
        ctx: Arc<QueryContext>,
        partition_streams: Vec<SharedPartitionStream>,
        local_pos: usize,
    ) -> Result<Self> {
        if partition_streams.is_empty() {
            return Err(ErrorCode::Internal("partition_streams is empty"));
        }

        match partition_streams.len() {
            1 => {
                let stream = partition_streams
                    .into_iter()
                    .next()
                    .ok_or_else(|| ErrorCode::Internal("partition_streams is empty"))?;
                let spiller = NewAggregateSpiller::try_create(
                    ctx.clone(),
                    MAX_AGGREGATE_HASHTABLE_BUCKETS_NUM as usize,
                    stream,
                    true,
                )?;
                Ok(Spiller::Standalone(spiller))
            }
            _ => {
                let spillers = partition_streams
                    .into_iter()
                    .enumerate()
                    .map(|(pos, stream)| {
                        NewAggregateSpiller::try_create(
                            ctx.clone(),
                            MAX_AGGREGATE_HASHTABLE_BUCKETS_NUM as usize,
                            stream,
                            pos == local_pos,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Spiller::Clusters(local_pos, spillers))
            }
        }
    }

    fn spill_partition(
        spiller: &mut NewAggregateSpiller<SharedPartitionStream>,
        payloads: Vec<Payload>,
    ) -> Result<()> {
        for (bucket, payload) in payloads.into_iter().enumerate() {
            if payload.len() == 0 {
                continue;
            }

            let data_block = payload.aggregate_flush_all()?.consume_convert_to_full();
            spiller.spill(bucket, data_block)?;
        }

        Ok(())
    }

    pub fn spill(
        &mut self,
        mut partition: PartitionedPayload,
        is_row_shuffle: bool,
        dispatch_method: &[u64],
    ) -> Result<()> {
        match self {
            Spiller::Standalone(spiller) => Self::spill_partition(spiller, partition.payloads),
            Spiller::Clusters(_, spillers) => {
                let nodes_num = spillers.len();
                if is_row_shuffle {
                    for (idx, partition) in scatter_partitioned_payload(partition, nodes_num)?
                        .into_iter()
                        .enumerate()
                    {
                        Self::spill_partition(&mut spillers[idx], partition.payloads)?;
                    }
                } else {
                    for (idx, payload_num) in dispatch_method.iter().enumerate() {
                        let partition = partition
                            .payloads
                            .drain(0..*payload_num as usize)
                            .collect::<Vec<_>>();
                        Self::spill_partition(&mut spillers[idx], partition)?;
                    }
                    debug_assert!(partition.payloads.is_empty());
                }

                Ok(())
            }
        }
    }

    fn finish_standalone(
        spiller: &mut NewAggregateSpiller<SharedPartitionStream>,
        is_row_shuffle: bool,
        dispatch_method: &[u64],
    ) -> Result<Vec<DataBlock>> {
        let payloads = spiller.spill_finish()?;

        if payloads.is_empty() {
            return Ok(vec![]);
        }

        if is_row_shuffle {
            let mut blocks = Vec::with_capacity(payloads.len());
            for payload in payloads {
                let meta = AggregateMeta::create_new_bucket_spilled(payload);
                blocks.push(DataBlock::empty_with_meta(meta));
            }
            return Ok(blocks);
        } else {
            let thread_num = *dispatch_method.first().ok_or_else(|| {
                ErrorCode::Internal("dispatch_method is empty in finish_standalone")
            })? as usize;

            return Ok(vec![DataBlock::empty_with_meta(
                ExchangeShuffleMeta::create(AggregateMeta::create_new_spilled_blocks(
                    thread_num, payloads,
                )),
            )]);
        }
    }

    fn finish_clusters(
        local_pos: usize,
        spillers: &mut [NewAggregateSpiller<SharedPartitionStream>],
        is_row_shuffle: bool,
        dispatch_method: &[u64],
    ) -> Result<Vec<DataBlock>> {
        let mut serialized_blocks = Vec::with_capacity(spillers.len());
        let write_options = exchange_defines::spilled_write_options();

        for (index, spiller) in spillers.iter_mut().enumerate() {
            let spilled_payloads = spiller.spill_finish()?;

            if index == local_pos {
                if is_row_shuffle {
                    let block = DataBlock::empty_with_meta(AggregateMeta::create_new_spilled(
                        spilled_payloads,
                    ));
                    serialized_blocks.push(FlightSerialized::DataBlock(block));
                } else {
                    let thread_num = dispatch_method[index] as usize;
                    let dispatch_blocks =
                        AggregateMeta::create_new_spilled_blocks(thread_num, spilled_payloads);

                    let block =
                        DataBlock::empty_with_meta(ExchangeShuffleMeta::create(dispatch_blocks));

                    serialized_blocks.push(FlightSerialized::DataBlock(block));
                }
                continue;
            }

            if spilled_payloads.is_empty() {
                serialized_blocks.push(FlightSerialized::DataBlock(serialize_block(
                    -1,
                    DataBlock::empty(),
                    &write_options,
                )?));
                continue;
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
            let meta = if is_row_shuffle {
                AggregateSerdeMeta::create_new_spilled(-1)
            } else {
                AggregateSerdeMeta::create_new_spilled(dispatch_method[index] as isize)
            };
            let data_block = data_block.add_meta(Some(meta))?;
            serialized_blocks.push(FlightSerialized::DataBlock(serialize_block(
                -1,
                data_block,
                &write_options,
            )?));
        }

        Ok(vec![DataBlock::empty_with_meta(
            FlightSerializedMeta::create(serialized_blocks),
        )])
    }

    pub fn finish(
        &mut self,
        is_row_shuffle: bool,
        dispatch_method: &[u64],
    ) -> Result<Vec<DataBlock>> {
        match self {
            Spiller::Standalone(spiller) => {
                Self::finish_standalone(spiller, is_row_shuffle, dispatch_method)
            }
            Spiller::Clusters(local_pos, spillers) => {
                Self::finish_clusters(*local_pos, spillers, is_row_shuffle, dispatch_method)
            }
        }
    }
}

/// NewTransformPartialAggregate combine partial aggregation and spilling logic
/// When memory exceeds threshold, it will spill out current hash table into a buffer
/// and real spill out will happen when the buffer is full.
pub struct NewTransformPartialAggregate {
    hash_table: HashTable,
    probe_state: ProbeState,
    params: Arc<AggregatorParams>,
    statistics: AggregationStatistics,
    settings: MemorySettings,
    spillers: Spiller,
    dispatch_method: Vec<u64>,
    is_row_shuffle: bool,
}

impl NewTransformPartialAggregate {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: Arc<AggregatorParams>,
        config: HashTableConfig,
        partition_streams: Vec<SharedPartitionStream>,
        local_pos: usize,
        dispatch_method: Vec<u64>,
        is_row_shuffle: bool,
    ) -> Result<Box<dyn Processor>> {
        let spillers = Spiller::create(ctx.clone(), partition_streams, local_pos)?;

        let arena = Arc::new(Bump::new());
        dbg!(&config.initial_radix_bits);
        let hash_table = HashTable::AggregateHashTable(AggregateHashTable::new(
            params.group_data_types.clone(),
            params.aggregate_functions.clone(),
            config,
            arena,
        ));

        Ok(AccumulatingTransformer::create(
            input,
            output,
            NewTransformPartialAggregate {
                params,
                hash_table,
                probe_state: ProbeState::default(),
                settings: MemorySettings::from_aggregate_settings(&ctx)?,
                statistics: AggregationStatistics::new("NewPartialAggregate"),
                spillers,
                dispatch_method,
                is_row_shuffle,
            },
        ))
    }

    #[inline(always)]
    fn aggregate_arguments<'a>(
        block: &'a DataBlock,
        aggregate_functions_arguments: &'a [Vec<usize>],
    ) -> Vec<ProjectedBlock<'a>> {
        aggregate_functions_arguments
            .iter()
            .map(|function_arguments| ProjectedBlock::project(function_arguments, block))
            .collect::<Vec<_>>()
    }

    #[inline(always)]
    fn execute_one_block(&mut self, block: DataBlock) -> Result<()> {
        let is_agg_index_block = block
            .get_meta()
            .and_then(AggIndexMeta::downcast_ref_from)
            .map(|index| index.is_agg)
            .unwrap_or_default();

        let group_columns = ProjectedBlock::project(&self.params.group_columns, &block);
        let rows_num = block.num_rows();
        let block_bytes = block.memory_size();

        self.statistics.record_block(rows_num, block_bytes);

        {
            match &mut self.hash_table {
                HashTable::MovedOut => {
                    unreachable!("[TRANSFORM-AGGREGATOR] Hash table already moved out")
                }
                HashTable::AggregateHashTable(hashtable) => {
                    let (params_columns, states_index) = if is_agg_index_block {
                        let num_columns = block.num_columns();
                        let states_count = self
                            .params
                            .states_layout
                            .as_ref()
                            .map(|layout| layout.num_aggr_func())
                            .unwrap_or(0);
                        (
                            vec![],
                            (num_columns - states_count..num_columns).collect::<Vec<_>>(),
                        )
                    } else {
                        (
                            Self::aggregate_arguments(
                                &block,
                                &self.params.aggregate_functions_arguments,
                            ),
                            vec![],
                        )
                    };

                    let agg_states = if !states_index.is_empty() {
                        ProjectedBlock::project(&states_index, &block)
                    } else {
                        (&[]).into()
                    };

                    let _ = hashtable.add_groups(
                        &mut self.probe_state,
                        group_columns,
                        &params_columns,
                        agg_states,
                        rows_num,
                    )?;
                    Ok(())
                }
            }
        }
    }

    fn spill_out(&mut self) -> Result<()> {
        if let HashTable::AggregateHashTable(v) = std::mem::take(&mut self.hash_table) {
            let group_types = v.payload.group_types.clone();
            let aggrs = v.payload.aggrs.clone();
            let config = v.config.clone();

            self.spillers
                .spill(v.payload, self.is_row_shuffle, &self.dispatch_method)?;

            let arena = Arc::new(Bump::new());
            self.hash_table = HashTable::AggregateHashTable(AggregateHashTable::new(
                group_types,
                aggrs,
                config,
                arena,
            ));
        } else {
            unreachable!("[TRANSFORM-AGGREGATOR] Invalid hash table state during spill check")
        }
        Ok(())
    }
}

impl AccumulatingTransform for NewTransformPartialAggregate {
    const NAME: &'static str = "NewTransformPartialAggregate";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        self.execute_one_block(block)?;

        if self.settings.check_spill() {
            self.spill_out()?;
        }

        Ok(vec![])
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        Ok(match std::mem::take(&mut self.hash_table) {
            HashTable::MovedOut => match !output && std::thread::panicking() {
                true => vec![],
                false => {
                    unreachable!("[TRANSFORM-AGGREGATOR] Hash table already moved out in finish")
                }
            },
            HashTable::AggregateHashTable(hashtable) => {
                let mut blocks = self
                    .spillers
                    .finish(self.is_row_shuffle, &self.dispatch_method)?;

                self.statistics.log_finish_statistics(&hashtable);

                let payloads = hashtable
                    .payload
                    .payloads
                    .into_iter()
                    .enumerate()
                    .map(|(bucket, payload)| {
                        AggregateMeta::AggregatePayload(AggregatePayload {
                            bucket: bucket as isize,
                            payload,
                            max_partition_count: 0,
                        })
                    })
                    .collect::<Vec<_>>();

                blocks.push(DataBlock::empty_with_meta(
                    AggregateMeta::create_partitioned(None, payloads),
                ));
                blocks
            }
        })
    }
}
