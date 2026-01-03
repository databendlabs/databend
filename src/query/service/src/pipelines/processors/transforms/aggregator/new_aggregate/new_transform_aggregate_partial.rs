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
use databend_common_exception::Result;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::HashTableConfig;
use databend_common_expression::PartitionedPayload;
use databend_common_expression::ProbeState;
use databend_common_expression::ProjectedBlock;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_pipeline_transforms::processors::AccumulatingTransformer;

use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::transforms::aggregator::AggregatePayload;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::NewAggregateSpiller;
use crate::pipelines::processors::transforms::aggregator::SharedPartitionStream;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::statistics::AggregationStatistics;
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

pub struct Spiller {
    inner: NewAggregateSpiller<SharedPartitionStream>,
}

impl Spiller {
    pub fn create(
        ctx: Arc<QueryContext>,
        partition_streams: SharedPartitionStream,
        bucket_num: usize,
    ) -> Result<Self> {
        let spiller =
            NewAggregateSpiller::try_create(ctx.clone(), bucket_num, partition_streams, true)?;
        Ok(Self { inner: spiller })
    }

    pub fn spill(&mut self, mut partition: PartitionedPayload, is_row_shuffle: bool) -> Result<()> {
        for (bucket, payload) in partition.payloads.into_iter().enumerate() {
            if payload.len() == 0 {
                continue;
            }

            let data_block = payload.aggregate_flush_all()?.consume_convert_to_full();
            self.inner.spill(bucket, data_block)?;
        }
        Ok(())
    }

    pub fn finish(&mut self, is_row_shuffle: bool) -> Result<Vec<DataBlock>> {
        let payloads = self.inner.spill_finish()?;

        if payloads.is_empty() {
            return Ok(vec![]);
        }
        let payloads = payloads
            .into_iter()
            .map(|p| AggregateMeta::NewBucketSpilled(p))
            .collect::<Vec<_>>();
        let partitioned_payload =
            DataBlock::empty_with_meta(AggregateMeta::create_partitioned(None, payloads));
        return Ok(vec![partitioned_payload]);
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
    is_row_shuffle: bool,
}

impl NewTransformPartialAggregate {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: Arc<AggregatorParams>,
        config: HashTableConfig,
        partition_stream: SharedPartitionStream,
        bucket_num: usize,
        is_row_shuffle: bool,
    ) -> Result<Box<dyn Processor>> {
        let spillers = Spiller::create(ctx.clone(), partition_stream, bucket_num)?;

        let arena = Arc::new(Bump::new());

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

            self.spillers.spill(v.payload, self.is_row_shuffle)?;

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
                let mut blocks = self.spillers.finish(self.is_row_shuffle)?;

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
