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
use std::vec;

use bumpalo::Bump;
use databend_common_base::base::convert_byte_size;
use databend_common_base::base::convert_number_size;
use databend_common_catalog::plan::AggIndexMeta;
use databend_common_exception::Result;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::HashTableConfig;
use databend_common_expression::PayloadFlushState;
use databend_common_expression::ProbeState;
use databend_common_expression::ProjectedBlock;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_pipeline_transforms::processors::AccumulatingTransformer;
use databend_common_pipeline_transforms::MemorySettings;

use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
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

// SELECT column_name, agg(xxx) FROM table_name GROUP BY column_name
pub struct TransformPartialAggregate {
    hash_table: HashTable,
    probe_state: ProbeState,
    params: Arc<AggregatorParams>,
    start: Instant,
    first_block_start: Option<Instant>,
    processed_bytes: usize,
    processed_rows: usize,
    settings: MemorySettings,
}

impl TransformPartialAggregate {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: Arc<AggregatorParams>,
        config: HashTableConfig,
    ) -> Result<Box<dyn Processor>> {
        let arena = Arc::new(Bump::new());
        // when enable_experiment_aggregate, we will repartition again in the final stage
        // it will be too small if we use max radix bits here
        let hash_table = if params.has_distinct_combinator() {
            let max_radix_bits = config.max_radix_bits;
            HashTable::AggregateHashTable(AggregateHashTable::new(
                params.group_data_types.clone(),
                params.aggregate_functions.clone(),
                config.with_initial_radix_bits(max_radix_bits),
                arena,
            ))
        } else {
            HashTable::AggregateHashTable(AggregateHashTable::new(
                params.group_data_types.clone(),
                params.aggregate_functions.clone(),
                config,
                arena,
            ))
        };

        Ok(AccumulatingTransformer::create(
            input,
            output,
            TransformPartialAggregate {
                params,
                hash_table,
                probe_state: ProbeState::default(),
                settings: MemorySettings::from_aggregate_settings(&ctx)?,
                start: Instant::now(),
                first_block_start: None,
                processed_bytes: 0,
                processed_rows: 0,
            },
        ))
    }

    // Block should be `convert_to_full`.
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

        self.processed_bytes += block.memory_size();
        self.processed_rows += rows_num;
        if self.first_block_start.is_none() {
            self.first_block_start = Some(Instant::now());
        }

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
}

impl AccumulatingTransform for TransformPartialAggregate {
    const NAME: &'static str = "TransformPartialAggregate";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        self.execute_one_block(block)?;

        if self.settings.check_spill() {
            if let HashTable::AggregateHashTable(v) = std::mem::take(&mut self.hash_table) {
                let group_types = v.payload.group_types.clone();
                let aggrs = v.payload.aggrs.clone();
                v.config.update_current_max_radix_bits();
                let config = v
                    .config
                    .clone()
                    .with_initial_radix_bits(v.config.max_radix_bits);

                let mut state = PayloadFlushState::default();

                // repartition to max for normalization
                let partitioned_payload = v
                    .payload
                    .repartition(1 << config.max_radix_bits, &mut state);

                let blocks = vec![DataBlock::empty_with_meta(
                    AggregateMeta::create_agg_spilling(partitioned_payload),
                )];

                let arena = Arc::new(Bump::new());
                self.hash_table = HashTable::AggregateHashTable(AggregateHashTable::new(
                    group_types,
                    aggrs,
                    config,
                    arena,
                ));
                return Ok(blocks);
            }

            unreachable!("[TRANSFORM-AGGREGATOR] Invalid hash table state during spill check")
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
                let partition_count = hashtable.payload.partition_count();
                let mut blocks = Vec::with_capacity(partition_count);

                log::info!(
                    "[TRANSFORM-AGGREGATOR] Aggregation completed: {} → {} rows in {:.2}s (real: {:.2}s), throughput: {} rows/sec, {}/sec, total: {}",
                    self.processed_rows,
                    hashtable.payload.len(),
                    self.start.elapsed().as_secs_f64(),
                    if let Some(t) = &self.first_block_start {
                        t.elapsed().as_secs_f64()
                    } else {
                        self.start.elapsed().as_secs_f64()
                    },
                    convert_number_size(
                        self.processed_rows as f64 / self.start.elapsed().as_secs_f64()
                    ),
                    convert_byte_size(
                        self.processed_bytes as f64 / self.start.elapsed().as_secs_f64()
                    ),
                    convert_byte_size(self.processed_bytes as f64),
                );

                for (bucket, payload) in hashtable.payload.payloads.into_iter().enumerate() {
                    if payload.len() != 0 {
                        blocks.push(DataBlock::empty_with_meta(
                            AggregateMeta::create_agg_payload(
                                bucket as isize,
                                payload,
                                partition_count,
                            ),
                        ));
                    }
                }

                blocks
            }
        })
    }
}
