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
use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::DataBlock;
use databend_common_expression::HashTableConfig;
use databend_common_expression::InputColumns;
use databend_common_expression::PayloadFlushState;
use databend_common_expression::ProbeState;
use databend_common_hashtable::HashtableLike;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_pipeline_transforms::processors::AccumulatingTransformer;
use log::info;

use crate::pipelines::processors::transforms::aggregator::aggregate_cell::GroupByHashTableDropper;
use crate::pipelines::processors::transforms::aggregator::aggregate_cell::HashTableCell;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::PartitionedHashMethod;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
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

struct GroupBySettings {
    convert_threshold: usize,
    max_memory_usage: usize,
    spilling_bytes_threshold_per_proc: usize,
}

impl TryFrom<Arc<QueryContext>> for GroupBySettings {
    type Error = ErrorCode;

    fn try_from(ctx: Arc<QueryContext>) -> std::result::Result<Self, Self::Error> {
        let settings = ctx.get_settings();
        let max_threads = settings.get_max_threads()? as usize;
        let convert_threshold = settings.get_group_by_two_level_threshold()? as usize;
        let mut memory_ratio = settings.get_aggregate_spilling_memory_ratio()? as f64 / 100_f64;

        if memory_ratio > 1_f64 {
            memory_ratio = 1_f64;
        }

        let max_memory_usage = match settings.get_max_memory_usage()? {
            0 => usize::MAX,
            max_memory_usage => match memory_ratio {
                x if x == 0_f64 => usize::MAX,
                memory_ratio => (max_memory_usage as f64 * memory_ratio) as usize,
            },
        };

        Ok(GroupBySettings {
            max_memory_usage,
            convert_threshold,
            spilling_bytes_threshold_per_proc: match settings
                .get_aggregate_spilling_bytes_threshold_per_proc()?
            {
                0 => max_memory_usage / max_threads,
                spilling_bytes_threshold_per_proc => spilling_bytes_threshold_per_proc,
            },
        })
    }
}

// SELECT column_name FROM table_name GROUP BY column_name
pub struct TransformPartialGroupBy {
    hash_table: HashTable,
    probe_state: ProbeState,
    settings: GroupBySettings,
    params: Arc<AggregatorParams>,

    start: Instant,
    first_block_start: Option<Instant>,
    processed_rows: usize,
    processed_bytes: usize,
}

impl TransformPartialGroupBy {
    pub fn create(
        ctx: Arc<QueryContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: Arc<AggregatorParams>,
        config: HashTableConfig,
    ) -> Box<dyn Processor> {
        let arena = Arc::new(Bump::new());
        let hash_table = HashTable::AggregateHashTable(AggregateHashTable::new(
            params.group_data_types.clone(),
            params.aggregate_functions.clone(),
            config,
            arena,
        ));

        AccumulatingTransformer::create(input, output, TransformPartialGroupBy {
            hash_table,
            probe_state: ProbeState::default(),
            params,
            settings: GroupBySettings::try_from(ctx)?,
            start: Instant::now(),
            first_block_start: None,
            processed_bytes: 0,
            processed_rows: 0,
        })
    }
}

impl AccumulatingTransform for TransformPartialGroupBy {
    const NAME: &'static str = "TransformPartialGroupBy";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        let block = block.consume_convert_to_full();

        let rows_num = block.num_rows();

        self.processed_bytes += block.memory_size();
        self.processed_rows += rows_num;
        if self.first_block_start.is_none() {
            self.first_block_start = Some(Instant::now());
        }

        let group_columns = InputColumns::new_block_proxy(&self.params.group_columns, &block);

        {
            match &mut self.hash_table {
                HashTable::MovedOut => unreachable!(),
                HashTable::AggregateHashTable(hashtable) => {
                    let _ = hashtable.add_groups(
                        &mut self.probe_state,
                        group_columns,
                        &[(&[]).into()],
                        (&[]).into(),
                        rows_num,
                    )?;
                }
            };

            if matches!(&self.hash_table, HashTable::AggregateHashTable(cell) if cell.allocated_bytes() > self.settings.spilling_bytes_threshold_per_proc
                    || GLOBAL_MEM_STAT.get_memory_usage() as usize >= self.settings.max_memory_usage)
            {
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
                        AggregateMeta::<()>::create_agg_spilling(partitioned_payload),
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

                unreachable!()
            }
        }

        Ok(vec![])
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        Ok(match std::mem::take(&mut self.hash_table) {
            HashTable::MovedOut => match !output && std::thread::panicking() {
                true => vec![],
                false => unreachable!(),
            },
            HashTable::AggregateHashTable(hashtable) => {
                let partition_count = hashtable.payload.partition_count();
                let mut blocks = Vec::with_capacity(partition_count);

                log::info!(
                    "Aggregated {} to {} rows in {} sec(real: {}). ({} rows/sec, {}/sec, {})",
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
                            AggregateMeta::<()>::create_agg_payload(
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
