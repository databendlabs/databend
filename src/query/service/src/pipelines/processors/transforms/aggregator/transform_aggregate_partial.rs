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

use bumpalo::Bump;
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::DataBlock;
use databend_common_expression::HashTableConfig;
use databend_common_expression::PartialAddStats;
use databend_common_expression::PartialAggregateMode;
use databend_common_expression::PartialDecision;
use databend_common_expression::PartialEnvironment;
use databend_common_expression::PartialState;
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
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatePayload;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::PartitionedData;
use crate::pipelines::processors::transforms::aggregator::statistics::AggregationStatistics;
use crate::sessions::QueryContext;

/// Rows aggregated between two observations of the partial state, so that it changes within
/// large input blocks rather than after them.
const DECISION_ROWS: usize = 64 * 1024;

#[allow(clippy::enum_variant_names)]
pub(crate) enum HashTable {
    MovedOut,
    AggregateHashTable(AggregateHashTable),
}

impl Default for HashTable {
    fn default() -> Self {
        Self::MovedOut
    }
}

/// Parse the `partial_aggregate_mode` setting.
pub fn parse_partial_aggregate_mode(mode: &str) -> Result<PartialAggregateMode> {
    Ok(match mode.to_lowercase().as_str() {
        "auto" => PartialAggregateMode::Auto,
        "bypass" => PartialAggregateMode::Bypass,
        other => {
            return Err(ErrorCode::InvalidArgument(format!(
                "Invalid partial_aggregate_mode: {other}"
            )));
        }
    })
}

/// TransformPartialAggregate is a cache in front of the final aggregate, see `PartialState`.
///
/// The table is emitted downstream whenever `PartialState` asks for it. In bypass, input blocks
/// are forwarded unchanged as raw rows for the final aggregate to aggregate. The partial
/// aggregate never spills: its table is bounded by the state machine, and the final aggregate
/// spills under memory pressure.
pub struct TransformPartialAggregate {
    hash_table: HashTable,
    probe_state: ProbeState,
    params: Arc<AggregatorParams>,
    statistics: AggregationStatistics,
    settings: MemorySettings,
    state: PartialState,
    /// The index restarted since the table was last emitted: the payload is no longer bounded
    /// by the index, so emit it once it holds `flush_bytes`.
    windowed: bool,
    flush_bytes: usize,
    concurrency: usize,
    /// Bytes per group of the table, updated once the table holds enough groups that page
    /// preallocation does not dominate.
    bytes_per_group: usize,
    output_rows: usize,
    /// Stats of all calls to the table, and times the index grew.
    total: PartialAddStats,
    grows: usize,
}

impl TransformPartialAggregate {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: Arc<AggregatorParams>,
        config: HashTableConfig,
        mode: PartialAggregateMode,
    ) -> Result<Box<dyn Processor>> {
        let concurrency = ctx.get_settings().get_max_threads()?.max(1) as usize;
        let hash_table = HashTable::AggregateHashTable(AggregateHashTable::new(
            params.group_data_types.clone(),
            params.aggregate_functions.clone(),
            config,
            Arc::new(Bump::new()),
        ));

        Ok(AccumulatingTransformer::create(
            input,
            output,
            TransformPartialAggregate {
                hash_table,
                probe_state: ProbeState::default(),
                settings: MemorySettings::from_aggregate_settings(&ctx)?,
                statistics: AggregationStatistics::new("PartialAggregate"),
                state: PartialState::new(mode),
                windowed: false,
                flush_bytes: params.max_block_bytes,
                concurrency,
                bytes_per_group: 64,
                output_rows: 0,
                total: PartialAddStats::default(),
                grows: 0,
                params,
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

    fn transform_block(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        let rows_num = block.num_rows();
        self.statistics.record_block(rows_num, block.memory_size());

        // Aggregate a large block in slices, so that the state can change within it.
        let mut output = vec![];
        let mut start = 0;
        while start < rows_num {
            if matches!(self.state, PartialState::Bypass) {
                // The rows are forwarded as is: the shuffle routes them by their group keys.
                self.output_rows += rows_num - start;
                output.push(match start {
                    0 => block,
                    _ => block.slice(start..rows_num),
                });
                break;
            }
            let end = (start + DECISION_ROWS).min(rows_num);
            let slice = match (start, end) {
                (0, end) if end == rows_num => block.clone(),
                _ => block.slice(start..end),
            };
            output.extend(self.aggregate_slice(&slice)?);
            start = end;
        }
        Ok(output)
    }

    fn aggregate_slice(&mut self, block: &DataBlock) -> Result<Vec<DataBlock>> {
        let rows_num = block.num_rows();
        let HashTable::AggregateHashTable(hashtable) = &mut self.hash_table else {
            unreachable!("[TRANSFORM-AGGREGATOR] Hash table already moved out")
        };
        let group_columns = ProjectedBlock::project(&self.params.group_columns, block);
        let params_columns =
            Self::aggregate_arguments(block, &self.params.aggregate_functions_arguments);
        let capacity = self.state.capacity();
        let stats = hashtable.add_groups_partial(
            &mut self.probe_state,
            group_columns,
            &params_columns,
            rows_num,
            capacity,
            self.state.sampler(),
        )?;
        self.windowed |= stats.windows > 0;
        self.total += stats;

        // An index entry costs a control byte and a row pointer at the index load factor.
        const INDEX_BYTES_PER_GROUP: usize = 13;
        if hashtable.len() >= 16 * 1024 {
            self.bytes_per_group =
                hashtable.partial_hot_bytes() / hashtable.len() + INDEX_BYTES_PER_GROUP;
        }
        let env = PartialEnvironment {
            headroom_bytes: self
                .settings
                .check_spill_remain()
                .map(|remain| remain.max(0) as usize),
            bytes_per_group: self.bytes_per_group,
            concurrency: self.concurrency,
        };
        let decision = self.state.observe(stats, &env);
        self.grows += matches!(decision, PartialDecision::Grow { .. }) as usize;

        if decision == PartialDecision::Continue
            && !(self.windowed && hashtable.partial_hot_bytes() >= self.flush_bytes)
        {
            return Ok(vec![]);
        }
        let hot = hashtable.take_partial_hot();
        self.windowed = false;
        self.output_rows += hot.len();
        Ok(Self::partitioned_blocks(hot, &self.params))
    }

    /// Wrap the non-empty buckets of `payload` into blocks for the final aggregate, each of at
    /// most about `max_block_rows` rows and `max_block_bytes` bytes. Buckets are split at page
    /// boundaries without copying rows, so the final aggregate checks memory and the exchange
    /// sends messages at block granularity whatever the size of the table.
    fn partitioned_blocks(
        payload: PartitionedPayload,
        params: &AggregatorParams,
    ) -> Vec<DataBlock> {
        let (max_rows, max_bytes) = (params.max_block_rows, params.max_block_bytes);
        let mut blocks = vec![];
        let mut payloads = vec![];
        let (mut rows, mut bytes) = (0, 0);
        for (bucket, payload) in payload.into_non_empty_bucket_payloads() {
            for payload in payload.split_pages(max_rows, max_bytes) {
                if !payloads.is_empty()
                    && (rows + payload.len() > max_rows
                        || bytes + payload.memory_size() > max_bytes)
                {
                    blocks.push(Self::payload_block(std::mem::take(&mut payloads)));
                    (rows, bytes) = (0, 0);
                }
                rows += payload.len();
                bytes += payload.memory_size();
                payloads.push(AggregatePayload {
                    bucket: bucket as isize,
                    payload,
                    max_partition_count: 0,
                });
            }
        }
        if !payloads.is_empty() {
            blocks.push(Self::payload_block(payloads));
        }
        blocks
    }

    fn payload_block(payloads: Vec<AggregatePayload>) -> DataBlock {
        DataBlock::empty_with_meta(AggregateMeta::create_partitioned(
            None,
            PartitionedData::AggregatePayload(payloads),
        ))
    }
}

impl AccumulatingTransform for TransformPartialAggregate {
    const NAME: &'static str = "TransformPartialAggregate";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        self.transform_block(block)
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        Ok(match std::mem::take(&mut self.hash_table) {
            HashTable::MovedOut => match !output {
                true => vec![],
                false => {
                    unreachable!("[TRANSFORM-AGGREGATOR] Hash table already moved out in finish")
                }
            },
            HashTable::AggregateHashTable(hashtable) => {
                self.statistics.log_partial_finish_statistics(
                    self.output_rows + hashtable.len(),
                    hashtable.hash_index_resize_count(),
                );
                let total = &self.total;
                log::info!(
                    "[PartialAggregate] Adaptive: final_state={:?}, rows={}, new_groups={}, windows={}, grows={}",
                    self.state,
                    total.rows,
                    total.new_groups,
                    total.windows,
                    self.grows,
                );
                Self::partitioned_blocks(hashtable.payload, &self.params)
            }
        })
    }
}
