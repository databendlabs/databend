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
use databend_common_base::runtime::MemoryMeter;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::DataBlock;
use databend_common_expression::EmitReason;
use databend_common_expression::HashTableConfig;
use databend_common_expression::PartialAddStats;
use databend_common_expression::PartialAggregateMode;
use databend_common_expression::PartialObservation;
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

/// TransformPartialAggregate is a cache in front of the final aggregate. For each slice of the
/// input it measures, lets `PartialState::step` decide and emits the table when asked; in bypass,
/// input blocks are forwarded unchanged as raw rows for the final aggregate to aggregate. The
/// partial aggregate never spills: the state bounds the table and the final aggregate spills.
pub struct TransformPartialAggregate {
    table: AggregateHashTable,
    probe_state: ProbeState,
    params: Arc<AggregatorParams>,
    statistics: AggregationStatistics,
    settings: MemorySettings,
    state: PartialState,
    /// Memory the table holds since it was last emitted, including what the aggregate states
    /// own outside the arena.
    memory: MemoryMeter,
    output_rows: usize,
    /// Stats of all calls to the table, and why it was emitted.
    total: PartialAddStats,
    emits: EmitCounts,
}

/// Times the table was emitted, by reason.
#[derive(Debug, Default)]
struct EmitCounts {
    grow: usize,
    bypass: usize,
    shrink: usize,
    memory_full: usize,
    payload_full: usize,
}

impl EmitCounts {
    fn add(&mut self, reason: EmitReason) {
        *match reason {
            EmitReason::Grow => &mut self.grow,
            EmitReason::Bypass => &mut self.bypass,
            EmitReason::Shrink => &mut self.shrink,
            EmitReason::MemoryFull => &mut self.memory_full,
            EmitReason::PayloadFull => &mut self.payload_full,
        } += 1;
    }
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
        let table = AggregateHashTable::new(
            params.group_data_types.clone(),
            params.aggregate_functions.clone(),
            config,
            Arc::new(Bump::new()),
        );

        Ok(AccumulatingTransformer::create(
            input,
            output,
            TransformPartialAggregate {
                table,
                probe_state: ProbeState::default(),
                settings: MemorySettings::from_aggregate_settings(&ctx)?,
                statistics: AggregationStatistics::new("PartialAggregate"),
                state: PartialState::new(mode),
                memory: MemoryMeter::default(),
                output_rows: 0,
                total: PartialAddStats::default(),
                emits: EmitCounts::default(),
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
        let group_columns = ProjectedBlock::project(&self.params.group_columns, block);
        let params_columns =
            Self::aggregate_arguments(block, &self.params.aggregate_functions_arguments);
        let capacity = self.state.capacity();
        let stats = self.memory.track(|| {
            self.table.add_groups_partial(
                &mut self.probe_state,
                group_columns,
                &params_columns,
                rows_num,
                capacity,
                self.state.sampler(),
            )
        })?;
        self.total += stats;

        let observation = PartialObservation {
            stats,
            held_bytes: self.memory.bytes(),
            group_bytes: self.group_bytes(),
            payload_full: self.table.partial_restarted()
                && self.table.partial_hot_bytes() >= self.params.max_block_bytes,
        };
        let settings = &self.settings;
        let Some(reason) = self.state.step(observation, || settings.check_spill()) else {
            return Ok(vec![]);
        };
        self.emits.add(reason);
        Ok(self.emit())
    }

    /// The memory a group took so far besides the index: its row, its states and what they own.
    fn group_bytes(&self) -> usize {
        self.memory.total_bytes() / self.total.new_groups.max(1)
    }

    /// Hand the groups of the table to the final aggregate and restart it.
    fn emit(&mut self) -> Vec<DataBlock> {
        let payload = self.table.take_partial_hot();
        self.memory.mark();
        self.output_rows += payload.len();
        Self::partitioned_blocks(payload, &self.params)
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
        if !output {
            return Ok(vec![]);
        }
        self.statistics.log_partial_finish_statistics(
            self.output_rows + self.table.len(),
            self.table.hash_index_resize_count(),
        );
        let total = &self.total;
        log::info!(
            "[PartialAggregate] Adaptive: final_state={:?}, rows={}, new_groups={}, windows={}, emits={:?}, group_bytes={}",
            self.state,
            total.rows,
            total.new_groups,
            total.windows,
            self.emits,
            self.group_bytes(),
        );
        Ok(self.emit())
    }
}
