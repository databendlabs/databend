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
use databend_common_expression::DictValueHashes;
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
    /// Created on the first rows to aggregate, so that the processors of a scan pruned to zero
    /// rows (or in bypass) do not each pay for a table while the pipeline is being built. Taken
    /// out when the input ends.
    table: Option<AggregateHashTable>,
    config: HashTableConfig,
    probe_state: ProbeState,
    params: Arc<AggregatorParams>,
    statistics: AggregationStatistics,
    settings: MemorySettings,
    state: PartialState,
    /// The most memory the table holds, see `partial_table_bytes`.
    max_table_bytes: usize,
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
        max_table_bytes: usize,
    ) -> Result<Box<dyn Processor>> {
        Ok(AccumulatingTransformer::create(
            input,
            output,
            Self::new(&ctx, params, config, mode, max_table_bytes)?,
        ))
    }

    fn new(
        ctx: &Arc<QueryContext>,
        params: Arc<AggregatorParams>,
        config: HashTableConfig,
        mode: PartialAggregateMode,
        max_table_bytes: usize,
    ) -> Result<Self> {
        Ok(TransformPartialAggregate {
            table: None,
            config,
            probe_state: ProbeState::default(),
            settings: MemorySettings::from_aggregate_settings(ctx)?,
            statistics: AggregationStatistics::new("PartialAggregate"),
            state: PartialState::new(mode),
            max_table_bytes,
            memory: MemoryMeter::default(),
            output_rows: 0,
            total: PartialAddStats::default(),
            emits: EmitCounts::default(),
            params,
        })
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

    fn aggregate_slice(
        &mut self,
        block: &DataBlock,
        dict_hashes: Option<&DictValueHashes>,
    ) -> Result<Vec<DataBlock>> {
        let rows_num = block.num_rows();
        let group_columns = ProjectedBlock::project(&self.params.group_columns, block);
        let params_columns =
            Self::aggregate_arguments(block, &self.params.aggregate_functions_arguments);
        let (params, config) = (&self.params, &self.config);
        let table = self.table.get_or_insert_with(|| {
            AggregateHashTable::new(
                params.group_data_types.clone(),
                params.aggregate_functions.clone(),
                config.clone(),
                Arc::new(Bump::new()),
            )
        });
        let capacity = self.state.capacity();
        let stats = self.memory.track(|| {
            table.add_groups_partial(
                &mut self.probe_state,
                group_columns,
                &params_columns,
                rows_num,
                capacity,
                self.state.sampler(),
                dict_hashes,
            )
        })?;
        self.total += stats;

        let observation = PartialObservation {
            stats,
            held_bytes: self.memory.bytes(),
            group_bytes: Self::group_bytes(&self.memory, &self.total),
            payload_full: table.partial_restarted()
                && table.partial_payload_bytes() >= self.params.max_block_bytes,
        };
        let settings = &self.settings;
        let Some(reason) = self
            .state
            .step(observation, self.max_table_bytes, || settings.check_spill())
        else {
            return Ok(vec![]);
        };
        self.emits.add(reason);
        let payload = table.take_partial_payload();
        Ok(self.emit(payload))
    }

    /// The memory a group took so far: its row, its states and what they own, and once grown, its
    /// share of the index.
    fn group_bytes(memory: &MemoryMeter, total: &PartialAddStats) -> usize {
        memory.total_bytes() / total.new_groups.max(1)
    }

    /// Hand `payload`, the groups taken from the table, to the final aggregate.
    fn emit(&mut self, payload: PartitionedPayload) -> Vec<DataBlock> {
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
                    blocks.push(AggregateMeta::payloads_block(std::mem::take(&mut payloads)));
                    (rows, bytes) = (0, 0);
                }
                rows += payload.len();
                bytes += payload.memory_size();
                payloads.push(AggregatePayload {
                    bucket: bucket as isize,
                    payload,
                });
            }
        }
        if !payloads.is_empty() {
            blocks.push(AggregateMeta::payloads_block(payloads));
        }
        blocks
    }
}

impl AccumulatingTransform for TransformPartialAggregate {
    const NAME: &'static str = "TransformPartialAggregate";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        let rows_num = block.num_rows();
        self.statistics.record_block(rows_num, block.memory_size());

        // Aggregate a large block in slices, so that the state can change within it.
        let mut output = vec![];
        let mut start = 0;
        // Each slice carries the whole dictionary of an encoded group column: hash it once, on
        // the first aggregated slice, rather than again per slice.
        let mut dict_hashes = None;
        while start < rows_num {
            if matches!(self.state, PartialState::Bypass) {
                // The rows are forwarded as is: the shuffle routes them by their group keys. The
                // final aggregate reads a block with a meta as states, so an upstream meta goes.
                self.output_rows += rows_num - start;
                let mut rows = match start {
                    0 => block,
                    _ => block.slice(start..rows_num),
                };
                rows.take_meta();
                output.push(rows);
                break;
            }
            let end = (start + DECISION_ROWS).min(rows_num);
            let slice = match (start, end) {
                (0, end) if end == rows_num => block.clone(),
                _ => block.slice(start..end),
            };
            let hashes = dict_hashes.get_or_insert_with(|| {
                let group_columns = ProjectedBlock::project(&self.params.group_columns, &block);
                DictValueHashes::new(group_columns, rows_num)
            });
            output.extend(self.aggregate_slice(&slice, hashes.as_ref())?);
            start = end;
        }
        Ok(output)
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        // No table means no rows were aggregated: they were bypassed, or there were none.
        let table = self.table.take();
        let (table_rows, hash_index_resizes) = table.as_ref().map_or((0, 0), |table| {
            (table.payload.len(), table.hash_index_resize_count())
        });
        self.statistics
            .log_partial_finish_statistics(self.output_rows + table_rows, hash_index_resizes);
        let total = &self.total;
        log::info!(
            "[PartialAggregate] Adaptive: final_state={:?}, rows={}, new_groups={}, windows={}, emits={:?}, group_bytes={}",
            self.state,
            total.rows,
            total.new_groups,
            total.windows,
            self.emits,
            Self::group_bytes(&self.memory, total),
        );
        match table {
            Some(table) if output => Ok(self.emit(table.payload)),
            _ => Ok(vec![]),
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::BlockMetaInfoDowncast;
    use databend_common_expression::DataField;
    use databend_common_expression::DataSchema;
    use databend_common_expression::FromData;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::UInt64Type;

    use super::*;
    use crate::pipelines::processors::transforms::aggregator::PartitionedData;
    use crate::test_kits::TestFixture;

    // Boxed like in the pipeline: the probe state alone is about 100KB, too large to
    // keep several transforms in the test future on the test thread's stack.
    fn create_transform(ctx: &Arc<QueryContext>) -> Result<Box<TransformPartialAggregate>> {
        let key_type = DataType::Number(NumberDataType::UInt64);
        let params = AggregatorParams::try_create(
            Arc::new(DataSchema::new(vec![DataField::new(
                "key",
                key_type.clone(),
            )])),
            vec![key_type],
            &[0],
            &[],
            &[],
            1024,
            1024 * 1024,
        )?;
        Ok(Box::new(TransformPartialAggregate::new(
            ctx,
            params,
            HashTableConfig::partial_aggregate(),
            PartialAggregateMode::Auto,
            64 * 1024 * 1024,
        )?))
    }

    fn output_rows(blocks: &[DataBlock]) -> usize {
        blocks
            .iter()
            .map(|block| {
                let meta = block.get_meta().and_then(AggregateMeta::downcast_ref_from);
                let Some(AggregateMeta::Partitioned(PartitionedData::AggregatePayload(payloads))) =
                    meta
                else {
                    unreachable!("partial aggregate only outputs aggregate payloads here")
                };
                payloads.iter().map(|p| p.payload.len()).sum::<usize>()
            })
            .sum()
    }

    // The hash table is created on the first rows to aggregate, not before.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_partial_aggregate_creates_table_on_first_rows() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;

        let mut transform = create_transform(&ctx)?;
        assert!(transform.table.is_none());
        assert!(transform.on_finish(true)?.is_empty());

        // An empty block aggregates nothing and creates no table.
        let mut transform = create_transform(&ctx)?;
        let empty = DataBlock::new_from_columns(vec![UInt64Type::from_data(Vec::<u64>::new())]);
        assert!(transform.transform(empty)?.is_empty());
        assert!(transform.table.is_none());
        assert!(transform.on_finish(true)?.is_empty());

        let mut transform = create_transform(&ctx)?;
        let rows = DataBlock::new_from_columns(vec![UInt64Type::from_data(vec![1, 2, 2, 3])]);
        assert!(transform.transform(rows)?.is_empty());
        assert!(transform.table.is_some());
        let blocks = transform.on_finish(true)?;
        assert_eq!(blocks.len(), 1);
        assert_eq!(output_rows(&blocks), 3);
        Ok(())
    }
}
