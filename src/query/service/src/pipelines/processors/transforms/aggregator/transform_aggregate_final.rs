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
use std::collections::VecDeque;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use async_channel::Receiver;
use async_channel::Sender;
use databend_common_base::base::WatchNotify;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::BlockConcatStream;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::HashTableConfig;
use databend_common_expression::Payload;
use databend_common_expression::PayloadFlushState;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::StagedGroups;
use databend_common_expression::StagedOutput;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::EventCause;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::check_interrupt;
use databend_common_pipeline_transforms::MemorySettings;

use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatePayload;
use crate::pipelines::processors::transforms::aggregator::AggregateSpiller;
use crate::pipelines::processors::transforms::aggregator::AggregateStatesMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::SerializedPayload;
use crate::pipelines::processors::transforms::aggregator::SpilledPayload;
use crate::pipelines::processors::transforms::aggregator::statistics::AggregationStatistics;
use crate::pipelines::processors::transforms::aggregator::statistics::FinalAggregateFinishMode;
use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;

const SPILL_BUCKET_NUM: usize = 4;
const SPILL_BUCKET_BITS: u64 = SPILL_BUCKET_NUM.trailing_zeros() as u64;
/// The bits of the group hash that partition a hash table; each spill depth takes the next
/// `SPILL_BUCKET_BITS` of them.
const PARTITION_HASH_BITS: u64 = 48;
/// The bits of the group hash the staged groups are bucketed by, the spill bucket bits first, see
/// `StagedGroups`.
const STAGED_BUCKET_BITS: u64 = 6;
/// The bytes of rows and index the hash table grows to before the groups are staged: a table up to
/// this size stays in the caches, beyond it each insert waits on memory.
const STAGE_TABLE_BYTES: usize = 4 << 20;

/// An input block.
enum FinalInput {
    /// Partial aggregate states of this node.
    Payloads(Vec<AggregatePayload>),
    /// Partial aggregate states serialized by another node.
    Serialized(Vec<SerializedPayload>),
    /// Raw rows the partial aggregate forwarded.
    Raw(DataBlock),
}

pub struct FinalAggregateTask {
    task_id: u64,
    spilled_depth: usize,
    spilled_payload: Vec<SpilledPayload>,
    tx: Sender<FinalAggregateTask>,
}

/// The groups still to output, a block per `process`.
enum PendingGroups {
    /// The payloads of a hash table.
    Payloads(VecDeque<Payload>),
    /// Staged groups, aggregated a bucket at a time.
    Staged(StagedOutput),
}

struct PendingOutput {
    groups: PendingGroups,
    stream: BlockConcatStream,
}

pub struct TransformFinalAggregate {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    input_data: Option<FinalInput>,
    channel_data: Option<FinalAggregateTask>,
    output_data: VecDeque<DataBlock>,
    pending_output: Option<PendingOutput>,
    should_finish: bool,
    /// Held while reading the input: the input finishes by taking it, see `finish`, after which
    /// the processor reads the tasks of the channel.
    tx: Option<Sender<FinalAggregateTask>>,
    rx: Receiver<FinalAggregateTask>,
    output_finished: WatchNotify,
    spilled_occurred: bool,
    /// How many times the table was output and reset under memory pressure, see
    /// `AggregatorParams::allow_duplicate_groups`.
    pressure_outputs: usize,

    // Created on the first non-empty input. With the global shuffle there are many final
    // processors and most of them may never receive rows, so an eagerly allocated
    // hash table (and its hash index) per processor is pure overhead.
    hashtable: Option<AggregateHashTable>,
    /// The groups, once the hash table outgrew the caches: see `stage_if_large`. At most one of
    /// `hashtable` and `staged` holds groups.
    staged: Option<StagedGroups>,
    params: Arc<AggregatorParams>,
    flush_state: PayloadFlushState,
    statistics: AggregationStatistics,
    id: usize,
    max_partition_depth: usize,
    current_partition_depth: usize,
    spiller: AggregateSpiller,
    settings: MemorySettings,
    max_aggregate_spill_level: usize,
    next_task_id: Arc<AtomicU64>,
}

impl TransformFinalAggregate {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: Arc<AggregatorParams>,
        id: usize,
        ctx: Arc<QueryContext>,
        tx: Sender<FinalAggregateTask>,
        rx: Receiver<FinalAggregateTask>,
        next_task_id: Arc<AtomicU64>,
    ) -> Result<Box<dyn Processor>> {
        let settings = ctx.get_settings();
        let available_partition_depths = (PARTITION_HASH_BITS / SPILL_BUCKET_BITS) as usize;
        let max_partition_depth = available_partition_depths.saturating_sub(1);
        let max_aggregate_spill_level =
            (settings.get_max_aggregate_spill_level()? as usize).min(max_partition_depth);

        let flush_state = PayloadFlushState::default();

        let spiller = AggregateSpiller::try_create(
            ctx.clone(),
            SPILL_BUCKET_NUM,
            params.states_schema(),
            // Every final processor buffers the blocks it spills on its own: halve the block
            // bytes to bound the buffers of all of them.
            0,
            params.max_block_bytes / 2,
        )?;

        Ok(Box::new(TransformFinalAggregate {
            input,
            output,
            input_data: None,
            channel_data: None,
            output_data: VecDeque::new(),
            pending_output: None,
            should_finish: false,
            tx: Some(tx),
            rx,
            output_finished: WatchNotify::new(),
            spilled_occurred: false,
            pressure_outputs: 0,
            hashtable: None,
            staged: None,
            params,
            flush_state,
            statistics: AggregationStatistics::new("FinalAggregate"),
            id,
            max_partition_depth,
            current_partition_depth: 0,
            spiller,
            settings: MemorySettings::from_aggregate_settings(&ctx)?,
            max_aggregate_spill_level,
            next_task_id,
        }))
    }
}

impl TransformFinalAggregate {
    fn next_task_id(&self) -> u64 {
        self.next_task_id.fetch_add(1, Ordering::Relaxed)
    }

    fn create_hashtable(
        params: &Arc<AggregatorParams>,
        spilled_depth: usize,
    ) -> AggregateHashTable {
        AggregateHashTable::new_with_partitioned_arenas(
            params.group_data_types.clone(),
            params.aggregate_functions.clone(),
            HashTableConfig::default()
                .with_initial_radix_bits(SPILL_BUCKET_BITS)
                .with_partition_start_bit(spilled_depth as u64 * SPILL_BUCKET_BITS),
        )
    }

    /// Drop the current hash table and switch to `partition_depth`. The next hash
    /// table is created by `hashtable_mut` when data arrives.
    fn reset_hashtable(&mut self, partition_depth: usize) {
        self.hashtable = None;
        self.staged = None;
        self.current_partition_depth = partition_depth;
    }

    /// Whether anything was aggregated since the last reset.
    fn has_groups(&self) -> bool {
        self.hashtable.is_some() || self.staged.is_some()
    }

    /// Stage the groups once the hash table outgrows the caches: from then on every input is
    /// kept by bucket of its group hash and aggregated a bucket at a time at the end, see
    /// `StagedGroups`. The buckets start at the bits of the current spill depth, so that staged
    /// groups spill by bucket. A table whose groups may repeat in the output is reset under
    /// memory pressure instead, and keeps aggregating on arrival.
    fn stage_if_large(&mut self) -> Result<()> {
        let start_bit = self.current_partition_depth as u64 * SPILL_BUCKET_BITS;
        if self.params.allow_duplicate_groups
            || !StagedGroups::fits(start_bit, STAGED_BUCKET_BITS)
            || self
                .hashtable
                .as_ref()
                .is_none_or(|ht| ht.table_bytes() <= STAGE_TABLE_BYTES)
        {
            return Ok(());
        }
        let hashtable = self.hashtable.take().unwrap();
        let mut staged = StagedGroups::new(
            self.params.group_data_types.clone(),
            self.params.aggregate_functions.clone(),
            start_bit,
            STAGED_BUCKET_BITS,
        );
        staged.add_table(hashtable, &mut self.flush_state)?;
        self.staged = Some(staged);
        Ok(())
    }

    // Takes the fields separately so callers can still borrow `flush_state`.
    fn hashtable_mut<'a>(
        hashtable: &'a mut Option<AggregateHashTable>,
        params: &Arc<AggregatorParams>,
        partition_depth: usize,
    ) -> &'a mut AggregateHashTable {
        hashtable.get_or_insert_with(|| Self::create_hashtable(params, partition_depth))
    }

    // One final-aggregate processor handles both the original input stream and
    // recursively spilled tasks received from the channel. Those tasks may
    // belong to different spill depths, and each depth must consume a different
    // hash-bit window when building the internal partitions. The window start is
    // `spilled_depth * SPILL_BUCKET_BITS`.
    fn ensure_spill_depth(&mut self, spilled_depth: usize) {
        let partition_depth = spilled_depth.min(self.max_partition_depth);
        if self.current_partition_depth != partition_depth {
            self.reset_hashtable(partition_depth);
        }
    }

    fn handle_serialized(
        &mut self,
        payload: SerializedPayload,
        need_check_spill: bool,
    ) -> Result<()> {
        if payload.data_block.is_empty() {
            return Ok(());
        }

        let rows = payload.data_block.num_rows();
        let bytes = payload.data_block.memory_size();
        self.statistics.record_block(rows, bytes);

        self.merge_serialized(payload)?;
        self.check_spill(need_check_spill)
    }

    fn handle_raw(&mut self, block: DataBlock, need_check_spill: bool) -> Result<()> {
        let rows = block.num_rows();
        self.statistics.record_block(rows, block.memory_size());

        if rows != 0 {
            let group_columns = ProjectedBlock::project(&self.params.group_columns, &block);
            let params = self
                .params
                .aggregate_functions_arguments
                .iter()
                .map(|args| ProjectedBlock::project(args, &block))
                .collect::<Vec<_>>();
            if let Some(staged) = self.staged.as_mut() {
                staged.add_rows(
                    &mut self.flush_state,
                    group_columns,
                    &params,
                    (&[]).into(),
                    rows,
                )?;
            } else {
                let ht = Self::hashtable_mut(
                    &mut self.hashtable,
                    &self.params,
                    self.current_partition_depth,
                );
                ht.add_raw_groups(&mut self.flush_state, group_columns, &params, rows)?;
                self.stage_if_large()?;
            }
        }
        self.check_spill(need_check_spill)
    }

    /// Merge the serialized states straight into the hash table: a serialized block holds the
    /// states, then the group keys.
    fn merge_serialized(&mut self, payload: SerializedPayload) -> Result<()> {
        let block = &payload.data_block;
        let num_states = self.params.num_states();
        let states = (0..num_states).collect::<Vec<_>>();
        let groups =
            (num_states..num_states + self.params.group_data_types.len()).collect::<Vec<_>>();
        let groups = ProjectedBlock::project(&groups, block);
        let states = ProjectedBlock::project(&states, block);
        if let Some(staged) = self.staged.as_mut() {
            return staged.add_rows(&mut self.flush_state, groups, &[], states, block.num_rows());
        }
        let ht = Self::hashtable_mut(
            &mut self.hashtable,
            &self.params,
            self.current_partition_depth,
        );
        ht.add_serialized_groups(&mut self.flush_state, groups, states, block.num_rows())?;
        self.stage_if_large()
    }

    fn handle_aggregate_payload(
        &mut self,
        payload: AggregatePayload,
        need_check_spill: bool,
    ) -> Result<()> {
        let rows = payload.payload.len();
        let bytes = payload.payload.memory_size();
        self.statistics.record_block(rows, bytes);

        if rows != 0 {
            if let Some(staged) = self.staged.as_mut() {
                staged.add_payload(payload.payload, &mut self.flush_state)?;
            } else {
                let ht = Self::hashtable_mut(
                    &mut self.hashtable,
                    &self.params,
                    self.current_partition_depth,
                );
                ht.combine_payload(&payload.payload, &mut self.flush_state)?;
                self.stage_if_large()?;
            }
        }

        self.check_spill(need_check_spill)
    }

    fn check_spill(&mut self, need_check_spill: bool) -> Result<()> {
        if self.params.allow_duplicate_groups {
            // The consumer merges repeated groups, so output the groups aggregated so far rather
            // than spilling them and merging them back at the end. Checked whatever the spill
            // level: this is what bounds the memory of the table.
            if self.has_groups() && self.settings.check_spill() {
                let (rows, _) = self.output_hashtable(Some(self.output_stream()))?;
                self.pressure_outputs += 1;
                // The pressure may not come from this table: then the table is reset on
                // every input, which these logs make visible.
                log::info!(
                    "Final aggregate output its groups under memory pressure: processor={}, rows={}, outputs_so_far={}",
                    self.id,
                    rows,
                    self.pressure_outputs,
                );
            }
            return Ok(());
        }

        // Once a task spills, all remaining input must follow it so a group cannot
        // be finalized separately from its already spilled states.
        if self.spilled_occurred || (need_check_spill && self.settings.check_spill()) {
            self.spill_out()?;
        }

        Ok(())
    }

    /// Coalesces the output blocks of one hash table to the block size of the aggregate.
    fn output_stream(&self) -> BlockConcatStream {
        BlockConcatStream::create(self.params.max_block_rows, self.params.max_block_bytes)
    }

    fn handle_input(&mut self, input: FinalInput, need_check_spill: bool) -> Result<()> {
        match input {
            FinalInput::Payloads(payloads) => {
                for payload in payloads {
                    check_interrupt()?;
                    self.handle_aggregate_payload(payload, need_check_spill)?;
                }
            }
            FinalInput::Serialized(payloads) => {
                for payload in payloads {
                    check_interrupt()?;
                    self.handle_serialized(payload, need_check_spill)?;
                }
            }
            FinalInput::Raw(block) => self.handle_raw(block, need_check_spill)?,
        }
        Ok(())
    }

    fn spill_out(&mut self) -> Result<()> {
        self.spilled_occurred = true;
        // No hash table means nothing was aggregated since the last reset.
        if let Some(v) = self.hashtable.take() {
            for (bucket, payload) in v.payload.into_non_empty_bucket_payloads() {
                check_interrupt()?;
                let data_block = payload.aggregate_flush_all()?.consume_convert_to_full();
                self.spiller.spill(bucket, data_block)?;
            }
        }
        if let Some(staged) = self.staged.take() {
            // Each bucket is aggregated, and its groups spilled to its spill bucket: the first
            // bits of a staged bucket.
            let mut output = staged.into_output(true);
            let shift = output.bucket_bits() - SPILL_BUCKET_BITS;
            while let Some((bucket, block)) = output.next_block(&mut self.flush_state)? {
                check_interrupt()?;
                self.spiller
                    .spill(bucket >> shift, block.consume_convert_to_full())?;
            }
        }
        self.reset_hashtable(self.current_partition_depth);
        Ok(())
    }

    fn finish(
        &mut self,
        task_id: Option<u64>,
        spilled_depth: usize,
        tx: Sender<FinalAggregateTask>,
    ) -> Result<()> {
        let pending_blocks = if self.spilled_occurred {
            self.spill_out()?;
            self.spiller.take_pending_if_unspilled()
        } else {
            None
        };

        let (output_rows, hash_index_resizes, finish_mode) = if let Some(pending_blocks) =
            pending_blocks
        {
            // Disjoint buckets can be output and released one at a time without
            // another spill check or buffering results to form larger blocks.
            let mut output_rows = 0;
            let mut hash_index_resizes = 0;
            for (bucket, data_block) in pending_blocks {
                self.ensure_spill_depth(spilled_depth);
                // These states were already counted as input.
                self.merge_serialized(SerializedPayload {
                    bucket: bucket as isize,
                    data_block,
                })?;
                let (rows, resizes) = self.output_hashtable(None)?;
                output_rows += rows;
                hash_index_resizes += resizes;
            }
            (
                output_rows,
                hash_index_resizes,
                FinalAggregateFinishMode::Buffered,
            )
        } else if self.spilled_occurred {
            // `spill_out` above took the hash table.
            self.spill_finish(spilled_depth, tx)?;
            (0, 0, FinalAggregateFinishMode::Spilled)
        } else {
            let output_stream = self.output_stream();
            let (output_rows, hash_index_resizes) = self.start_streaming_output(output_stream)?;
            (
                output_rows,
                hash_index_resizes,
                FinalAggregateFinishMode::InMemory,
            )
        };

        self.statistics.log_final_finish_statistics(
            task_id,
            self.id,
            spilled_depth,
            output_rows,
            hash_index_resizes,
            finish_mode,
        );
        self.spilled_occurred = false;
        Ok(())
    }

    fn output_hashtable(
        &mut self,
        mut output_stream: Option<BlockConcatStream>,
    ) -> Result<(usize, usize)> {
        // Nothing to output if nothing was aggregated.
        let Some((mut groups, output_rows, hash_index_resizes)) = self.take_groups() else {
            return Ok((0, 0));
        };

        // With a top N above, only the first rows of each table are needed: the groups of
        // different tables are distinct.
        let mut top_n_blocks = Vec::new();
        loop {
            check_interrupt()?;
            let Some(block) = Self::next_group_block(&mut groups, &mut self.flush_state)? else {
                break;
            };
            if let Some((order_by, limit)) = &self.params.top_n {
                top_n_blocks.push(block);
                if top_n_blocks.len() >= TOP_N_PRUNE_BLOCKS {
                    top_n_blocks = vec![sort_blocks(&top_n_blocks, order_by, *limit)?];
                }
            } else if let Some(output_stream) = output_stream.as_mut() {
                if let Some(block) = output_stream.push(block) {
                    self.output_data.push_back(block);
                }
            } else {
                self.output_data.push_back(block);
            }
        }
        if let Some((order_by, limit)) = &self.params.top_n
            && !top_n_blocks.is_empty()
        {
            self.output_data
                .push_back(sort_blocks(&top_n_blocks, order_by, *limit)?);
        }
        if let Some(block) = output_stream.and_then(|mut stream| stream.finish()) {
            self.output_data.push_back(block);
        }
        Ok((output_rows, hash_index_resizes))
    }

    fn start_streaming_output(&mut self, stream: BlockConcatStream) -> Result<(usize, usize)> {
        if self.params.top_n.is_some() {
            return self.output_hashtable(Some(stream));
        }
        // Nothing to output if nothing was aggregated.
        let Some((groups, output_rows, hash_index_resizes)) = self.take_groups() else {
            return Ok((0, 0));
        };
        self.pending_output = Some(PendingOutput { groups, stream });
        Ok((output_rows, hash_index_resizes))
    }

    /// The groups aggregated since the last reset, to output: the payloads of the hash table, its
    /// index dropped, or the staged groups. With the rows taken and the resizes of the index:
    /// staged groups are counted by their rows, and have no index to resize.
    fn take_groups(&mut self) -> Option<(PendingGroups, usize, usize)> {
        self.flush_state.clear();
        if let Some(staged) = self.staged.take() {
            let rows = staged.len();
            return Some((PendingGroups::Staged(staged.into_output(false)), rows, 0));
        }
        let hashtable = self.hashtable.take()?;
        let output_rows = hashtable.payload.len();
        let hash_index_resizes = hashtable.hash_index_resize_count();
        // Consuming the hash table drops its index before result materialization.
        Some((
            PendingGroups::Payloads(hashtable.into_payloads().into()),
            output_rows,
            hash_index_resizes,
        ))
    }

    /// The next block of results of `groups`, `None` once all were output.
    fn next_group_block(
        groups: &mut PendingGroups,
        flush_state: &mut PayloadFlushState,
    ) -> Result<Option<DataBlock>> {
        match groups {
            PendingGroups::Payloads(payloads) => {
                while let Some(payload) = payloads.front() {
                    if let Some(block) = payload.merge_result(flush_state)? {
                        return Ok(Some(block));
                    }
                    // Each payload owns its arena, so dropping it releases its states.
                    payloads.pop_front();
                    flush_state.clear();
                }
                Ok(None)
            }
            PendingGroups::Staged(output) => {
                Ok(output.next_block(flush_state)?.map(|(_, block)| block))
            }
        }
    }

    fn output_next_block(&mut self) -> Result<()> {
        let Some(pending) = self.pending_output.as_mut() else {
            return Ok(());
        };
        loop {
            check_interrupt()?;
            let Some(block) = Self::next_group_block(&mut pending.groups, &mut self.flush_state)?
            else {
                break;
            };
            if let Some(block) = pending.stream.push(block) {
                self.output_data.push_back(block);
                return Ok(());
            }
        }
        if let Some(mut pending) = self.pending_output.take()
            && let Some(block) = pending.stream.finish()
        {
            self.output_data.push_back(block);
        }
        Ok(())
    }

    fn spill_finish(&mut self, spilled_depth: usize, tx: Sender<FinalAggregateTask>) -> Result<()> {
        let spilled_payload = self.spiller.spill_finish()?;
        let mut chunks = (0..SPILL_BUCKET_NUM).map(|_| vec![]).collect::<Vec<_>>();
        for payload in spilled_payload.into_iter() {
            chunks[payload.bucket as usize].push(payload);
        }

        let next_spill_depth = spilled_depth + 1;
        for (bucket, chunk) in chunks.into_iter().enumerate() {
            let task_id = self.next_task_id();
            let rows = chunk
                .iter()
                .map(|payload| payload.row_group.num_rows() as usize)
                .sum::<usize>();
            log::info!(
                "Spill finish emitted task: task_id={}, processor={}, spill_depth={}, bucket={}, payloads={}, rows={}",
                task_id,
                self.id,
                spilled_depth,
                bucket,
                chunk.len(),
                rows,
            );
            let spilled = FinalAggregateTask {
                task_id,
                spilled_depth: next_spill_depth,
                spilled_payload: chunk,
                tx: tx.clone(),
            };
            tx.try_send(spilled).map_err(|e| {
                ErrorCode::Internal(format!(
                    "Failed to send final aggregate meta to spiller: {}",
                    e
                ))
            })?;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl Processor for TransformFinalAggregate {
    fn name(&self) -> String {
        "TransformFinalAggregate".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            let _ = self.tx.take();
            self.output_data.clear();
            self.pending_output = None;
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.pop_front() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        // Nothing is read until the pending output is all output.
        if self.pending_output.is_some() {
            return Ok(Event::Sync);
        }

        if self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.channel_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;
            self.input_data = match data_block.take_meta() {
                None => (data_block.num_rows() != 0).then_some(FinalInput::Raw(data_block)),
                Some(meta) if AggregateStatesMeta::downcast_ref_from(&meta).is_some() => {
                    let states = AggregateStatesMeta::downcast_from(meta).unwrap();
                    Some(FinalInput::Serialized(states.into_payloads(data_block)?))
                }
                Some(meta) => match AggregateMeta::downcast_from(meta) {
                    Some(meta) => Some(FinalInput::Payloads(meta.payloads)),
                    None => {
                        return Err(ErrorCode::Internal(
                            "TransformFinalAggregate only accepts aggregate states or raw rows",
                        ));
                    }
                },
            };
            if self.input_data.is_some() {
                return Ok(Event::Sync);
            }
        }

        if self.input.is_finished() {
            if self.tx.is_some() {
                // Finish the input, then read the tasks of the channel.
                return Ok(Event::Sync);
            }

            if !self.should_finish {
                return Ok(Event::Async);
            }
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if self.pending_output.is_some() {
            return self.output_next_block();
        }

        let input_data = self.input_data.take();
        if let Some(input) = input_data {
            self.ensure_spill_depth(0);
            let need_check_spill = self.max_aggregate_spill_level > 0;
            self.handle_input(input, need_check_spill)?;
            return Ok(());
        } else if let Some(mut task) = self.channel_data.take() {
            self.ensure_spill_depth(task.spilled_depth);
            self.statistics.reset();
            let need_check_spill = task.spilled_depth < self.max_aggregate_spill_level;
            for payload in mem::take(&mut task.spilled_payload) {
                check_interrupt()?;
                let restored = self.spiller.restore(payload)?;
                self.handle_serialized(restored, need_check_spill)?;
            }
            self.finish(Some(task.task_id), task.spilled_depth, task.tx)?;
        } else {
            self.ensure_spill_depth(0);
            let sender = mem::take(&mut self.tx)
                .expect("logic error: called finished for input data more than once");
            self.finish(None, 0, sender)?;
        }

        // Output the first block of the hash table `finish` took, if any.
        self.output_next_block()
    }

    fn un_reacted(&self, cause: EventCause, _id: usize) -> Result<()> {
        if matches!(cause, EventCause::Output(_)) && self.output.is_finished() {
            // event() cannot run while async_process() is waiting. Wake only
            // this processor: peers may still need the shared task channel.
            self.output_finished.notify_waiters();
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        tokio::select! {
            biased;
            _ = self.output_finished.notified() => {}
            task = self.rx.recv() => {
                match task {
                    Ok(meta) => self.channel_data = Some(meta),
                    Err(_) => self.should_finish = true,
                }
            }
        }

        Ok(())
    }
}

/// The blocks of a top N are reduced to its first rows once there are this many.
const TOP_N_PRUNE_BLOCKS: usize = 32;

/// The first `limit` rows of `blocks` ordered by `order_by`.
fn sort_blocks(
    blocks: &[DataBlock],
    order_by: &[SortColumnDescription],
    limit: usize,
) -> Result<DataBlock> {
    DataBlock::sort(&DataBlock::concat(blocks)?, order_by, Some(limit))
}

#[cfg(test)]
mod tests {
    use bumpalo::Bump;
    use databend_common_expression::DataField;
    use databend_common_expression::DataSchema;
    use databend_common_expression::FromData;
    use databend_common_expression::types::AccessType;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::UInt64Type;
    use databend_common_pipeline::core::ProcessorPtr;
    use databend_common_pipeline::core::port::connect;
    use futures::FutureExt;

    use super::*;
    use crate::test_kits::TestFixture;

    fn create_params() -> Result<Arc<AggregatorParams>> {
        let key_type = DataType::Number(NumberDataType::UInt64);
        AggregatorParams::try_create(
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
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_finished_output_interrupts_final_aggregate_receive() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let (tx, rx) = async_channel::unbounded();
        let params = create_params()?;
        let input = InputPort::create();
        let output = OutputPort::create();
        let downstream = InputPort::create();
        // The ports are not yet used by a processor or executor.
        unsafe { connect(&downstream, &output) };
        downstream.set_need_data();
        input.finish();
        let mut processor = TransformFinalAggregate::try_create(
            input,
            output,
            params,
            0,
            ctx,
            tx.clone(),
            rx,
            Arc::new(AtomicU64::new(1)),
        )?;
        assert!(matches!(processor.event()?, Event::Sync));
        processor.process()?;
        assert!(matches!(processor.event()?, Event::Async));
        let processor = ProcessorPtr::create(processor);

        // Match executor scheduling: poll sequentially and invoke only the
        // thread-safe un_reacted hook while async_process is pending.
        let mut task = unsafe { processor.async_process() };
        assert!(futures::poll!(&mut task).is_pending());
        downstream.finish();
        unsafe { processor.un_reacted(EventCause::Output(0))? };
        task.now_or_never()
            .expect("a finished output must wake the receiver even with a live sender")?;
        assert!(matches!(
            unsafe { processor.event(EventCause::Other)? },
            Event::Finished
        ));
        drop(tx);
        Ok(())
    }

    // Final processors that only receive empty payloads must not allocate a hash table.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_final_aggregate_without_rows_creates_no_hashtable() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let (tx, rx) = async_channel::unbounded();
        let params = create_params()?;
        let upstream = OutputPort::create();
        let input = InputPort::create();
        let output = OutputPort::create();
        let downstream = InputPort::create();
        // The ports are not yet used by a processor or executor.
        unsafe {
            connect(&input, &upstream);
            connect(&downstream, &output);
        }
        downstream.set_need_data();
        input.set_need_data();

        let empty_payloads = AggregateHashTable::new(
            params.group_data_types.clone(),
            params.aggregate_functions.clone(),
            HashTableConfig::default().with_initial_radix_bits(2),
            Arc::new(Bump::new()),
        )
        .payload
        .into_bucket_payloads()
        .map(|(bucket, payload)| AggregatePayload {
            bucket: bucket as isize,
            payload,
        })
        .collect::<Vec<_>>();
        upstream.push_data(Ok(AggregateMeta::payloads_block(empty_payloads)));
        upstream.finish();

        let mut processor = TransformFinalAggregate::try_create(
            input,
            output,
            params,
            0,
            ctx,
            tx.clone(),
            rx,
            Arc::new(AtomicU64::new(1)),
        )?;
        let has_hashtable = |processor: &mut Box<dyn Processor>| {
            processor
                .as_any()
                .downcast_mut::<TransformFinalAggregate>()
                .unwrap()
                .hashtable
                .is_some()
        };
        assert!(!has_hashtable(&mut processor));

        // Merge the empty payloads.
        assert!(matches!(processor.event()?, Event::Sync));
        processor.process()?;
        assert!(!has_hashtable(&mut processor));

        // Finish the input stage: nothing to output, then wait on the task channel.
        assert!(matches!(processor.event()?, Event::Sync));
        processor.process()?;
        assert!(!has_hashtable(&mut processor));
        assert!(matches!(processor.event()?, Event::Async));
        assert!(!downstream.has_data());
        drop(tx);
        Ok(())
    }

    // With `allow_duplicate_groups`, memory pressure makes the final aggregate output the groups
    // it holds, so a group seen again is output again, and nothing is ever spilled.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_allow_duplicate_groups_outputs_under_memory_pressure() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        // Every memory check reports pressure.
        ctx.get_settings()
            .set_setting("force_aggregate_data_spill".to_string(), "1".to_string())?;
        let (tx, rx) = async_channel::unbounded();
        let mut params = create_params()?;
        Arc::get_mut(&mut params).unwrap().allow_duplicate_groups = true;
        let upstream = OutputPort::create();
        let input = InputPort::create();
        let output = OutputPort::create();
        let downstream = InputPort::create();
        // The ports are not yet used by a processor or executor.
        unsafe {
            connect(&input, &upstream);
            connect(&downstream, &output);
        }
        input.set_need_data();

        let mut processor = TransformFinalAggregate::try_create(
            input,
            output,
            params,
            0,
            ctx,
            tx.clone(),
            rx,
            Arc::new(AtomicU64::new(1)),
        )?;
        let keys = || DataBlock::new_from_columns(vec![UInt64Type::from_data(vec![1u64, 2])]);

        let mut output_rows = 0;
        for _ in 0..2 {
            downstream.set_need_data();
            upstream.push_data(Ok(keys()));
            assert!(matches!(processor.event()?, Event::Sync));
            processor.process()?;
            // The groups of this block were output right away.
            assert!(matches!(processor.event()?, Event::NeedConsume));
            output_rows += downstream.pull_data().unwrap()?.num_rows();
        }
        assert_eq!(output_rows, 4, "each block outputs the same two groups");

        downstream.set_need_data();
        upstream.finish();
        // Finish the input stage: no retained groups, no spilled task to wait for.
        assert!(matches!(processor.event()?, Event::Sync));
        processor.process()?;
        assert!(!downstream.has_data());
        let final_aggregate = processor
            .as_any()
            .downcast_mut::<TransformFinalAggregate>()
            .unwrap();
        assert!(!final_aggregate.spilled_occurred);
        assert!(final_aggregate.hashtable.is_none());
        assert_eq!(final_aggregate.pressure_outputs, 2);
        drop(tx);
        Ok(())
    }

    // A table that outgrows the caches is staged by bucket: every group is still output once,
    // also when it came again after the groups were staged, and the output stays a block per
    // process.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_final_aggregate_stages_large_tables() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let (tx, rx) = async_channel::unbounded();
        let params = create_params()?;
        let upstream = OutputPort::create();
        let input = InputPort::create();
        let output = OutputPort::create();
        let downstream = InputPort::create();
        // The ports are not yet used by a processor or executor.
        unsafe {
            connect(&input, &upstream);
            connect(&downstream, &output);
        }
        input.set_need_data();

        let mut processor = TransformFinalAggregate::try_create(
            input,
            output,
            params,
            0,
            ctx,
            tx.clone(),
            rx,
            Arc::new(AtomicU64::new(1)),
        )?;
        let groups = 400_000u64;
        let mut output_keys = Vec::new();
        let mut staged = false;
        // Each block holds every group, in another order.
        for round in 0..3u64 {
            let keys = (0..groups)
                .map(|i| (i * 7919 + round * 104_729) % groups)
                .collect::<Vec<_>>();
            downstream.set_need_data();
            upstream.push_data(Ok(DataBlock::new_from_columns(vec![
                UInt64Type::from_data(keys),
            ])));
            assert!(matches!(processor.event()?, Event::Sync));
            processor.process()?;
            // Nothing is output before the input finishes.
            assert!(matches!(processor.event()?, Event::NeedData));
            staged |= processor
                .as_any()
                .downcast_mut::<TransformFinalAggregate>()
                .unwrap()
                .staged
                .is_some();
        }
        assert!(staged, "a table of {groups} groups outgrows the caches");
        upstream.finish();

        loop {
            downstream.set_need_data();
            match processor.event()? {
                Event::Sync => {
                    processor.process()?;
                    let final_aggregate = processor
                        .as_any()
                        .downcast_mut::<TransformFinalAggregate>()
                        .unwrap();
                    assert!(final_aggregate.output_data.len() <= 1);
                }
                Event::NeedConsume => {
                    let block = downstream.pull_data().unwrap()?;
                    let column = block.get_by_offset(0).to_column();
                    output_keys.extend(
                        UInt64Type::try_downcast_column(&column)
                            .unwrap()
                            .iter()
                            .copied(),
                    );
                }
                // All output: waiting on the task channel.
                Event::Async => break,
                event => panic!("unexpected event {event:?}"),
            }
        }
        output_keys.sort_unstable();
        assert_eq!(output_keys, (0..groups).collect::<Vec<_>>());
        drop(tx);
        Ok(())
    }

    // The groups are output a block at a time: a block is built only once the previous one was
    // pushed, so the blocks of all the groups are never held at once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_final_aggregate_outputs_one_block_per_process() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let (tx, rx) = async_channel::unbounded();
        // Blocks of 1024 rows.
        let params = create_params()?;
        let upstream = OutputPort::create();
        let input = InputPort::create();
        let output = OutputPort::create();
        let downstream = InputPort::create();
        // The ports are not yet used by a processor or executor.
        unsafe {
            connect(&input, &upstream);
            connect(&downstream, &output);
        }
        input.set_need_data();

        let mut processor = TransformFinalAggregate::try_create(
            input,
            output,
            params,
            0,
            ctx,
            tx.clone(),
            rx,
            Arc::new(AtomicU64::new(1)),
        )?;
        let groups = 10_000u64;
        upstream.push_data(Ok(DataBlock::new_from_columns(vec![
            UInt64Type::from_data((0..groups).collect::<Vec<_>>()),
        ])));
        upstream.finish();

        let mut output_rows = 0;
        let mut output_blocks = 0;
        loop {
            downstream.set_need_data();
            match processor.event()? {
                Event::Sync => {
                    processor.process()?;
                    let final_aggregate = processor
                        .as_any()
                        .downcast_mut::<TransformFinalAggregate>()
                        .unwrap();
                    assert!(final_aggregate.output_data.len() <= 1);
                }
                Event::NeedConsume => {
                    output_rows += downstream.pull_data().unwrap()?.num_rows();
                    output_blocks += 1;
                }
                // All output: waiting on the task channel.
                Event::Async => break,
                event => panic!("unexpected event {event:?}"),
            }
        }
        assert_eq!(output_rows as u64, groups);
        assert!(output_blocks > 1);
        drop(tx);
        Ok(())
    }
}
