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
use std::mem;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use async_channel::Receiver;
use async_channel::Sender;
use bumpalo::Bump;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::HashTableConfig;
use databend_common_expression::PayloadFlushState;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline_transforms::MemorySettings;

use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatePayload;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::LocalPartitionStream;
use crate::pipelines::processors::transforms::aggregator::NewAggregateSpiller;
use crate::pipelines::processors::transforms::aggregator::NewSpilledPayload;
use crate::pipelines::processors::transforms::aggregator::SerializedPayload;
use crate::pipelines::processors::transforms::aggregator::statistics::AggregationStatistics;
use crate::pipelines::processors::transforms::aggregator::transform_aggregate_partial::HashTable;
use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;

const SPILL_BUCKET_NUM: usize = 2;
const SPILL_BUCKET_BITS: u64 = SPILL_BUCKET_NUM.trailing_zeros() as u64;

enum Stage {
    Input,
    Channel,
}

pub struct FinalAggregateTask {
    task_id: u64,
    spilled_depth: usize,
    spilled_payload: Vec<NewSpilledPayload>,
    tx: Sender<FinalAggregateTask>,
}

pub struct NewTransformFinalAggregate {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    input_data: Option<AggregateMeta>,
    channel_data: Option<FinalAggregateTask>,
    should_finish: bool,
    tx: Option<Sender<FinalAggregateTask>>,
    rx: Receiver<FinalAggregateTask>,
    stage: Stage,
    spilled_occurred: bool,

    hashtable: HashTable,
    params: Arc<AggregatorParams>,
    flush_state: PayloadFlushState,
    statistics: AggregationStatistics,
    _id: usize,
    base_consumed_bits: u64,
    max_partition_depth: usize,
    current_partition_depth: usize,
    spiller: NewAggregateSpiller<LocalPartitionStream>,
    settings: MemorySettings,
    max_aggregate_spill_level: usize,
    next_task_id: Arc<AtomicU64>,
}

impl NewTransformFinalAggregate {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: Arc<AggregatorParams>,
        _id: usize,
        base_consumed_bits: u64,
        ctx: Arc<QueryContext>,
        tx: Sender<FinalAggregateTask>,
        rx: Receiver<FinalAggregateTask>,
        next_task_id: Arc<AtomicU64>,
    ) -> Result<Box<dyn Processor>> {
        let settings = ctx.get_settings();
        let available_partition_depths =
            (48_u64.saturating_sub(base_consumed_bits) / SPILL_BUCKET_BITS) as usize;
        let max_partition_depth = available_partition_depths.saturating_sub(1);
        let max_aggregate_spill_level =
            (settings.get_max_aggregate_spill_level()? as usize).min(max_partition_depth);

        let hashtable = Self::create_hashtable(&params, base_consumed_bits, 0);
        let flush_state = PayloadFlushState::default();

        let spiller = NewAggregateSpiller::try_create(
            ctx.clone(),
            SPILL_BUCKET_NUM,
            params.spill_schema(),
            LocalPartitionStream::new(
                params.max_block_rows,
                params.max_block_bytes,
                SPILL_BUCKET_NUM,
            ),
        )?;

        Ok(Box::new(NewTransformFinalAggregate {
            input,
            output,
            input_data: None,
            channel_data: None,
            should_finish: false,
            tx: Some(tx),
            rx,
            stage: Stage::Input,
            spilled_occurred: false,
            hashtable: HashTable::AggregateHashTable(hashtable),
            params,
            flush_state,
            statistics: AggregationStatistics::new("NewFinalAggregate"),
            _id,
            base_consumed_bits,
            max_partition_depth,
            current_partition_depth: 0,
            spiller,
            settings: MemorySettings::from_aggregate_settings(&ctx)?,
            max_aggregate_spill_level,
            next_task_id,
        }))
    }
}

impl NewTransformFinalAggregate {
    fn next_task_id(&self) -> u64 {
        self.next_task_id.fetch_add(1, Ordering::Relaxed)
    }

    fn partition_start_bit(base_consumed_bits: u64, spilled_depth: usize) -> u64 {
        base_consumed_bits + spilled_depth as u64 * SPILL_BUCKET_BITS
    }

    fn create_hashtable(
        params: &Arc<AggregatorParams>,
        base_consumed_bits: u64,
        spilled_depth: usize,
    ) -> AggregateHashTable {
        AggregateHashTable::new(
            params.group_data_types.clone(),
            params.aggregate_functions.clone(),
            HashTableConfig::default()
                .with_initial_radix_bits(SPILL_BUCKET_BITS)
                .with_partition_start_bit(Self::partition_start_bit(
                    base_consumed_bits,
                    spilled_depth,
                ))
                .with_experiment_hash_index(params.enable_experiment_hash_index),
            Arc::new(Bump::new()),
        )
    }

    fn reset_hashtable(&mut self, spilled_depth: usize) {
        let partition_depth = spilled_depth.min(self.max_partition_depth);
        self.hashtable = HashTable::AggregateHashTable(Self::create_hashtable(
            &self.params,
            self.base_consumed_bits,
            partition_depth,
        ));
        self.current_partition_depth = partition_depth;
    }

    // One final-aggregate processor handles both the original input stream and
    // recursively spilled tasks received from the channel. Those tasks may
    // belong to different spill depths, and each depth must consume a different
    // hash-bit window when building the internal partitions. The window start is
    // `base_consumed_bits + spilled_depth * SPILL_BUCKET_BITS`.
    fn ensure_spill_depth(&mut self, spilled_depth: usize) {
        let partition_depth = spilled_depth.min(self.max_partition_depth);
        if self.current_partition_depth != partition_depth {
            self.reset_hashtable(spilled_depth);
        }
    }

    fn handle_serialized(&mut self, payload: SerializedPayload) -> Result<()> {
        if payload.data_block.is_empty() {
            return Ok(());
        }

        let rows = payload.data_block.num_rows();
        let bytes = payload.data_block.memory_size();
        self.statistics.record_block(rows, bytes);

        let partitioned_payload = payload.convert_to_partitioned_payload(
            self.params.group_data_types.clone(),
            self.params.aggregate_functions.clone(),
            self.params.num_states(),
            0,
            self.params.enable_experiment_hash_index,
            Arc::new(Bump::new()),
        )?;

        if let HashTable::AggregateHashTable(ht) = &mut self.hashtable {
            ht.combine_payloads(&partitioned_payload, &mut self.flush_state)?;
        }

        Ok(())
    }

    fn handle_aggregate_payload(&mut self, payload: AggregatePayload) -> Result<()> {
        let rows = payload.payload.len();
        let bytes = payload.payload.memory_size();
        self.statistics.record_block(rows, bytes);

        if let HashTable::AggregateHashTable(ht) = &mut self.hashtable {
            ht.combine_payload(&payload.payload, &mut self.flush_state)?;
        }

        Ok(())
    }

    fn handle_meta(&mut self, meta: AggregateMeta, need_check_spill: bool) -> Result<()> {
        match meta {
            AggregateMeta::Serialized(payload) => {
                self.handle_serialized(payload)?;
            }
            AggregateMeta::AggregatePayload(payload) => {
                self.handle_aggregate_payload(payload)?;
            }
            AggregateMeta::NewSpilled(payloads) => {
                for payload in payloads {
                    let restored = self.spiller.restore(payload)?;
                    self.handle_meta(restored, need_check_spill)?;
                }
                return Ok(());
            }
            AggregateMeta::NewBucketSpilled(payload) => {
                let restored = self.spiller.restore(payload)?;
                self.handle_meta(restored, need_check_spill)?;
                return Ok(());
            }
            AggregateMeta::Partitioned { bucket: _, data } => {
                for meta in data {
                    self.handle_meta(meta, need_check_spill)?;
                }
                return Ok(());
            }
            _ => {
                unreachable!("unexpected aggregate meta, found type: {:?}", meta);
            }
        }

        // If already trigger spilled for this task, we continue to spill the remaining part
        if self.spilled_occurred || (need_check_spill && self.settings.check_spill()) {
            self.spill_out()?;
        }

        Ok(())
    }

    fn spill_out(&mut self) -> Result<()> {
        self.spilled_occurred = true;
        if let HashTable::AggregateHashTable(v) = mem::take(&mut self.hashtable) {
            for (bucket, payload) in v.payload.payloads.into_iter().enumerate() {
                if payload.len() == 0 {
                    continue;
                }

                let data_block = payload.aggregate_flush_all()?.consume_convert_to_full();
                self.spiller.spill(bucket, data_block)?;
            }
        } else {
            unreachable!("[TRANSFORM-AGGREGATOR] Invalid hash table state during spill check")
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
        if self.spilled_occurred {
            let (output_rows, hash_index_resizes) = match &self.hashtable {
                HashTable::AggregateHashTable(ht) => {
                    (ht.payload.len(), ht.hash_index_resize_count())
                }
                _ => unreachable!("[TRANSFORM-AGGREGATOR] Invalid hash table state before spill"),
            };
            self.spill_finish(spilled_depth, tx)?;
            if let Some(task_id) = task_id {
                self.statistics.log_task_finish_statistics(
                    task_id,
                    self._id,
                    spilled_depth,
                    output_rows,
                    hash_index_resizes,
                    true,
                );
            } else {
                self.statistics.reset();
            }

            self.spilled_occurred = false;
            return Ok(());
        }

        if let HashTable::AggregateHashTable(mut ht) = mem::take(&mut self.hashtable) {
            let mut blocks = vec![];
            self.flush_state.clear();

            loop {
                if ht.merge_result(&mut self.flush_state)? {
                    let mut entries = self.flush_state.take_aggregate_results();
                    let group_columns = self.flush_state.take_group_columns();
                    entries.extend_from_slice(&group_columns);
                    let num_rows = entries[0].len();
                    blocks.push(DataBlock::new(entries, num_rows));
                } else {
                    break;
                }
            }

            if !blocks.is_empty() {
                let concat = DataBlock::concat(&blocks)?;
                if !concat.is_empty() {
                    self.output.push_data(Ok(concat));
                }
            }
            if let Some(task_id) = task_id {
                self.statistics.log_task_finish_statistics(
                    task_id,
                    self._id,
                    spilled_depth,
                    ht.payload.len(),
                    ht.hash_index_resize_count(),
                    false,
                );
            } else {
                self.statistics.log_finish_statistics(&ht);
            }
            self.reset_hashtable(self.current_partition_depth);
        }

        Ok(())
    }

    fn spill_finish(&mut self, spilled_depth: usize, tx: Sender<FinalAggregateTask>) -> Result<()> {
        self.spill_out()?;

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
                self._id,
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
impl Processor for NewTransformFinalAggregate {
    fn name(&self) -> String {
        "NewTransformFinalAggregate".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            let _ = self.tx.take();
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.channel_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;
            if let Some(block_meta) = data_block
                .take_meta()
                .and_then(AggregateMeta::downcast_from)
            {
                self.input_data = Some(block_meta);
                return Ok(Event::Sync);
            }
        }

        if self.input.is_finished() {
            if matches!(self.stage, Stage::Input) {
                // the stage that get meta from input is end now
                // begin to get meta from channel
                self.stage = Stage::Channel;
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
        let input_data = self.input_data.take();
        if let Some(meta) = input_data {
            self.ensure_spill_depth(0);
            self.handle_meta(meta, self.max_aggregate_spill_level > 0)?;
            return Ok(());
        } else if let Some(mut task) = self.channel_data.take() {
            self.ensure_spill_depth(task.spilled_depth);
            self.statistics.reset();
            let meta = AggregateMeta::NewSpilled(mem::take(&mut task.spilled_payload));
            if task.spilled_depth >= self.max_aggregate_spill_level {
                self.handle_meta(meta, false)?;
            } else {
                self.handle_meta(meta, true)?;
            }
            self.finish(Some(task.task_id), task.spilled_depth, task.tx)?;

            return Ok(());
        } else {
            self.ensure_spill_depth(0);
            let sender = mem::take(&mut self.tx)
                .expect("logic error: called finished for input data more than once");
            self.finish(None, 0, sender)?;
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match self.rx.recv().await {
            Ok(meta) => {
                self.channel_data = Some(meta);
            }
            Err(_) => {
                self.should_finish = true;
            }
        }

        Ok(())
    }
}
