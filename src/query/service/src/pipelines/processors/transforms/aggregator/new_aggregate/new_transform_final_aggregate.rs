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
use std::sync::atomic::AtomicUsize;

use async_channel::Receiver;
use async_channel::Sender;
use bumpalo::Bump;
use databend_common_catalog::table_context::TableContext;
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
use databend_common_pipeline_transforms::AccumulatingTransform;
use databend_common_pipeline_transforms::AccumulatingTransformer;
use databend_common_pipeline_transforms::MemorySettings;
use log::info;
use tokio::select;
use tokio::sync::Barrier;

use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatePayload;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::LocalPartitionStream;
use crate::pipelines::processors::transforms::aggregator::NewAggregateSpillReader;
use crate::pipelines::processors::transforms::aggregator::NewAggregateSpiller;
use crate::pipelines::processors::transforms::aggregator::NewSpilledPayload;
use crate::pipelines::processors::transforms::aggregator::SerializedPayload;
use crate::pipelines::processors::transforms::aggregator::statistics::AggregationStatistics;
use crate::pipelines::processors::transforms::aggregator::transform_aggregate_partial::HashTable;
use crate::sessions::QueryContext;

const SPILL_BUCKET_NUM: usize = 2;

enum Stage {
    Input,
    Channel,
}

pub struct NewTransformFinalAggregate {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    input_data: Option<AggregateMeta>,
    should_finish: bool,
    tx: Option<Sender<AggregateMeta>>,
    rx: Receiver<AggregateMeta>,
    stage: Stage,
    spilled_occurred: bool,
    finish_barrier: Arc<Barrier>,

    hashtable: HashTable,
    params: Arc<AggregatorParams>,
    flush_state: PayloadFlushState,
    statistics: AggregationStatistics,
    _id: usize,
    spiller: NewAggregateSpiller<LocalPartitionStream>,
    settings: MemorySettings,
}

impl NewTransformFinalAggregate {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: Arc<AggregatorParams>,
        _id: usize,
        ctx: Arc<QueryContext>,
        tx: Sender<AggregateMeta>,
        rx: Receiver<AggregateMeta>,
        finish_barrier: Arc<Barrier>,
    ) -> Result<Box<dyn Processor>> {
        let settings = ctx.get_settings();
        let max_aggregate_spill_level = settings.get_max_aggregate_spill_level()?;

        let hashtable = AggregateHashTable::new(
            params.group_data_types.clone(),
            params.aggregate_functions.clone(),
            HashTableConfig::default()
                .with_initial_radix_bits(SPILL_BUCKET_NUM.trailing_zeros() as u64),
            Arc::new(Bump::new()),
        );
        let flush_state = PayloadFlushState::default();

        let spiller = NewAggregateSpiller::try_create(
            ctx.clone(),
            SPILL_BUCKET_NUM,
            LocalPartitionStream::new(
                params.max_block_rows,
                params.max_block_bytes,
                SPILL_BUCKET_NUM,
            ),
            true,
        )?;

        Ok(Box::new(NewTransformFinalAggregate {
            input,
            output,
            input_data: None,
            should_finish: false,
            tx: Some(tx),
            rx,
            stage: Stage::Input,
            finish_barrier,
            spilled_occurred: false,
            hashtable: HashTable::AggregateHashTable(hashtable),
            params,
            flush_state,
            statistics: AggregationStatistics::new("NewFinalAggregate"),
            _id,
            spiller,
            settings: MemorySettings::from_aggregate_settings(&ctx)?,
        }))
    }
}

impl NewTransformFinalAggregate {
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

    fn handle_new_spilled(&mut self, payloads: Vec<NewSpilledPayload>) -> Result<()> {
        for payload in payloads {
            let restored = self.spiller.restore(payload)?;
            let AggregateMeta::Serialized(restored) = restored else {
                unreachable!("unexpected aggregate meta, found type: {:?}", restored)
            };
            self.handle_serialized(restored)?;
        }

        Ok(())
    }

    fn handle_meta(&mut self, meta: AggregateMeta) -> Result<()> {
        match meta {
            AggregateMeta::Serialized(payload) => {
                self.handle_serialized(payload)?;
            }
            AggregateMeta::AggregatePayload(payload) => {
                self.handle_aggregate_payload(payload)?;
            }
            AggregateMeta::NewSpilled(payloads) => {
                self.handle_new_spilled(payloads)?;
            }
            AggregateMeta::NewBucketSpilled(payload) => {
                self.handle_new_spilled(vec![payload])?;
            }
            AggregateMeta::Partitioned { bucket: _, data } => {
                for meta in data {
                    self.handle_meta(meta)?;
                }
            }
            _ => {
                unreachable!("unexpected aggregate meta, found type: {:?}", meta);
            }
        }

        if self.settings.check_spill() {
            self.spill_out()?;
        }

        Ok(())
    }

    fn spill_out(&mut self) -> Result<()> {
        if let HashTable::AggregateHashTable(v) = mem::take(&mut self.hashtable) {
            let group_types = v.payload.group_types.clone();
            let aggrs = v.payload.aggrs.clone();
            let config = v.config.clone();

            for (bucket, payload) in v.payload.payloads.into_iter().enumerate() {
                if payload.len() == 0 {
                    continue;
                }

                let data_block = payload.aggregate_flush_all()?.consume_convert_to_full();
                self.spiller.spill(bucket, data_block)?;
            }

            let arena = Arc::new(Bump::new());
            self.hashtable = HashTable::AggregateHashTable(AggregateHashTable::new(
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

    fn finish(&mut self) -> Result<()> {
        if self.spilled_occurred {
            self.spilled_occurred = false;
            self.spill_finish()?;
            return Ok(());
        }

        if let HashTable::AggregateHashTable(mut ht) = mem::take(&mut self.hashtable) {
            self.statistics.log_finish_statistics(&ht);
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
        }

        Ok(())
    }

    fn spill_finish(&mut self) -> Result<()> {
        if let Some(tx) = &self.tx {
            let spilled_payload = self.spiller.spill_finish()?;
            let mut chunks = (0..SPILL_BUCKET_NUM).map(|_| vec![]).collect::<Vec<_>>();
            let spilled_payload = spilled_payload
                .into_iter()
                .map(AggregateMeta::NewBucketSpilled)
                .collect::<Vec<_>>();
            for meta in spilled_payload.into_iter() {
                let AggregateMeta::NewBucketSpilled(ref payload) = meta else {
                    unreachable!("unexpected aggregate meta, found type: {:?}", meta)
                };
                chunks[payload.bucket as usize % SPILL_BUCKET_NUM].push(meta);
            }

            for chunk in chunks.into_iter() {
                let partitioned_meta = AggregateMeta::Partitioned {
                    bucket: None,
                    data: chunk,
                };
                tx.try_send(partitioned_meta).map_err(|e| {
                    ErrorCode::Internal(format!(
                        "Failed to send final aggregate meta to spiller: {}",
                        e
                    ))
                })?;
            }
        } else {
            unreachable!(
                "[FINAL-AGG] Tx channel is missing when trying to send final aggregate meta."
            );
        }
        Ok(())
    }

    fn debug_event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
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
        let event = self.debug_event()?;
        dbg!(self._id, &event);
        Ok(event)
    }

    fn process(&mut self) -> Result<()> {
        let input_data = self.input_data.take();
        if let Some(meta) = input_data {
            self.handle_meta(meta)?;
            if matches!(self.stage, Stage::Channel) {
                self.finish()?;
            }
        } else {
            self.finish()?;
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        select! {
            meta = self.rx.recv() => {
                match meta {
                    Ok(meta) => {
                        self.input_data = Some(meta);
                    }
                    Err(_) => {
                        self.should_finish = true;
                    }
                }
            }
            _ = self.finish_barrier.wait() => {
                self.should_finish = true;
            }
        }

        Ok(())
    }
}
