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
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::HashTableConfig;
use databend_common_expression::InputColumns;
use databend_common_expression::Payload;
use databend_common_expression::PayloadFlushState;
use databend_common_expression::ProbeState;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;

use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;

pub struct TransformFinalAggregate {
    params: Arc<AggregatorParams>,
    flush_state: PayloadFlushState,
    hash_table: AggregateHashTable,

    working_partition: isize,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    next_partition_data: VecDeque<(AggregateMeta, DataBlock)>,

    input_data: Vec<(AggregateMeta, DataBlock)>,
    output_data: Option<DataBlock>,
}

#[async_trait::async_trait]
impl Processor for TransformFinalAggregate {
    fn name(&self) -> String {
        String::from("TransformFinalAggregate")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input_data.clear();
            self.next_partition_data.clear();

            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.take() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input.has_data() {
            return match self.add_data(self.input.pull_data().unwrap()?)? {
                true => Ok(Event::Sync),
                false => {
                    self.input.set_need_data();
                    Ok(Event::NeedData)
                }
            };
        }

        if self.input.is_finished() {
            return match self.input_data.is_empty() {
                true => {
                    self.output.finish();
                    Ok(Event::Finished)
                }
                false => Ok(Event::Sync),
            };
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        for (meta, block) in std::mem::take(&mut self.input_data).into_iter() {
            match meta {
                AggregateMeta::SpilledPayload(_) => unreachable!(),
                AggregateMeta::FinalPartition(_) => unreachable!(),
                AggregateMeta::InFlightPayload(_) => {
                    let payload = self.deserialize_flight(block)?;

                    self.hash_table
                        .combine_payload(&payload, &mut self.flush_state)?;
                }
                AggregateMeta::AggregatePayload(payload) => {
                    self.hash_table
                        .combine_payload(&payload.payload, &mut self.flush_state)?;
                }
            };
        }

        while let Some(next_partition_data) = self.next_partition_data.pop_front() {
            self.input_data.push(next_partition_data);
        }

        match self.hash_table.len() {
            0 => {
                self.output_data = Some(self.params.empty_result_block());
            }
            _ => {
                let flush_blocks = self.flush_result_blocks()?;
                self.output_data = Some(flush_blocks);
            }
        }

        Ok(())
    }
}

impl TransformFinalAggregate {
    pub fn add_data(&mut self, mut block: DataBlock) -> Result<bool> {
        let Some(meta) = block.take_meta() else {
            return Err(ErrorCode::Internal(
                "Internal, TransformFinalAggregate only recv DataBlock with meta.",
            ));
        };

        let Some(aggregate_meta) = AggregateMeta::downcast_from(meta) else {
            return Err(ErrorCode::Internal(
                "Internal, TransformFinalAggregate only recv DataBlock with meta.",
            ));
        };

        match aggregate_meta {
            AggregateMeta::SpilledPayload(_) => Ok(false),
            AggregateMeta::FinalPartition(payload) => {
                let mut need_final = false;
                let working_partition = self.working_partition;

                for block in payload.data {
                    self.working_partition = working_partition;
                    need_final = self.add_data(block)?;
                }

                Ok(need_final)
            }
            AggregateMeta::InFlightPayload(payload) => {
                debug_assert!(payload.partition >= self.working_partition);
                debug_assert_eq!(payload.max_partition, payload.global_max_partition);

                if self.working_partition != payload.partition {
                    self.working_partition = payload.partition;
                    self.next_partition_data
                        .push_back((AggregateMeta::InFlightPayload(payload), block));
                    return Ok(true);
                }

                if !block.is_empty() {
                    self.input_data
                        .push((AggregateMeta::InFlightPayload(payload), block));
                }

                Ok(false)
            }
            AggregateMeta::AggregatePayload(payload) => {
                debug_assert!(payload.partition >= self.working_partition);
                debug_assert_eq!(payload.max_partition, payload.global_max_partition);

                if self.working_partition != payload.partition {
                    self.working_partition = payload.partition;
                    self.next_partition_data
                        .push_back((AggregateMeta::AggregatePayload(payload), block));
                    return Ok(true);
                }

                if payload.payload.len() != 0 {
                    self.input_data
                        .push((AggregateMeta::AggregatePayload(payload), block));
                }

                Ok(false)
            }
        }
    }
}

impl TransformFinalAggregate {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: Arc<AggregatorParams>,
    ) -> Result<Box<dyn Processor>> {
        let config = HashTableConfig::default().with_initial_radix_bits(0);

        let hash_table = AggregateHashTable::new(
            params.group_data_types.clone(),
            params.aggregate_functions.clone(),
            config,
            Arc::new(Bump::new()),
        );

        Ok(Box::new(TransformFinalAggregate {
            params,
            hash_table,
            input,
            output,
            input_data: vec![],
            output_data: None,
            working_partition: 0,
            next_partition_data: VecDeque::new(),
            flush_state: PayloadFlushState::default(),
        }))
    }

    fn deserialize_flight(&mut self, data: DataBlock) -> Result<Payload> {
        let rows_num = data.num_rows();
        let group_len = self.params.group_data_types.len();

        let mut state = ProbeState::default();

        // create single partition hash table for deserialize
        let capacity = AggregateHashTable::get_capacity_for_count(rows_num);
        let config = HashTableConfig::default().with_initial_radix_bits(0);
        let mut hashtable = AggregateHashTable::new_directly(
            self.params.group_data_types.clone(),
            self.params.aggregate_functions.clone(),
            config,
            capacity,
            Arc::new(Bump::new()),
            false,
        );

        let num_states = self.params.num_states();
        let states_index: Vec<usize> = (0..num_states).collect();
        let agg_states = InputColumns::new_block_proxy(&states_index, &data);

        let group_index: Vec<usize> = (num_states..(num_states + group_len)).collect();
        let group_columns = InputColumns::new_block_proxy(&group_index, &data);

        let _ = hashtable.add_groups(
            &mut state,
            group_columns,
            &[(&[]).into()],
            agg_states,
            rows_num,
        )?;

        hashtable.payload.mark_min_cardinality();
        assert_eq!(hashtable.payload.payloads.len(), 1);
        Ok(hashtable.payload.payloads.pop().unwrap())
    }

    fn flush_result_blocks(&mut self) -> Result<DataBlock> {
        let mut blocks = vec![];
        self.flush_state.clear();

        while self.hash_table.merge_result(&mut self.flush_state)? {
            let mut cols = self.flush_state.take_aggregate_results();
            cols.extend_from_slice(&self.flush_state.take_group_columns());
            blocks.push(DataBlock::new_from_columns(cols));
        }

        let config = HashTableConfig::default().with_initial_radix_bits(0);
        self.hash_table = AggregateHashTable::new(
            self.params.group_data_types.clone(),
            self.params.aggregate_functions.clone(),
            config,
            Arc::new(Bump::new()),
        );

        DataBlock::concat(&blocks)
    }
}
