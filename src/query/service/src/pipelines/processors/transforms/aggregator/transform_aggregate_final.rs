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
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::AccumulatingTransform;
use databend_common_pipeline_transforms::AccumulatingTransformer;

use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;

pub struct TransformFinalAggregate {
    params: Arc<AggregatorParams>,
    flush_state: PayloadFlushState,
    hash_table: AggregateHashTable,
    has_output: bool,
}

impl AccumulatingTransform for TransformFinalAggregate {
    const NAME: &'static str = "TransformFinalAggregate";

    fn transform(&mut self, mut data: DataBlock) -> Result<Vec<DataBlock>> {
        let Some(meta) = data.take_meta() else {
            return Err(ErrorCode::Internal(""));
        };

        let Some(aggregate_meta) = AggregateMeta::downcast_from(meta) else {
            return Err(ErrorCode::Internal(""));
        };

        match aggregate_meta {
            AggregateMeta::SpilledPayload(_) => unreachable!(),
            AggregateMeta::InFlightPayload(payload) => {
                debug_assert_eq!(payload.max_partition, payload.global_max_partition);

                if !data.is_empty() {
                    let payload = self.deserialize_flight(data)?;
                    self.hash_table
                        .combine_payload(&payload, &mut self.flush_state)?;
                }
            }
            AggregateMeta::AggregatePayload(payload) => {
                debug_assert_eq!(payload.max_partition, payload.global_max_partition);

                if payload.payload.len() != 0 {
                    self.hash_table
                        .combine_payload(&payload.payload, &mut self.flush_state)?;
                }
            }
            AggregateMeta::FinalPartition => {
                if self.hash_table.len() == 0 {
                    return Ok(vec![]);
                }

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

                self.has_output |= !blocks.is_empty();
                return Ok(blocks);
            }
        }

        Ok(vec![])
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        assert!(!output || self.hash_table.len() == 0);

        if output && !self.has_output {
            return Ok(vec![self.params.empty_result_block()]);
        }

        Ok(vec![])
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

        Ok(AccumulatingTransformer::create(
            input,
            output,
            TransformFinalAggregate {
                params,
                hash_table,
                flush_state: PayloadFlushState::default(),
                has_output: false,
            },
        ))
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
}
