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
use databend_common_exception::Result;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::HashTableConfig;
use databend_common_expression::InputColumns;
use databend_common_expression::Payload;
use databend_common_expression::PayloadFlushState;
use databend_common_expression::ProbeState;
use databend_common_pipeline_core::processors::Exchange;

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatePayload;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::InFlightPayload;

pub struct ExchangePartition {
    params: Arc<AggregatorParams>,
}

impl ExchangePartition {
    pub fn create(params: Arc<AggregatorParams>) -> Arc<Self> {
        Arc::new(ExchangePartition { params })
    }
}

impl ExchangePartition {
    fn partition_final_payload(n: usize) -> Result<Vec<DataBlock>> {
        Ok((0..n)
            .map(|_| DataBlock::empty_with_meta(AggregateMeta::create_final(None)))
            .collect())
    }

    fn partition_aggregate_payload(
        mut payload: AggregatePayload,
        n: usize,
    ) -> Result<Vec<DataBlock>> {
        let mut repartition_payloads = Vec::with_capacity(n);

        let group_types = payload.payload.group_types.clone();
        let aggrs = payload.payload.aggrs.clone();
        let mut state = PayloadFlushState::default();

        for _ in 0..repartition_payloads.capacity() {
            repartition_payloads.push(Payload::new(
                payload.payload.arena.clone(),
                group_types.clone(),
                aggrs.clone(),
                payload.payload.states_layout.clone(),
            ));
        }

        // scatter each page of the payload.
        while payload
            .payload
            .scatter_with_seed::<9263883436177860930>(&mut state, repartition_payloads.len())
        {
            // copy to the corresponding bucket.
            for (idx, bucket) in repartition_payloads.iter_mut().enumerate() {
                let count = state.probe_state.partition_count[idx];

                if count > 0 {
                    let sel = &state.probe_state.partition_entries[idx];
                    bucket.copy_rows(sel, count, &state.addresses);
                }
            }
        }

        payload.payload.state_move_out = true;

        let mut partitions = Vec::with_capacity(repartition_payloads.len());

        for repartition_payload in repartition_payloads {
            partitions.push(DataBlock::empty_with_meta(
                AggregateMeta::create_agg_payload(
                    repartition_payload,
                    payload.partition,
                    payload.max_partition,
                    payload.global_max_partition,
                ),
            ));
        }

        Ok(partitions)
    }

    fn partition_flight_payload(
        &self,
        payload: InFlightPayload,
        block: DataBlock,
        n: usize,
    ) -> Result<Vec<DataBlock>> {
        let rows_num = block.num_rows();
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
        let agg_states = InputColumns::new_block_proxy(&states_index, &block);

        let group_index: Vec<usize> = (num_states..(num_states + group_len)).collect();
        let group_columns = InputColumns::new_block_proxy(&group_index, &block);

        let _ = hashtable.add_groups(
            &mut state,
            group_columns,
            &[(&[]).into()],
            agg_states,
            rows_num,
        )?;

        hashtable.payload.mark_min_cardinality();
        assert_eq!(hashtable.payload.payloads.len(), 1);

        Self::partition_aggregate_payload(
            AggregatePayload {
                partition: payload.partition,
                payload: hashtable.payload.payloads.pop().unwrap(),
                max_partition: payload.max_partition,
                global_max_partition: payload.global_max_partition,
            },
            n,
        )
    }
}

impl Exchange for ExchangePartition {
    const NAME: &'static str = "AggregatePartitionExchange";
    const MULTIWAY_SORT: bool = false;

    fn partition(&self, mut data_block: DataBlock, n: usize) -> Result<Vec<DataBlock>> {
        let Some(meta) = data_block.take_meta() else {
            return Ok(vec![data_block]);
        };

        let Some(meta) = AggregateMeta::downcast_from(meta) else {
            return Ok(vec![data_block]);
        };

        match meta {
            // already restore in upstream
            AggregateMeta::SpilledPayload(_) => unreachable!(),
            // broadcast final partition to downstream
            AggregateMeta::FinalPartition(_) => Self::partition_final_payload(n),
            AggregateMeta::AggregatePayload(payload) => {
                Self::partition_aggregate_payload(payload, n)
            }
            AggregateMeta::InFlightPayload(payload) => {
                self.partition_flight_payload(payload, data_block, n)
            }
        }
    }
}
