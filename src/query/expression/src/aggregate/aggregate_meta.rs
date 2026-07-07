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

use super::AggregateHashTable;
use super::HashTableConfig;
use super::PartitionedPayload;
use super::Payload;
use super::ProbeState;
use crate::DataBlock;
use crate::ProjectedBlock;
use crate::aggregate::AggregateFunction;
use crate::types::DataType;

pub struct SerializedPayload {
    pub bucket: isize,
    pub data_block: DataBlock,
    // use for new agg_hashtable
    pub max_partition_count: usize,
}

impl SerializedPayload {
    #[fastrace::trace(name = "SerializedPayload::convert_to_aggregate_table")]
    pub fn convert_to_aggregate_table(
        &self,
        group_types: Vec<DataType>,
        aggrs: Vec<Arc<dyn AggregateFunction>>,
        num_states: usize,
        radix_bits: u64,
        arena: Arc<Bump>,
        need_init_entry: bool,
    ) -> Result<AggregateHashTable> {
        let rows_num = self.data_block.num_rows();
        let capacity = AggregateHashTable::get_capacity_for_count(rows_num);
        let config = HashTableConfig::default().with_initial_radix_bits(radix_bits);
        let mut state = ProbeState::default();
        let group_len = group_types.len();
        let mut hashtable = AggregateHashTable::new_directly(
            group_types,
            aggrs,
            config,
            capacity,
            arena,
            need_init_entry,
        );

        let states_index: Vec<usize> = (0..num_states).collect();
        let agg_states = ProjectedBlock::project(&states_index, &self.data_block);

        let group_index: Vec<usize> = (num_states..(num_states + group_len)).collect();
        let group_columns = ProjectedBlock::project(&group_index, &self.data_block);

        let _ = hashtable.add_groups(
            &mut state,
            group_columns,
            &[(&[]).into()],
            agg_states,
            rows_num,
        )?;

        hashtable.payload.mark_min_cardinality();
        Ok(hashtable)
    }

    pub fn convert_to_partitioned_payload(
        &self,
        group_types: Vec<DataType>,
        aggrs: Vec<Arc<dyn AggregateFunction>>,
        num_states: usize,
        radix_bits: u64,
        arena: Arc<Bump>,
    ) -> Result<PartitionedPayload> {
        let hashtable = self.convert_to_aggregate_table(
            group_types,
            aggrs,
            num_states,
            radix_bits,
            arena,
            false,
        )?;
        Ok(hashtable.payload)
    }

    pub fn convert_to_single_payload(
        &self,
        group_types: Vec<DataType>,
        aggrs: Vec<Arc<dyn AggregateFunction>>,
        num_states: usize,
        arena: Arc<Bump>,
    ) -> Result<Payload> {
        let hashtable =
            self.convert_to_aggregate_table(group_types, aggrs, num_states, 0, arena, false)?;
        Ok(hashtable.payload.into_single_payload())
    }
}

pub struct AggregatePayload {
    pub bucket: isize,
    pub payload: Payload,
    // use for new agg_hashtable
    pub max_partition_count: usize,
}

impl AggregatePayload {
    pub fn exchange_block_number(&self) -> isize {
        self.max_partition_count as isize * 1000 + self.bucket
    }
}
