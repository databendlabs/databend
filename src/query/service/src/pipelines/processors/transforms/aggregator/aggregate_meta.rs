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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::ops::Range;
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_exception::Result;
use databend_common_expression::local_block_meta_serde;
use databend_common_expression::types::DataType;
use databend_common_expression::AggregateFunction;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::HashTableConfig;
use databend_common_expression::InputColumns;
use databend_common_expression::PartitionedPayload;
use databend_common_expression::Payload;
use databend_common_expression::ProbeState;

pub struct SerializedPayload {
    pub bucket: isize,
    pub data_block: DataBlock,
    // use for new agg_hashtable
    pub max_partition: usize,
    pub global_max_partition: usize,
}

impl SerializedPayload {
    pub fn get_group_by_column(&self) -> &Column {
        let entry = self.data_block.columns().last().unwrap();
        entry.value.as_column().unwrap()
    }

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
        let agg_states = InputColumns::new_block_proxy(&states_index, &self.data_block);

        let group_index: Vec<usize> = (num_states..(num_states + group_len)).collect();
        let group_columns = InputColumns::new_block_proxy(&group_index, &self.data_block);

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
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct SpilledPayload {
    pub partition: isize,
    pub location: String,
    pub data_range: Range<u64>,
    pub destination_node: String,
    pub max_partition: usize,
    pub global_max_partition: usize,
}

pub struct AggregatePayload {
    pub partition: isize,
    pub payload: Payload,
    // use for new agg_hashtable
    pub max_partition: usize,
    pub global_max_partition: usize,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct InFlightPayload {
    pub partition: isize,
    pub max_partition: usize,
    pub global_max_partition: usize,
}

pub struct FinalPayload {
    pub data: Vec<(AggregateMeta, DataBlock)>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum AggregateMeta {
    Serialized(SerializedPayload),
    SpilledPayload(SpilledPayload),
    AggregatePayload(AggregatePayload),
    InFlightPayload(InFlightPayload),
    FinalPartition,
}

impl AggregateMeta {
    pub fn create_agg_payload(
        payload: Payload,
        partition: isize,
        max_partition: usize,
    ) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::AggregatePayload(AggregatePayload {
            payload,
            partition,
            max_partition,
            global_max_partition: max_partition,
        }))
    }

    pub fn create_in_flight_payload(partition: isize, max_partition: usize) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::InFlightPayload(InFlightPayload {
            partition,
            max_partition,
            global_max_partition: max_partition,
        }))
    }

    pub fn create_serialized(
        bucket: isize,
        block: DataBlock,
        max_partition: usize,
    ) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::Serialized(SerializedPayload {
            bucket,
            data_block: block,
            max_partition,
            global_max_partition: max_partition,
        }))
    }

    pub fn create_spilled_payload(payload: SpilledPayload) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::SpilledPayload(payload))
    }

    pub fn create_final() -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::FinalPartition)
    }

    pub fn get_global_max_partition(&self) -> usize {
        match self {
            AggregateMeta::Serialized(v) => v.global_max_partition,
            AggregateMeta::SpilledPayload(v) => v.global_max_partition,
            AggregateMeta::AggregatePayload(v) => v.global_max_partition,
            AggregateMeta::InFlightPayload(v) => v.global_max_partition,
            AggregateMeta::FinalPartition => unreachable!(),
        }
    }

    pub fn get_partition(&self) -> isize {
        match self {
            AggregateMeta::Serialized(v) => v.bucket,
            AggregateMeta::SpilledPayload(v) => v.partition,
            AggregateMeta::AggregatePayload(v) => v.partition,
            AggregateMeta::InFlightPayload(v) => v.partition,
            AggregateMeta::FinalPartition => unreachable!(),
        }
    }

    pub fn get_max_partition(&self) -> usize {
        match self {
            AggregateMeta::Serialized(v) => v.max_partition,
            AggregateMeta::SpilledPayload(v) => v.max_partition,
            AggregateMeta::AggregatePayload(v) => v.max_partition,
            AggregateMeta::InFlightPayload(v) => v.max_partition,
            AggregateMeta::FinalPartition => unreachable!(),
        }
    }

    pub fn set_global_max_partition(&mut self, global_max_partition: usize) {
        match self {
            AggregateMeta::Serialized(v) => {
                v.global_max_partition = global_max_partition;
            }
            AggregateMeta::SpilledPayload(v) => {
                v.global_max_partition = global_max_partition;
            }
            AggregateMeta::AggregatePayload(v) => {
                v.global_max_partition = global_max_partition;
            }
            AggregateMeta::InFlightPayload(v) => {
                v.global_max_partition = global_max_partition;
            }
            AggregateMeta::FinalPartition => unreachable!(),
        }
    }
}

impl Debug for AggregateMeta {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            AggregateMeta::FinalPartition => f.debug_struct("AggregateMeta::Partitioned").finish(),
            AggregateMeta::Serialized { .. } => {
                f.debug_struct("AggregateMeta::Serialized").finish()
            }
            AggregateMeta::SpilledPayload(_) => {
                f.debug_struct("Aggregate::SpilledPayload").finish()
            }
            AggregateMeta::InFlightPayload(_) => {
                f.debug_struct("Aggregate:InFlightPayload").finish()
            }
            AggregateMeta::AggregatePayload(_) => {
                f.debug_struct("AggregateMeta:AggregatePayload").finish()
            }
        }
    }
}

#[typetag::serde(name = "AggregateMeta")]
impl BlockMetaInfo for AggregateMeta {}

local_block_meta_serde!(FinalPayload);
local_block_meta_serde!(AggregatePayload);
local_block_meta_serde!(SerializedPayload);
