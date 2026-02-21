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
use databend_common_expression::AggregateFunction;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::HashTableConfig;
use databend_common_expression::PartitionedPayload;
use databend_common_expression::Payload;
use databend_common_expression::ProbeState;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::types::DataType;
use parquet::file::metadata::RowGroupMetaData;

pub struct SerializedPayload {
    pub bucket: isize,
    pub data_block: DataBlock,
    // use for new agg_hashtable
    pub max_partition_count: usize,
}

impl SerializedPayload {
    pub fn get_group_by_column(&self) -> &Column {
        let entry = self.data_block.columns().last().unwrap();
        entry.as_column().unwrap()
    }

    #[fastrace::trace(name = "SerializedPayload::convert_to_aggregate_table")]
    pub fn convert_to_aggregate_table(
        &self,
        group_types: Vec<DataType>,
        aggrs: Vec<Arc<dyn AggregateFunction>>,
        num_states: usize,
        radix_bits: u64,
        enable_experiment_hash_index: bool,
        arena: Arc<Bump>,
        need_init_entry: bool,
    ) -> Result<AggregateHashTable> {
        let rows_num = self.data_block.num_rows();
        let capacity = AggregateHashTable::get_capacity_for_count(rows_num);
        let config = HashTableConfig::default()
            .with_initial_radix_bits(radix_bits)
            .with_experiment_hash_index(enable_experiment_hash_index);
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
        enable_experiment_hash_index: bool,
        arena: Arc<Bump>,
    ) -> Result<PartitionedPayload> {
        let hashtable = self.convert_to_aggregate_table(
            group_types,
            aggrs,
            num_states,
            radix_bits,
            enable_experiment_hash_index,
            arena,
            false,
        )?;
        Ok(hashtable.payload)
    }
}

#[derive(Debug)]
pub struct BucketSpilledPayload {
    pub bucket: isize,
    pub location: String,
    pub data_range: Range<u64>,
    pub columns_layout: Vec<u64>,
    pub max_partition_count: usize,
}

pub struct NewSpilledPayload {
    pub bucket: isize,
    pub location: String,
    pub row_group: RowGroupMetaData,
}

pub struct AggregatePayload {
    pub bucket: isize,
    pub payload: Payload,
    // use for new agg_hashtable
    pub max_partition_count: usize,
}

pub enum AggregateMeta {
    Serialized(SerializedPayload),
    AggregatePayload(AggregatePayload),
    AggregateSpilling(PartitionedPayload),
    BucketSpilled(BucketSpilledPayload),
    Spilled(Vec<BucketSpilledPayload>),

    Partitioned {
        bucket: Option<isize>,
        data: Vec<Self>,
    },

    NewBucketSpilled(NewSpilledPayload),
    NewSpilled(Vec<NewSpilledPayload>),
}

impl AggregateMeta {
    pub fn create_agg_payload(
        bucket: isize,
        payload: Payload,
        max_partition_count: usize,
    ) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::AggregatePayload(AggregatePayload {
            bucket,
            payload,
            max_partition_count,
        }))
    }

    pub fn create_agg_spilling(payload: PartitionedPayload) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::AggregateSpilling(payload))
    }

    pub fn create_serialized(
        bucket: isize,
        block: DataBlock,
        max_partition_count: usize,
    ) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::Serialized(SerializedPayload {
            bucket,
            data_block: block,
            max_partition_count,
        }))
    }

    pub fn create_spilled(buckets_payload: Vec<BucketSpilledPayload>) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::Spilled(buckets_payload))
    }

    pub fn create_bucket_spilled(payload: BucketSpilledPayload) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::BucketSpilled(payload))
    }

    pub fn create_partitioned(bucket: Option<isize>, data: Vec<Self>) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::Partitioned { data, bucket })
    }

    pub fn create_new_bucket_spilled(payload: NewSpilledPayload) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::NewBucketSpilled(payload))
    }

    pub fn create_new_spilled(payloads: Vec<NewSpilledPayload>) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::NewSpilled(payloads))
    }
}

impl serde::Serialize for AggregateMeta {
    fn serialize<S>(&self, _: S) -> std::result::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        unreachable!("AggregateMeta does not support exchanging between multiple nodes")
    }
}

impl<'de> serde::Deserialize<'de> for AggregateMeta {
    fn deserialize<D>(_: D) -> std::result::Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        unreachable!("AggregateMeta does not support exchanging between multiple nodes")
    }
}

impl Debug for AggregateMeta {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            AggregateMeta::Partitioned { .. } => {
                f.debug_struct("AggregateMeta::Partitioned").finish()
            }
            AggregateMeta::Serialized { .. } => {
                f.debug_struct("AggregateMeta::Serialized").finish()
            }
            AggregateMeta::Spilled(_) => f.debug_struct("Aggregate::Spilled").finish(),
            AggregateMeta::BucketSpilled(_) => f.debug_struct("Aggregate::BucketSpilled").finish(),
            AggregateMeta::NewBucketSpilled(_) => {
                f.debug_struct("Aggregate::NewBucketSpilled").finish()
            }
            AggregateMeta::NewSpilled(_) => f.debug_struct("Aggregate::NewSpilled").finish(),
            AggregateMeta::AggregatePayload(_) => {
                f.debug_struct("AggregateMeta:AggregatePayload").finish()
            }
            AggregateMeta::AggregateSpilling(_) => {
                f.debug_struct("AggregateMeta:AggregateSpilling").finish()
            }
        }
    }
}

impl BlockMetaInfo for AggregateMeta {
    fn typetag_deserialize(&self) {
        unimplemented!("AggregateMeta does not support exchanging between multiple nodes")
    }

    fn typetag_name(&self) -> &'static str {
        unimplemented!("AggregateMeta does not support exchanging between multiple nodes")
    }
}
