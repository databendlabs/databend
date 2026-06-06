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

use std::ops::Range;
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_exception::Result;

use super::AggregateHashTable;
use super::HashTableConfig;
use super::PartitionedPayload;
use super::Payload;
use super::PayloadFlushState;
use super::ProbeState;
use crate::DataBlock;
use crate::ProjectedBlock;
use crate::aggregate::AggregateFunction;
use crate::types::DataType;
use crate::utils::arrow::deserialize_column;

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

    pub fn convert_to_single_payload(
        &self,
        group_types: Vec<DataType>,
        aggrs: Vec<Arc<dyn AggregateFunction>>,
        num_states: usize,
        enable_experiment_hash_index: bool,
        arena: Arc<Bump>,
    ) -> Result<Payload> {
        let hashtable = self.convert_to_aggregate_table(
            group_types,
            aggrs,
            num_states,
            0,
            enable_experiment_hash_index,
            arena,
            false,
        )?;
        Ok(hashtable.payload.into_single_payload())
    }

    pub fn repartition_to_payloads(
        self,
        max_partition_count: usize,
        group_types: Vec<DataType>,
        aggrs: Vec<Arc<dyn AggregateFunction>>,
        num_states: usize,
        enable_experiment_hash_index: bool,
        flush_state: &mut PayloadFlushState,
    ) -> Result<Vec<AggregatePayload>> {
        Ok(self
            .convert_to_aggregate_table(
                group_types,
                aggrs,
                num_states,
                0,
                enable_experiment_hash_index,
                Arc::new(Bump::new()),
                false,
            )?
            .payload
            .repartition(max_partition_count, flush_state)
            .payloads
            .into_iter()
            .enumerate()
            .map(|(bucket, payload)| AggregatePayload {
                bucket: bucket as isize,
                payload,
                max_partition_count,
            })
            .collect())
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

impl BucketSpilledPayload {
    pub fn deserialize(self, data: Vec<u8>) -> Result<SerializedPayload> {
        let mut begin = 0;
        let mut columns = Vec::with_capacity(self.columns_layout.len());

        for column_layout in self.columns_layout {
            let end = begin + column_layout as usize;
            columns.push(deserialize_column(&data[begin..end])?);
            begin = end;
        }

        Ok(SerializedPayload {
            bucket: self.bucket,
            data_block: DataBlock::new_from_columns(columns),
            max_partition_count: self.max_partition_count,
        })
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

    pub fn repartition_to_payloads(
        self,
        max_partition_count: usize,
        group_types: Vec<DataType>,
        aggrs: Vec<Arc<dyn AggregateFunction>>,
        flush_state: &mut PayloadFlushState,
    ) -> Vec<Self> {
        let mut partitioned_payload =
            PartitionedPayload::new(group_types, aggrs, max_partition_count as u64, vec![
                self.payload.arena.clone(),
            ]);

        partitioned_payload.combine_single(self.payload, flush_state);

        partitioned_payload
            .into_bucket_payloads()
            .map(|(bucket, payload)| AggregatePayload {
                bucket: bucket as isize,
                payload,
                max_partition_count,
            })
            .collect()
    }
}
