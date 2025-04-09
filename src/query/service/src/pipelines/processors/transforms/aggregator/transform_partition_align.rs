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
use std::collections::BTreeMap;
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
use databend_common_expression::PartitionedPayload;
use databend_common_expression::Payload;
use databend_common_expression::PayloadFlushState;
use databend_common_expression::ProbeState;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::MemorySettings;

use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::transforms::aggregator::transform_partition_bucket::SINGLE_LEVEL_BUCKET_NUM;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatePayload;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::sessions::QueryContext;

pub struct TransformPartitionAlign {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    #[allow(dead_code)]
    settings: MemorySettings,
    params: Arc<AggregatorParams>,

    max_partition: usize,
    working_partition: isize,
    partitions: Partitions,

    output_data: VecDeque<DataBlock>,
    input_data: Option<(AggregateMeta, DataBlock)>,
}

impl TransformPartitionAlign {
    pub fn create(
        ctx: Arc<QueryContext>,
        params: Arc<AggregatorParams>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<Self> {
        let settings = MemorySettings::from_aggregate_settings(&ctx)?;
        Ok(TransformPartitionAlign {
            input,
            output,
            params,
            settings,
            max_partition: 0,
            working_partition: 0,
            partitions: Partitions::create(),
            input_data: None,
            output_data: Default::default(),
        })
    }

    fn ready_partition(&mut self) -> Option<isize> {
        let storage_min_partition = self.partitions.min_partition()?;

        if storage_min_partition >= self.working_partition {
            return None;
        }

        Some(storage_min_partition)
    }

    fn fetch_ready_partition(&mut self) -> Result<()> {
        if let Some(ready_partition_id) = self.ready_partition() {
            let ready_partition = self.partitions.take_partition(ready_partition_id);

            for (meta, data_block) in ready_partition {
                self.output_data
                    .push_back(data_block.add_meta(Some(Box::new(meta)))?);
            }

            self.output_data
                .push_back(DataBlock::empty_with_meta(AggregateMeta::create_final()));
        }

        Ok(())
    }

    fn unpark_block(&self, mut data_block: DataBlock) -> Result<(AggregateMeta, DataBlock)> {
        let Some(meta) = data_block.take_meta() else {
            return Err(ErrorCode::Internal(
                "Internal, TransformPartitionBucket only recv DataBlock with meta.",
            ));
        };

        let Some(meta) = AggregateMeta::downcast_from(meta) else {
            return Err(ErrorCode::Internal(
                "Internal, TransformPartitionBucket only recv AggregateMeta".to_string(),
            ));
        };

        Ok((meta, data_block))
    }

    fn repartition(&mut self, meta: AggregateMeta, data_block: DataBlock) -> Result<()> {
        match meta {
            AggregateMeta::FinalPartition => unreachable!(),
            AggregateMeta::SpilledPayload(_payload) => unreachable!(),
            AggregateMeta::InFlightPayload(payload) => {
                if data_block.is_empty() {
                    return Ok(());
                }

                let payload = AggregatePayload {
                    partition: payload.partition,
                    max_partition: payload.max_partition,
                    payload: self.deserialize_flight(data_block)?,
                    global_max_partition: payload.global_max_partition,
                };

                let repartition = payload.global_max_partition;
                let partitioned = self.partition_payload(payload, repartition);

                for payload in partitioned {
                    self.partitions
                        .add_data(AggregateMeta::AggregatePayload(payload), DataBlock::empty());
                }
            }
            AggregateMeta::AggregatePayload(payload) => {
                if payload.payload.len() == 0 {
                    return Ok(());
                }

                let repartition = payload.global_max_partition;
                let partitioned = self.partition_payload(payload, repartition);
                for payload in partitioned {
                    self.partitions
                        .add_data(AggregateMeta::AggregatePayload(payload), DataBlock::empty());
                }
            }
        }

        Ok(())
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

    fn partition_payload(&mut self, from: AggregatePayload, to: usize) -> Vec<AggregatePayload> {
        let mut partitioned = Vec::with_capacity(to);
        let mut partitioned_payload = PartitionedPayload::new(
            self.params.group_data_types.clone(),
            self.params.aggregate_functions.clone(),
            to as u64,
            vec![from.payload.arena.clone()],
        );

        let mut flush_state = PayloadFlushState::default();
        partitioned_payload.combine_single(from.payload, &mut flush_state, None);

        for (partition, payload) in partitioned_payload.payloads.into_iter().enumerate() {
            partitioned.push(AggregatePayload {
                payload,
                partition: partition as isize,
                max_partition: to,
                global_max_partition: from.global_max_partition,
            });
        }

        partitioned
    }
}

impl Processor for TransformPartitionAlign {
    fn name(&self) -> String {
        String::from("TransformPartitionAlign")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
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

        if self.input.has_data() {
            let data_block = self.input.pull_data().unwrap()?;

            let (meta, data_block) = self.unpark_block(data_block)?;
            self.max_partition = meta.get_global_max_partition();

            // need repartition
            if meta.get_max_partition() != meta.get_global_max_partition() {
                self.input_data = Some((meta, data_block));
                return Ok(Event::Sync);
            }

            let partition = meta.get_sorting_partition();
            self.partitions.add_data(meta, data_block);

            if partition > SINGLE_LEVEL_BUCKET_NUM && partition != self.working_partition {
                self.working_partition = partition;
            }
        }

        if self.input.is_finished() && self.working_partition as usize != self.max_partition {
            self.working_partition = self.max_partition as isize;
        }

        if self.output_data.is_empty() {
            self.fetch_ready_partition()?;
        }

        if let Some(data_block) = self.output_data.pop_front() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some((meta, data_block)) = self.input_data.take() {
            self.repartition(meta, data_block)?;
        }

        Ok(())
    }
}

// #[async_trait::async_trait]
// impl AccumulatingTransform for TransformPartitionAlign {
//     const NAME: &'static str = "TransformPartitionAlign";
//
//     fn transform(&mut self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
//         let (meta, data_block) = self.unpark_block(data_block)?;
//         self.max_partition = meta.get_global_max_partition();
//
//         // need repartition
//         if meta.get_max_partition() != meta.get_global_max_partition() {
//             self.repartition(meta, data_block)?;
//             return Ok(vec![]);
//         }
//
//         let partition = meta.get_sorting_partition();
//         self.partitions.add_data(meta, data_block);
//
//         if partition > SINGLE_LEVEL_BUCKET_NUM && partition != self.working_partition {
//             self.fetch_ready_partition()?;
//             self.working_partition = partition;
//             // return Ok(ready_partition);
//         }
//
//         Ok(vec![])
//     }
//
//     fn on_finish(&mut self, _output: bool) -> Result<Vec<DataBlock>> {
//         let remain_size = self
//             .partitions
//             .data
//             .values()
//             .map(|x| x.len())
//             .sum::<usize>();
//
//         let mut remain_partitions = Vec::with_capacity(remain_size + self.partitions.data.len());
//         self.working_partition = self.max_partition as isize;
//
//         loop {
//             let ready_partition = self.fetch_ready_partition()?;
//
//             if !ready_partition.is_empty() {
//                 remain_partitions.extend(ready_partition);
//                 continue;
//             }
//
//             return Ok(remain_partitions);
//         }
//     }
//
//     fn need_spill(&self) -> bool {
//         self.settings.check_spill()
//     }
//
//     fn prepare_spill_payload(&mut self) -> Result<bool> {
//         // self.partitions.data.f
//         Ok(false)
//     }
//
//     async fn flush_spill_payload(&mut self) -> Result<bool> {
//         Ok(false)
//     }
// }

#[derive(Debug)]
struct Partitions {
    data: BTreeMap<isize, Vec<(AggregateMeta, DataBlock)>>,
}

impl Partitions {
    pub fn create() -> Partitions {
        Partitions {
            data: BTreeMap::new(),
        }
    }

    pub fn add_data(&mut self, meta: AggregateMeta, block: DataBlock) {
        if matches!(&meta, AggregateMeta::AggregatePayload(v) if v.payload.len() == 0)
            || matches!(&meta, AggregateMeta::InFlightPayload(_) if block.is_empty())
        {
            return;
        }

        match self.data.entry(meta.get_partition()) {
            std::collections::btree_map::Entry::Vacant(v) => {
                v.insert(vec![(meta, block)]);
            }
            std::collections::btree_map::Entry::Occupied(mut v) => {
                v.get_mut().push((meta, block));
            }
        };
    }

    pub fn min_partition(&self) -> Option<isize> {
        self.data.keys().min().cloned()
    }

    pub fn take_partition(&mut self, partition: isize) -> Vec<(AggregateMeta, DataBlock)> {
        self.data.remove(&partition).unwrap_or_default()
    }
}
