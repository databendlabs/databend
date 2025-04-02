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
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering as AtomicOrdering;
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
use databend_common_pipeline_core::processors::Exchange;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::PipeItem;
use databend_common_pipeline_core::Pipeline;
use databend_common_storage::DataOperator;
use tokio::sync::Semaphore;

use super::AggregatePayload;
use super::TransformFinalAggregate;
use super::TransformSpillReader;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;

static SINGLE_LEVEL_BUCKET_NUM: isize = -1;

pub struct TransformPartitionDispatch {
    params: Arc<AggregatorParams>,
    outputs: Vec<Arc<OutputPort>>,
    input: Arc<InputPort>,
    outputs_data: Vec<VecDeque<DataBlock>>,
    output_index: usize,
    finished: bool,
    input_data: Option<(AggregateMeta, DataBlock)>,

    max_partition: usize,
    working_partition: isize,
    partitions: AlignedPartitions,
}

impl TransformPartitionDispatch {
    pub fn create(output_nums: usize, params: Arc<AggregatorParams>) -> Result<Self> {
        let mut outputs = Vec::with_capacity(output_nums);
        let mut outputs_data = Vec::with_capacity(output_nums);

        for _index in 0..output_nums {
            outputs.push(OutputPort::create());
            outputs_data.push(VecDeque::new());
        }

        Ok(TransformPartitionDispatch {
            params,
            outputs,
            outputs_data,
            input: InputPort::create(),
            output_index: 0,
            max_partition: 0,
            working_partition: 0,
            finished: false,
            input_data: None,
            partitions: AlignedPartitions::create(),
        })
    }

    pub fn get_input(&self) -> Arc<InputPort> {
        self.input.clone()
    }

    pub fn get_outputs(&self) -> Vec<Arc<OutputPort>> {
        self.outputs.clone()
    }

    fn ready_partition(&mut self) -> Option<isize> {
        let storage_min_partition = self.partitions.min_partition()?;

        if storage_min_partition > self.working_partition {
            return None;
        }

        Some(storage_min_partition)
    }

    fn fetch_ready_partition(&mut self) -> Result<()> {
        if let Some(ready_partition_id) = self.ready_partition() {
            let ready_partition = self.partitions.take_partition(ready_partition_id);

            for (meta, data_block) in ready_partition {
                self.outputs_data[self.output_index]
                    .push_back(data_block.add_meta(Some(Box::new(meta)))?);
            }

            self.outputs_data[self.output_index]
                .push_back(DataBlock::empty_with_meta(AggregateMeta::create_final()));

            self.output_index += 1;
            if self.output_index >= self.outputs_data.len() {
                self.output_index = 0;
            }
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
}

#[async_trait::async_trait]
impl Processor for TransformPartitionDispatch {
    fn name(&self) -> String {
        String::from("TransformPartitionDispatch")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        loop {
            let mut all_output_finished = true;
            let mut all_data_pushed_output = true;

            for (idx, output) in self.outputs.iter().enumerate() {
                if output.is_finished() {
                    self.outputs_data[idx].clear();
                    continue;
                }

                if self.finished && self.outputs_data[idx].is_empty() {
                    output.finish();
                    continue;
                }

                all_output_finished = false;

                if output.can_push() {
                    if let Some(block) = self.outputs_data[idx].pop_front() {
                        if !block.is_empty() || block.get_meta().is_some() {
                            output.push_data(Ok(block));
                        }

                        continue;
                    }
                }

                if !self.outputs_data[idx].is_empty() {
                    all_data_pushed_output = false;
                }
            }

            if all_output_finished {
                self.input.finish();
                return Ok(Event::Finished);
            }

            if !all_data_pushed_output {
                self.input.set_not_need_data();
                return Ok(Event::NeedConsume);
            }

            if self.input.has_data() {
                let data_block = self.input.pull_data().unwrap()?;
                let (meta, data_block) = self.unpark_block(data_block)?;

                // need repartition
                if meta.get_max_partition() != meta.get_global_max_partition() {
                    self.input_data = Some((meta, data_block));
                    return Ok(Event::Sync);
                }

                let partition = meta.get_sorting_partition();
                self.partitions.add_data(meta, data_block);

                if partition > SINGLE_LEVEL_BUCKET_NUM && partition != self.working_partition {
                    self.fetch_ready_partition()?;
                    self.working_partition = partition;
                    continue;
                }
            }

            if self.input.is_finished() {
                self.working_partition = self.max_partition as isize;

                self.fetch_ready_partition()?;
                self.finished = self.partitions.is_empty();
                continue;
            }

            self.input.set_need_data();
            return Ok(Event::NeedData);
        }
    }

    fn process(&mut self) -> Result<()> {
        let Some((meta, data_block)) = self.input_data.take() else {
            return Ok(());
        };

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
}

pub struct ResortingPartition {
    global_max_partition: AtomicUsize,
}

impl ResortingPartition {
    fn block_number(meta: &AggregateMeta) -> (isize, usize) {
        (meta.get_sorting_partition(), meta.get_max_partition())
    }
}

impl Exchange for ResortingPartition {
    const NAME: &'static str = "PartitionResorting";
    const MULTIWAY_SORT: bool = true;

    fn partition(&self, mut data_block: DataBlock, n: usize) -> Result<Vec<DataBlock>> {
        debug_assert_eq!(n, 1);

        let Some(meta) = data_block.take_meta() else {
            return Ok(vec![data_block]);
        };

        let Some(_) = AggregateMeta::downcast_ref_from(&meta) else {
            return Ok(vec![data_block]);
        };

        let global_max_partition = self.global_max_partition.load(AtomicOrdering::SeqCst);
        let mut meta = AggregateMeta::downcast_from(meta).unwrap();
        meta.set_global_max_partition(global_max_partition);

        Ok(vec![data_block.add_meta(Some(Box::new(meta)))?])
    }

    fn init_way(&self, _index: usize, first_data: &DataBlock) -> Result<()> {
        let max_partition = match first_data.get_meta() {
            None => 0,
            Some(meta) => match AggregateMeta::downcast_ref_from(meta) {
                None => 0,
                Some(v) => v.get_global_max_partition(),
            },
        };

        self.global_max_partition
            .fetch_max(max_partition, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    fn sorting_function(left_block: &DataBlock, right_block: &DataBlock) -> Ordering {
        let Some(left_meta) = left_block.get_meta() else {
            return Ordering::Equal;
        };
        let Some(left_meta) = AggregateMeta::downcast_ref_from(left_meta) else {
            return Ordering::Equal;
        };

        let Some(right_meta) = right_block.get_meta() else {
            return Ordering::Equal;
        };
        let Some(right_meta) = AggregateMeta::downcast_ref_from(right_meta) else {
            return Ordering::Equal;
        };

        let (l_partition, l_max_partition) = ResortingPartition::block_number(left_meta);
        let (r_partition, r_max_partition) = ResortingPartition::block_number(right_meta);

        // ORDER BY max_partition asc, partition asc, idx asc
        match l_max_partition.cmp(&r_max_partition) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => l_partition.cmp(&r_partition),
        }
    }
}

pub fn build_partition_dispatch(
    pipeline: &mut Pipeline,
    params: Arc<AggregatorParams>,
) -> Result<()> {
    let output = pipeline.output_len();

    // 1. reorder partition
    pipeline.exchange(
        1,
        Arc::new(ResortingPartition {
            global_max_partition: AtomicUsize::new(0),
        }),
    );

    let transform = TransformPartitionDispatch::create(output, params.clone())?;

    let input_port = transform.get_input();
    let outputs_port = transform.get_outputs();

    pipeline.add_pipe(Pipe::create(1, outputs_port.len(), vec![PipeItem::create(
        ProcessorPtr::create(Box::new(transform)),
        vec![input_port],
        outputs_port,
    )]));

    let semaphore = Arc::new(Semaphore::new(params.max_spill_io_requests));
    let operator = DataOperator::instance().spill_operator();
    pipeline.add_transform(|input, output| {
        let operator = operator.clone();
        TransformSpillReader::create(input, output, operator, semaphore.clone(), params.clone())
    })?;

    pipeline.add_transform(|input, output| {
        Ok(ProcessorPtr::create(TransformFinalAggregate::try_create(
            input,
            output,
            params.clone(),
        )?))
    })?;
    Ok(())
}

// repartition implementation
impl TransformPartitionDispatch {
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
                global_max_partition: 0,
            });
        }

        partitioned
    }
}

#[derive(Debug)]
struct AlignedPartitions {
    data: BTreeMap<isize, Vec<(AggregateMeta, DataBlock)>>,
}

impl AlignedPartitions {
    pub fn create() -> AlignedPartitions {
        AlignedPartitions {
            data: BTreeMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn add_data(&mut self, meta: AggregateMeta, block: DataBlock) {
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

// #[derive(Debug)]
// enum Partitions {
//     Aligned(AlignedPartitions),
//     Unaligned(UnalignedPartitions),
// }
//
// impl Partitions {
//     pub fn create_unaligned(params: Arc<AggregatorParams>) -> Partitions {
//         Partitions::Unaligned(UnalignedPartitions::create(params))
//     }
//
//     pub fn is_empty(&self) -> bool {
//         match self {
//             Partitions::Aligned(v) => v.is_empty(),
//             Partitions::Unaligned(v) => v.is_empty(),
//         }
//     }
//
//     fn add_data(&mut self, meta: AggregateMeta, block: DataBlock) -> (isize, usize, usize) {
//         match self {
//             Partitions::Aligned(v) => v.add_data(meta, block),
//             Partitions::Unaligned(v) => v.add_data(meta, block),
//         }
//     }
//
//     pub fn min_partition(&self) -> Option<isize> {
//         match self {
//             Partitions::Unaligned(_) => unreachable!(),
//             Partitions::Aligned(v) => v.data.keys().min().cloned(),
//         }
//     }
//
//
//     pub fn align(&mut self, max_partitions: usize) -> Result<()> {
//         let mut partitions = match self {
//             Partitions::Aligned(_) => {
//                 return Ok(());
//             }
//             Partitions::Unaligned(v) => Self::create_unaligned(v.params.clone()),
//         };
//
//         std::mem::swap(self, &mut partitions);
//
//         *self = match partitions {
//             Partitions::Aligned(_) => unreachable!(),
//             Partitions::Unaligned(v) => Partitions::Aligned(v.align(max_partitions)?),
//         };
//
//         Ok(())
//     }
// }

#[cfg(test)]
mod tests {
    // use databend_common_expression::types::DataType;
    // use databend_common_expression::types::NumberDataType;
    // use databend_common_expression::DataField;
    // use databend_common_expression::DataSchemaRefExt;
    // use databend_common_functions::aggregates::AggregateFunctionFactory;
    //
    // use crate::pipelines::processors::transforms::aggregator::transform_partition_bucket::UnalignedPartitions;
    // use crate::pipelines::processors::transforms::aggregator::AggregatorParams;

    // fn create_unaligned_partitions() -> UnalignedPartitions {
    //     let schema = DataSchemaRefExt::create(vec![
    //         DataField::new("a", DataType::Number(NumberDataType::Int16)),
    //         DataField::new("b", DataType::Number(NumberDataType::Float32)),
    //         DataField::new("c", DataType::String),
    //     ]);
    //
    //     let aggregate_functions = vec![AggregateFunctionFactory::instance()
    //         .get("count", vec![], vec![], vec![])
    //         .unwrap()];
    //
    //     let params = AggregatorParams::try_create(
    //         schema,
    //         vec![
    //             DataType::Number(NumberDataType::Int16),
    //             DataType::Number(NumberDataType::Float32),
    //             DataType::String,
    //         ],
    //         &[0, 1, 2],
    //         &aggregate_functions,
    //         &[],
    //         true,
    //         false,
    //         1024,
    //         1024,
    //     );
    //
    //     UnalignedPartitions::create(params.unwrap())
    // }

    // #[test]
    // fn test_add_data_spilled_payload() {
    //     let mut partitions = create_unaligned_partitions();
    //     let max_partition = 5;
    //     let meta = AggregateMeta::SpilledPayload(SpilledPayload {
    //         partition: 0,
    //         location: "".to_string(),
    //         data_range: Default::default(),
    //         destination_node: "".to_string(),
    //         max_partition: max_partition,
    //     });
    //
    //     let result = partitions.add_data(meta, DataBlock::empty(), -1);
    //
    //     assert_eq!(result, (SINGLE_LEVEL_BUCKET_NUM, max_partition));
    //     assert_eq!(partitions.data.get(&max_partition).unwrap().len(), 1);
    // }
    //
    // #[test]
    // fn test_add_data_in_flight_payload() {
    //     let mut partitions = create_unaligned_partitions();
    //     let partition = 2;
    //     let max_partition = 8;
    //     let meta = AggregateMeta::InFlightPayload(InFlightPayload {
    //         partition,
    //         max_partition,
    //     });
    //
    //     let result = partitions.add_data(meta, DataBlock::empty(), -1);
    //
    //     assert_eq!(result, (partition, max_partition));
    //     assert_eq!(partitions.data.get(&max_partition).unwrap().len(), 1);
    // }

    #[test]
    fn test_add_data_aggregate_payload() {
        // let mut partitions = create_unaligned_partitions();
        // let partition = 3;
        // let max_partition = 10;
        // // Payload::new()
        // let meta = AggregateMeta::AggregatePayload(AggregatePayload {
        //     partition,
        //     // payload: Payload {},
        //     max_partition_count: max_partition,
        // });
        //
        // let result = partitions.add_data(meta, DataBlock::empty());
        //
        // assert_eq!(result, (partition, max_partition));
        // assert_eq!(partitions.data.get(&max_partition).unwrap().len(), 1);
    }

    // #[test]
    // fn test_multiple_inserts_same_partition() {
    //     let mut container = YourContainerStruct::new();
    //     let max_partition = 5;
    //
    //     let meta1 = AggregateMeta::SpilledPayload(SpilledPayload {
    //         max_partition_count: max_partition,
    //         // ...
    //     });
    //     container.add_data(meta1, DataBlock);
    //
    //     let meta2 = AggregateMeta::SpilledPayload(SpilledPayload {
    //         max_partition_count: max_partition,
    //         // ...
    //     });
    //     container.add_data(meta2, DataBlock);
    //
    //     assert_eq!(container.data.get(&max_partition).unwrap().len(), 2);
    // }
}
