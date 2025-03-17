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
use std::collections::hash_map::Entry;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Formatter;
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
use super::TransformAggregateSpillReader;
use super::TransformFinalAggregate;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;

static SINGLE_LEVEL_BUCKET_NUM: isize = -1;

pub struct TransformPartitionDispatch {
    outputs: Vec<Arc<OutputPort>>,
    input: Arc<InputPort>,
    outputs_data: Vec<VecDeque<DataBlock>>,
    output_index: usize,
    initialized_input: bool,

    max_partition: usize,
    working_partition: isize,
    partitions: Partitions,
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
            outputs,
            outputs_data,
            input: InputPort::create(),
            output_index: 0,
            max_partition: 0,
            initialized_input: false,
            partitions: Partitions::create_unaligned(params),
            working_partition: 0,
        })
    }

    pub fn get_input(&self) -> Arc<InputPort> {
        self.input.clone()
    }

    pub fn get_outputs(&self) -> Vec<Arc<OutputPort>> {
        self.outputs.clone()
    }

    // Align each input's max_partition to the maximum max_partition.
    // If an input's max_partition is smaller than the maximum, continuously fetch its data until either the stream ends or its max_partition reaches/exceeds the maximum value.
    fn initialize_input(&mut self) -> Result<bool> {
        if self.input.is_finished() {
            return Ok(true);
        }

        if !self.input.has_data() {
            self.input.set_need_data();
            return Ok(false);
        }

        let data_block = self.input.pull_data().unwrap()?;
        let (partition, max_partition, global_max_partition) =
            self.partitions.add_block(data_block)?;
        self.max_partition = global_max_partition;
        Ok(partition > SINGLE_LEVEL_BUCKET_NUM && max_partition == global_max_partition)
    }

    fn ready_partition(&mut self) -> Option<isize> {
        let storage_min_partition = self.partitions.min_partition()?;

        if storage_min_partition >= self.working_partition {
            return None;
        }

        Some(storage_min_partition)
    }

    fn fetch_ready_partition(&mut self) -> Result<()> {
        while let Some(ready_partition_id) = self.ready_partition() {
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
        let mut all_output_finished = true;

        for output in &self.outputs {
            if !output.is_finished() {
                all_output_finished = false;
                break;
            }
        }

        if all_output_finished {
            self.input.finish();
            return Ok(Event::Finished);
        }

        // We pull the first unsplitted data block
        if !self.initialized_input {
            if self.initialize_input()? {
                return Ok(Event::Sync);
            }

            self.input.set_need_data();
            return Ok(Event::NeedData);
        }

        let mut output_can_push = false;
        for (idx, output) in self.outputs.iter().enumerate() {
            if output.can_push() {
                output_can_push = true;
                if let Some(block) = self.outputs_data[idx].pop_front() {
                    output.push_data(Ok(block));
                }
            }
        }

        if !output_can_push {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if self.input.is_finished() {
            self.working_partition = self.max_partition as isize;
            self.fetch_ready_partition()?;
        }

        if self.input.has_data() {
            let data_block = self.input.pull_data().unwrap()?;
            let (partition, _, _) = self.partitions.add_block(data_block)?;

            if partition != self.working_partition {
                // ready partition
                self.fetch_ready_partition()?;
                self.working_partition = partition;
            }
        }

        self.input.set_need_data();

        let mut has_data = false;
        for (idx, output) in self.outputs.iter().enumerate() {
            if self.outputs_data[idx].is_empty() {
                continue;
            }

            if output.can_push() {
                if let Some(block) = self.outputs_data[idx].pop_front() {
                    output.push_data(Ok(block));
                }
            }

            has_data |= !self.outputs_data[idx].is_empty();
        }

        if self.input.is_finished() && !has_data {
            for output in &self.outputs {
                output.finish();
            }

            return Ok(Event::Finished);
        }

        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if !self.initialized_input {
            self.initialized_input = true;
            return self.partitions.align(self.max_partition);
        }

        Ok(())
    }
}

struct ResortingPartition {
    global_max_partition: AtomicUsize,
}

impl ResortingPartition {
    fn block_number(meta: &AggregateMeta) -> (isize, usize) {
        match meta {
            AggregateMeta::Serialized(v) => (v.bucket, v.max_partition),
            AggregateMeta::SpilledPayload(v) => (v.partition, v.max_partition),
            AggregateMeta::AggregatePayload(v) => (v.partition, v.max_partition),
            AggregateMeta::InFlightPayload(v) => (v.partition, v.max_partition),
            AggregateMeta::FinalPartition => unreachable!(),
        }
    }

    fn get_global_max_partition(data_blocks: &[Option<DataBlock>]) -> usize {
        let mut global_max_partition = 0;

        for data_block in data_blocks {
            let Some(data_block) = data_block else {
                continue;
            };

            let Some(meta) = data_block.get_meta() else {
                continue;
            };
            let Some(meta) = AggregateMeta::downcast_ref_from(meta) else {
                continue;
            };

            global_max_partition = global_max_partition.max(meta.get_global_max_partition())
        }

        global_max_partition
    }
}

impl Exchange for ResortingPartition {
    const NAME: &'static str = "PartitionResorting";
    const MULTIWAY_SORT: bool = true;

    fn partition(&self, data_block: DataBlock, n: usize) -> Result<Vec<DataBlock>> {
        debug_assert_eq!(n, 1);
        Ok(vec![data_block])
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

    fn multiway_pick(&self, data_blocks: &mut [Option<DataBlock>]) -> Option<usize> {
        let new_value = Self::get_global_max_partition(data_blocks);
        let old_value = self
            .global_max_partition
            .fetch_max(new_value, AtomicOrdering::SeqCst);

        let global_max_partition = std::cmp::max(new_value, old_value);

        let min_position = data_blocks
            .iter()
            .enumerate()
            .filter_map(|(i, x)| x.as_ref().map(|x| (i, x)))
            .min_by(|(left_idx, left_block), (right_idx, right_block)| {
                match ResortingPartition::sorting_function(left_block, right_block) {
                    Ordering::Less => Ordering::Less,
                    Ordering::Greater => Ordering::Greater,
                    Ordering::Equal => left_idx.cmp(right_idx),
                }
            })
            .map(|(idx, _)| idx);

        if let Some(min_pos) = min_position {
            if global_max_partition == 0 {
                return Some(min_pos);
            }

            if let Some(mut block) = data_blocks[min_pos].take() {
                let mut meta = AggregateMeta::downcast_from(block.take_meta().unwrap()).unwrap();
                meta.set_global_max_partition(global_max_partition);
                data_blocks[min_pos] = Some(block.add_meta(Some(Box::new(meta))).unwrap());
            }
        }

        min_position
    }
}

pub fn build_partition_bucket(
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
        TransformAggregateSpillReader::create(
            input,
            output,
            operator,
            semaphore.clone(),
            params.clone(),
        )
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

struct UnalignedPartitions {
    params: Arc<AggregatorParams>,
    data: HashMap<usize, Vec<(AggregateMeta, DataBlock)>>,
}

impl Debug for UnalignedPartitions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnalignedPartitions")
            .field("data", &self.data)
            .finish()
    }
}

impl UnalignedPartitions {
    pub fn create(params: Arc<AggregatorParams>) -> UnalignedPartitions {
        UnalignedPartitions {
            params,
            data: HashMap::new(),
        }
    }

    fn insert_data(&mut self, idx: usize, meta: AggregateMeta, block: DataBlock) {
        match self.data.entry(idx) {
            Entry::Vacant(v) => {
                v.insert(vec![(meta, block)]);
            }
            Entry::Occupied(mut v) => {
                v.get_mut().push((meta, block));
            }
        }
    }

    pub fn add_data(&mut self, meta: AggregateMeta, block: DataBlock) -> (isize, usize, usize) {
        match &meta {
            AggregateMeta::Serialized(_) => unreachable!(),
            AggregateMeta::FinalPartition => unreachable!(),
            AggregateMeta::SpilledPayload(payload) => {
                let max_partition = payload.max_partition;
                let global_max_partition = payload.global_max_partition;
                self.insert_data(max_partition, meta, block);

                (SINGLE_LEVEL_BUCKET_NUM, max_partition, global_max_partition)
            }
            AggregateMeta::InFlightPayload(payload) => {
                let partition = payload.partition;
                let max_partition = payload.max_partition;
                let global_max_partition = payload.global_max_partition;
                self.insert_data(max_partition, meta, block);

                (partition, max_partition, global_max_partition)
            }
            AggregateMeta::AggregatePayload(payload) => {
                let partition = payload.partition;
                let max_partition = payload.max_partition;
                let global_max_partition = payload.global_max_partition;

                self.insert_data(max_partition, meta, block);
                (partition, max_partition, global_max_partition)
            }
        }
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
                global_max_partition: 0,
            });
        }

        partitioned
    }

    pub fn align(mut self, max_partitions: usize) -> Result<AlignedPartitions> {
        let repartition_data = self
            .data
            .extract_if(|k, _| *k != max_partitions)
            .collect::<Vec<_>>();

        let mut aligned_partitions = AlignedPartitions {
            max_partition: max_partitions,
            data: BTreeMap::new(),
        };

        for (_max_partition, data) in std::mem::take(&mut self.data) {
            for (meta, block) in data {
                aligned_partitions.add_data(meta, block);
            }
        }

        for (_, repartition_data) in repartition_data {
            for (meta, block) in repartition_data {
                match meta {
                    AggregateMeta::Serialized(_) => unreachable!(),
                    AggregateMeta::FinalPartition => unreachable!(),
                    AggregateMeta::SpilledPayload(_) => unreachable!(),
                    AggregateMeta::InFlightPayload(payload) => {
                        if block.is_empty() {
                            continue;
                        }

                        let payload = AggregatePayload {
                            partition: payload.partition,
                            max_partition: payload.max_partition,
                            payload: self.deserialize_flight(block)?,
                            global_max_partition: 0,
                        };

                        let partitioned = self.partition_payload(payload, max_partitions);

                        for payload in partitioned {
                            aligned_partitions.add_data(
                                AggregateMeta::AggregatePayload(payload),
                                DataBlock::empty(),
                            );
                        }
                    }
                    AggregateMeta::AggregatePayload(payload) => {
                        if payload.payload.len() == 0 {
                            continue;
                        }

                        let partitioned = self.partition_payload(payload, max_partitions);
                        for payload in partitioned {
                            aligned_partitions.add_data(
                                AggregateMeta::AggregatePayload(payload),
                                DataBlock::empty(),
                            );
                        }
                    }
                }
            }
        }

        Ok(aligned_partitions)
    }
}

#[derive(Debug)]
struct AlignedPartitions {
    max_partition: usize,
    data: BTreeMap<isize, Vec<(AggregateMeta, DataBlock)>>,
}

impl AlignedPartitions {
    pub fn add_data(&mut self, meta: AggregateMeta, block: DataBlock) -> (isize, usize, usize) {
        let (partition, max_partition, global_max_partition) = match &meta {
            AggregateMeta::Serialized(_) => unreachable!(),
            AggregateMeta::FinalPartition => unreachable!(),
            AggregateMeta::SpilledPayload(v) => {
                (v.partition, v.max_partition, v.global_max_partition)
            }
            AggregateMeta::AggregatePayload(v) => {
                (v.partition, v.max_partition, v.global_max_partition)
            }
            AggregateMeta::InFlightPayload(v) => {
                (v.partition, v.max_partition, v.global_max_partition)
            }
        };

        assert_eq!(max_partition, self.max_partition);
        match self.data.entry(partition) {
            std::collections::btree_map::Entry::Vacant(v) => {
                v.insert(vec![(meta, block)]);
            }
            std::collections::btree_map::Entry::Occupied(mut v) => {
                v.get_mut().push((meta, block));
            }
        }

        (partition, max_partition, global_max_partition)
    }
}

#[derive(Debug)]
enum Partitions {
    Aligned(AlignedPartitions),
    Unaligned(UnalignedPartitions),
}

impl Partitions {
    pub fn create_unaligned(params: Arc<AggregatorParams>) -> Partitions {
        Partitions::Unaligned(UnalignedPartitions::create(params))
    }

    // pub fn is_empty(&self) -> bool {
    //     match self {
    //         Partitions::Aligned(c) => {}
    //     }
    // }

    fn add_data(&mut self, meta: AggregateMeta, block: DataBlock) -> (isize, usize, usize) {
        match self {
            Partitions::Aligned(v) => v.add_data(meta, block),
            Partitions::Unaligned(v) => v.add_data(meta, block),
        }
    }

    pub fn add_block(&mut self, mut block: DataBlock) -> Result<(isize, usize, usize)> {
        let Some(meta) = block.take_meta() else {
            return Err(ErrorCode::Internal(
                "Internal, TransformPartitionBucket only recv DataBlock with meta.",
            ));
        };

        let Some(meta) = AggregateMeta::downcast_from(meta) else {
            return Err(ErrorCode::Internal(
                "Internal, TransformPartitionBucket only recv AggregateMeta".to_string(),
            ));
        };

        Ok(self.add_data(meta, block))
    }

    pub fn min_partition(&self) -> Option<isize> {
        match self {
            Partitions::Unaligned(_) => unreachable!(),
            Partitions::Aligned(v) => v.data.keys().min().cloned(),
        }
    }

    pub fn take_partition(&mut self, partition: isize) -> Vec<(AggregateMeta, DataBlock)> {
        match self {
            Partitions::Unaligned(_) => unreachable!(),
            Partitions::Aligned(v) => v.data.remove(&partition).unwrap_or_default(),
        }
    }

    pub fn align(&mut self, max_partitions: usize) -> Result<()> {
        let mut partitions = match self {
            Partitions::Aligned(_) => {
                return Ok(());
            }
            Partitions::Unaligned(v) => Self::create_unaligned(v.params.clone()),
        };

        std::mem::swap(self, &mut partitions);

        *self = match partitions {
            Partitions::Aligned(_) => unreachable!(),
            Partitions::Unaligned(v) => Partitions::Aligned(v.align(max_partitions)?),
        };

        Ok(())
    }
}

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
