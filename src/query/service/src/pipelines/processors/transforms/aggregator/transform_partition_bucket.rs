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
use std::collections::hash_map::Entry;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
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
static MAX_PARTITION_COUNT: usize = 128;

struct InputPortState {
    port: Arc<InputPort>,
    partition: isize,
    max_partition: usize,
}

impl Debug for InputPortState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InputPortState")
            .field("bucket", &self.partition)
            .field("max_partition_count", &self.max_partition)
            .finish()
    }
}

pub struct TransformPartitionDispatch {
    output: Arc<OutputPort>,
    inputs: Vec<InputPortState>,
    max_partition: usize,
    initialized_all_inputs: bool,
    partitions: Partitions,
}

impl TransformPartitionDispatch {
    pub fn create(input_nums: usize, params: Arc<AggregatorParams>) -> Result<Self> {
        let mut inputs = Vec::with_capacity(input_nums);

        for _index in 0..input_nums {
            inputs.push(InputPortState {
                partition: -1,
                port: InputPort::create(),
                max_partition: 0,
            });
        }

        let max_partition = match params.cluster_aggregator {
            true => MAX_PARTITION_COUNT,
            false => 0,
        };

        Ok(TransformPartitionDispatch {
            inputs,
            max_partition,
            output: OutputPort::create(),
            initialized_all_inputs: false,
            partitions: Partitions::create_unaligned(params),
        })
    }

    pub fn get_inputs(&self) -> Vec<Arc<InputPort>> {
        let mut inputs = Vec::with_capacity(self.inputs.len());

        for input_state in &self.inputs {
            inputs.push(input_state.port.clone());
        }

        inputs
    }

    pub fn get_output(&self) -> Arc<OutputPort> {
        self.output.clone()
    }

    // Align each input's max_partition to the maximum max_partition.
    // If an input's max_partition is smaller than the maximum, continuously fetch its data until either the stream ends or its max_partition reaches/exceeds the maximum value.
    fn initialize_all_inputs(&mut self) -> Result<bool> {
        let mut initialized_all_inputs = true;

        for index in 0..self.inputs.len() {
            if self.inputs[index].port.is_finished() {
                self.inputs[index].partition = self.max_partition as isize;
                continue;
            }

            if self.inputs[index].max_partition > 0
                && self.inputs[index].partition > SINGLE_LEVEL_BUCKET_NUM
                && self.inputs[index].max_partition == self.max_partition
            {
                continue;
            }

            if !self.inputs[index].port.has_data() {
                self.inputs[index].port.set_need_data();
                initialized_all_inputs = false;
                continue;
            }

            let before_max_partition_count = self.max_partition;

            self.add_block(index, self.inputs[index].port.pull_data().unwrap()?)?;

            // we need pull all spill data in init, and data less than max partition
            if self.inputs[index].partition <= SINGLE_LEVEL_BUCKET_NUM
                || self.inputs[index].max_partition < self.max_partition
            {
                self.inputs[index].port.set_need_data();
                initialized_all_inputs = false;
            }

            // max partition count change
            if before_max_partition_count > 0 && before_max_partition_count != self.max_partition {
                // set need data for inputs which is less than the max partition
                for i in 0..index {
                    if !self.inputs[i].port.is_finished()
                        && !self.inputs[i].port.has_data()
                        && self.inputs[i].max_partition != self.max_partition
                    {
                        self.inputs[i].port.set_need_data();
                        initialized_all_inputs = false;
                    }
                }
            }
        }

        Ok(initialized_all_inputs)
    }

    fn add_block(&mut self, index: usize, data_block: DataBlock) -> Result<()> {
        (
            self.inputs[index].partition,
            self.inputs[index].max_partition,
        ) = self.partitions.add_block(data_block)?;

        self.max_partition = std::cmp::max(self.max_partition, self.inputs[index].max_partition);
        Ok(())
    }

    fn ready_partition(&mut self) -> Option<isize> {
        let inputs_min_partition = self.working_partition()?;
        let stroage_min_partition = self.partitions.min_partition()?;

        if stroage_min_partition >= inputs_min_partition {
            return None;
        }

        Some(stroage_min_partition)
    }

    fn working_partition(&mut self) -> Option<isize> {
        self.inputs.iter().map(|x| x.partition).min()
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
        if self.output.is_finished() {
            for input_state in &self.inputs {
                input_state.port.finish();
            }

            return Ok(Event::Finished);
        }

        // We pull the first unsplitted data block
        if !self.initialized_all_inputs {
            if self.initialize_all_inputs()? {
                return Ok(Event::Sync);
            }

            return Ok(Event::NeedData);
        }

        if !self.output.can_push() {
            for input_state in &self.inputs {
                input_state.port.set_not_need_data();
            }

            return Ok(Event::NeedConsume);
        }

        if let Some(ready_partition) = self.ready_partition() {
            // TODO: read spill data
            let ready_partition = self.partitions.take_partition(ready_partition);
            self.output
                .push_data(Ok(DataBlock::empty_with_meta(AggregateMeta::create_final(
                    ready_partition,
                ))));
        }

        let working_partition = self.working_partition().unwrap_or(0);

        let mut all_inputs_is_finished = true;
        for index in 0..self.inputs.len() {
            if self.inputs[index].port.is_finished() {
                self.inputs[index].partition = self.max_partition as isize;
                continue;
            }

            all_inputs_is_finished = false;

            eprintln!(
                "working partition: {}, input_{}:{}",
                working_partition, index, self.inputs[index].partition
            );

            if self.inputs[index].partition > working_partition {
                continue;
            }

            if !self.inputs[index].port.has_data() {
                self.inputs[index].port.set_need_data();
                continue;
            }

            self.add_block(index, self.inputs[index].port.pull_data().unwrap()?)?;

            if self.inputs[index].partition <= working_partition {
                self.inputs[index].port.set_need_data();
            }
        }

        if let Some(ready_partition) = self.ready_partition() {
            if self.output.can_push() {
                let ready_partition = self.partitions.take_partition(ready_partition);
                self.output
                    .push_data(Ok(DataBlock::empty_with_meta(AggregateMeta::create_final(
                        ready_partition,
                    ))));
            }

            return Ok(Event::NeedConsume);
            // TODO: read spill data
            // TODO: try push
        }

        if all_inputs_is_finished {
            self.output.finish();
            return Ok(Event::Finished);
        }

        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if !self.initialized_all_inputs {
            self.initialized_all_inputs = true;
            return self.partitions.align(self.max_partition);
        }

        Ok(())
    }
}

pub fn build_partition_bucket(
    pipeline: &mut Pipeline,
    params: Arc<AggregatorParams>,
) -> Result<()> {
    let input_nums = pipeline.output_len();
    let transform = TransformPartitionDispatch::create(input_nums, params.clone())?;

    let output = transform.get_output();
    let inputs_port = transform.get_inputs();

    pipeline.add_pipe(Pipe::create(inputs_port.len(), 1, vec![PipeItem::create(
        ProcessorPtr::create(Box::new(transform)),
        inputs_port,
        vec![output],
    )]));

    pipeline.try_resize(input_nums)?;

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

    pub fn add_data(&mut self, meta: AggregateMeta, block: DataBlock) -> (isize, usize) {
        match &meta {
            AggregateMeta::Serialized(_) => unreachable!(),
            AggregateMeta::FinalPayload(_) => unreachable!(),
            AggregateMeta::SpilledPayload(payload) => {
                let max_partition = payload.max_partition_count;
                self.insert_data(max_partition, meta, block);

                (SINGLE_LEVEL_BUCKET_NUM, max_partition)
            }
            AggregateMeta::InFlightPayload(payload) => {
                let partition = payload.partition;
                let max_partition = payload.max_partition;
                self.insert_data(max_partition, meta, block);

                (partition, max_partition)
            }
            AggregateMeta::AggregatePayload(payload) => {
                let partition = payload.partition;
                let max_partition = payload.max_partition_count;
                self.insert_data(max_partition, meta, block);

                (partition, max_partition)
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
        assert_eq!(hashtable.payload.len(), 1);
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
                max_partition_count: to,
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
                    AggregateMeta::FinalPayload(_) => unreachable!(),
                    AggregateMeta::SpilledPayload(_) => unreachable!(),
                    AggregateMeta::InFlightPayload(payload) => {
                        let payload = AggregatePayload {
                            partition: payload.partition,
                            max_partition_count: payload.max_partition,
                            payload: self.deserialize_flight(block)?,
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
    pub fn add_data(&mut self, meta: AggregateMeta, block: DataBlock) -> (isize, usize) {
        let (partition, max_partition) = match &meta {
            AggregateMeta::Serialized(_) => unreachable!(),
            AggregateMeta::FinalPayload(_) => unreachable!(),
            AggregateMeta::SpilledPayload(v) => (v.partition, v.max_partition_count),
            AggregateMeta::AggregatePayload(v) => (v.partition, v.max_partition_count),
            AggregateMeta::InFlightPayload(v) => (v.partition, v.max_partition),
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

        (partition, max_partition)
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

    fn add_data(&mut self, meta: AggregateMeta, block: DataBlock) -> (isize, usize) {
        match self {
            Partitions::Aligned(v) => v.add_data(meta, block),
            Partitions::Unaligned(v) => v.add_data(meta, block),
        }
    }

    pub fn add_block(&mut self, mut block: DataBlock) -> Result<(isize, usize)> {
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
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::DataBlock;
    use databend_common_expression::DataField;
    use databend_common_expression::DataSchemaRefExt;
    use databend_common_functions::aggregates::AggregateFunctionFactory;

    use crate::pipelines::processors::transforms::aggregator::transform_partition_bucket::UnalignedPartitions;
    use crate::pipelines::processors::transforms::aggregator::transform_partition_bucket::SINGLE_LEVEL_BUCKET_NUM;
    use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
    use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
    use crate::pipelines::processors::transforms::aggregator::InFlightPayload;
    use crate::pipelines::processors::transforms::aggregator::SpilledPayload;

    fn create_unaligned_partitions() -> UnalignedPartitions {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("a", DataType::Number(NumberDataType::Int16)),
            DataField::new("b", DataType::Number(NumberDataType::Float32)),
            DataField::new("c", DataType::String),
        ]);

        let aggregate_functions = vec![AggregateFunctionFactory::instance()
            .get("count", vec![], vec![], vec![])
            .unwrap()];

        let params = AggregatorParams::try_create(
            schema,
            vec![
                DataType::Number(NumberDataType::Int16),
                DataType::Number(NumberDataType::Float32),
                DataType::String,
            ],
            &[0, 1, 2],
            &aggregate_functions,
            &[],
            true,
            false,
            1024,
            1024,
        );

        UnalignedPartitions::create(params.unwrap())
    }

    #[test]
    fn test_add_data_spilled_payload() {
        let mut partitions = create_unaligned_partitions();
        let max_partition = 5;
        let meta = AggregateMeta::SpilledPayload(SpilledPayload {
            partition: 0,
            location: "".to_string(),
            data_range: Default::default(),
            destination_node: "".to_string(),
            max_partition_count: max_partition,
        });

        let result = partitions.add_data(meta, DataBlock::empty());

        assert_eq!(result, (SINGLE_LEVEL_BUCKET_NUM, max_partition));
        assert_eq!(partitions.data.get(&max_partition).unwrap().len(), 1);
    }

    #[test]
    fn test_add_data_in_flight_payload() {
        let mut partitions = create_unaligned_partitions();
        let partition = 2;
        let max_partition = 8;
        let meta = AggregateMeta::InFlightPayload(InFlightPayload {
            partition,
            max_partition,
        });

        let result = partitions.add_data(meta, DataBlock::empty());

        assert_eq!(result, (partition, max_partition));
        assert_eq!(partitions.data.get(&max_partition).unwrap().len(), 1);
    }

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
