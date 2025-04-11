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
use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_base::base::tokio::sync::Barrier;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;

use crate::processors::Event;
use crate::processors::EventCause;
use crate::processors::InputPort;
use crate::processors::OutputPort;
use crate::processors::Processor;
use crate::processors::ProcessorPtr;

#[derive(Eq, PartialEq)]
pub enum MultiwayStrategy {
    Random,
    Custom,
}

pub trait Exchange: Send + Sync + 'static {
    const NAME: &'static str;
    const MULTIWAY_SORT: bool = false;
    const SKIP_EMPTY_DATA_BLOCK: bool = false;

    fn partition(&self, data_block: DataBlock, n: usize) -> Result<Vec<DataBlock>>;

    fn init_way(&self, _index: usize, _first_data: &DataBlock) -> Result<()> {
        Ok(())
    }

    fn sorting_function(_: &DataBlock, _: &DataBlock) -> Ordering {
        unimplemented!()
    }

    fn multiway_pick(&self, data_blocks: &mut [Option<DataBlock>]) -> Option<usize> {
        let position =
            data_blocks
                .iter()
                .enumerate()
                .filter_map(|(idx, x)| x.as_ref().map(|d| (idx, d)))
                .min_by(|(left_idx, left_block), (right_idx, right_block)| {
                    match Self::sorting_function(left_block, right_block) {
                        Ordering::Less => Ordering::Less,
                        Ordering::Greater => Ordering::Greater,
                        Ordering::Equal => left_idx.cmp(right_idx),
                    }
                });

        position.map(|(idx, _)| idx)
    }
}

pub struct ShuffleProcessor {
    input2output: Vec<usize>,
    output2input: Vec<usize>,

    finished_port: usize,
    inputs: Vec<(bool, Arc<InputPort>)>,
    outputs: Vec<(bool, Arc<OutputPort>)>,
}

impl ShuffleProcessor {
    pub fn create(
        inputs: Vec<Arc<InputPort>>,
        outputs: Vec<Arc<OutputPort>>,
        edges: Vec<usize>,
    ) -> Self {
        let len = edges.len();
        debug_assert!({
            let mut sorted = edges.clone();
            sorted.sort();
            let expected = (0..len).collect::<Vec<_>>();
            sorted == expected
        });

        let mut input2output = vec![0_usize; edges.len()];
        let mut output2input = vec![0_usize; edges.len()];

        for (input, output) in edges.into_iter().enumerate() {
            input2output[input] = output;
            output2input[output] = input;
        }

        ShuffleProcessor {
            input2output,
            output2input,
            finished_port: 0,
            inputs: inputs.into_iter().map(|x| (false, x)).collect(),
            outputs: outputs.into_iter().map(|x| (false, x)).collect(),
        }
    }
}

#[async_trait::async_trait]
impl Processor for ShuffleProcessor {
    fn name(&self) -> String {
        "Shuffle".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event_with_cause(&mut self, cause: EventCause) -> Result<Event> {
        let ((input_finished, input), (output_finished, output)) = match cause {
            EventCause::Other => unreachable!(),
            EventCause::Input(index) => (
                &mut self.inputs[index],
                &mut self.outputs[self.input2output[index]],
            ),
            EventCause::Output(index) => (
                &mut self.inputs[self.output2input[index]],
                &mut self.outputs[index],
            ),
        };

        if output.is_finished() {
            input.finish();

            if !*input_finished {
                *input_finished = true;
                self.finished_port += 1;
            }

            if !*output_finished {
                *output_finished = true;
                self.finished_port += 1;
            }

            return match self.finished_port == (self.inputs.len() + self.outputs.len()) {
                true => Ok(Event::Finished),
                false => Ok(Event::NeedConsume),
            };
        }

        if !output.can_push() {
            input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if input.has_data() {
            output.push_data(input.pull_data().unwrap());
            return Ok(Event::NeedConsume);
        }

        if input.is_finished() {
            output.finish();

            if !*input_finished {
                *input_finished = true;
                self.finished_port += 1;
            }

            if !*output_finished {
                *output_finished = true;
                self.finished_port += 1;
            }

            return match self.finished_port == (self.inputs.len() + self.outputs.len()) {
                true => Ok(Event::Finished),
                false => Ok(Event::NeedConsume),
            };
        }

        input.set_need_data();
        Ok(Event::NeedData)
    }
}

pub struct PartitionProcessor<T: Exchange> {
    input: Arc<InputPort>,
    outputs: Vec<Arc<OutputPort>>,

    exchange: Arc<T>,
    input_data: Option<DataBlock>,
    partitioned_data: Vec<Option<DataBlock>>,

    index: usize,
    initialized: bool,
    barrier: Arc<Barrier>,
}

impl<T: Exchange> PartitionProcessor<T> {
    pub fn create(
        input: Arc<InputPort>,
        outputs: Vec<Arc<OutputPort>>,
        exchange: Arc<T>,
        index: usize,
        barrier: Arc<Barrier>,
    ) -> ProcessorPtr {
        let partitioned_data = vec![None; outputs.len()];
        ProcessorPtr::create(Box::new(PartitionProcessor {
            input,
            outputs,
            exchange,
            partitioned_data,
            input_data: None,
            initialized: !T::MULTIWAY_SORT,
            index,
            barrier,
        }))
    }
}

#[async_trait::async_trait]
impl<T: Exchange> Processor for PartitionProcessor<T> {
    fn name(&self) -> String {
        format!("ShufflePartition({})", T::NAME)
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        let mut all_output_finished = true;
        let mut all_data_pushed_output = true;

        for (index, output) in self.outputs.iter().enumerate() {
            if output.is_finished() {
                self.partitioned_data[index].take();
                continue;
            }

            all_output_finished = false;

            if output.can_push() {
                if let Some(block) = self.partitioned_data[index].take() {
                    if !block.is_empty() || block.get_meta().is_some() {
                        output.push_data(Ok(block));
                    }

                    continue;
                }
            }

            if !output.can_push() || self.partitioned_data[index].is_some() {
                all_data_pushed_output = false;
            }
        }

        if all_output_finished {
            self.input.finish();

            return match self.initialized {
                true => Ok(Event::Finished),
                false => Ok(Event::Async),
            };
        }

        if !all_data_pushed_output {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if self.input_data.is_some() {
            return match self.initialized {
                true => Ok(Event::Sync),
                false => Ok(Event::Async),
            };
        }

        if self.input.has_data() {
            self.input_data = Some(self.input.pull_data().unwrap()?);

            return match self.initialized {
                true => Ok(Event::Sync),
                false => Ok(Event::Async),
            };
        }

        if self.input.is_finished() {
            for output in &self.outputs {
                output.finish();
            }

            return match self.initialized {
                true => Ok(Event::Finished),
                false => Ok(Event::Async),
            };
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(block) = self.input_data.take() {
            if T::SKIP_EMPTY_DATA_BLOCK && block.is_empty() {
                return Ok(());
            }

            let partitioned = self.exchange.partition(block, self.outputs.len())?;

            if partitioned.is_empty() {
                return Ok(());
            }

            assert_eq!(partitioned.len(), self.outputs.len());
            for (index, block) in partitioned.into_iter().enumerate() {
                self.partitioned_data[index] = Some(block);
            }
        }

        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        self.initialized = true;
        if let Some(data_block) = self.input_data.as_ref() {
            self.exchange.init_way(self.index, data_block)?;
        }

        self.barrier.wait().await;
        Ok(())
    }
}

pub struct OnePartitionProcessor<T: Exchange> {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    exchange: Arc<T>,
    input_data: Option<DataBlock>,

    index: usize,
    initialized: bool,
    barrier: Arc<Barrier>,
}

impl<T: Exchange> OnePartitionProcessor<T> {
    pub fn create(
        input: Arc<InputPort>,
        outputs: Arc<OutputPort>,
        exchange: Arc<T>,
        index: usize,
        barrier: Arc<Barrier>,
    ) -> ProcessorPtr {
        ProcessorPtr::create(Box::new(OnePartitionProcessor {
            input,
            output: outputs,
            exchange,
            input_data: None,
            initialized: !T::MULTIWAY_SORT,
            index,
            barrier,
        }))
    }
}

#[async_trait::async_trait]
impl<T: Exchange> Processor for OnePartitionProcessor<T> {
    fn name(&self) -> String {
        format!("ShufflePartition({})", T::NAME)
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();

            return match self.initialized {
                true => Ok(Event::Finished),
                false => Ok(Event::Async),
            };
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if self.input_data.is_some() {
            if !self.initialized {
                return Ok(Event::Async);
            }

            let block = self.input_data.take().unwrap();
            let mut partitioned_data = self.exchange.partition(block, 1)?;

            if let Some(block) = partitioned_data.pop() {
                debug_assert!(partitioned_data.is_empty());
                self.output.push_data(Ok(block));
                return Ok(Event::NeedConsume);
            }
        }

        if self.input.has_data() {
            if !self.initialized {
                self.input_data = Some(self.input.pull_data().unwrap()?);
                return Ok(Event::Async);
            }

            let data_block = self.input.pull_data().unwrap()?;
            let mut partitioned_data = self.exchange.partition(data_block, 1)?;

            if let Some(block) = partitioned_data.pop() {
                debug_assert!(partitioned_data.is_empty());
                self.output.push_data(Ok(block));
                return Ok(Event::NeedConsume);
            }
        }

        if self.input.is_finished() {
            self.output.finish();

            return match self.initialized {
                true => Ok(Event::Finished),
                false => Ok(Event::Async),
            };
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    async fn async_process(&mut self) -> Result<()> {
        self.initialized = true;
        if let Some(data_block) = self.input_data.as_ref() {
            self.exchange.init_way(self.index, data_block)?;
        }

        self.barrier.wait().await;
        Ok(())
    }
}

#[derive(Clone, PartialEq)]
enum PortStatus {
    Idle,
    HasData,
    Finished,
}

pub struct MergePartitionProcessor<T: Exchange> {
    output: Arc<OutputPort>,
    inputs: Vec<Arc<InputPort>>,
    inputs_data: Vec<Option<DataBlock>>,
    exchange: Arc<T>,

    initialize: bool,
    finished_inputs: usize,
    waiting_inputs: VecDeque<usize>,
    wakeup_inputs: VecDeque<usize>,
    inputs_status: Vec<PortStatus>,
}

impl<T: Exchange> MergePartitionProcessor<T> {
    pub fn create(
        inputs: Vec<Arc<InputPort>>,
        output: Arc<OutputPort>,
        exchange: Arc<T>,
    ) -> ProcessorPtr {
        let inputs_data = vec![None; inputs.len()];
        let inputs_status = vec![PortStatus::Idle; inputs.len()];
        let waiting_inputs = VecDeque::with_capacity(inputs.len());
        let wakeup_inputs = VecDeque::with_capacity(inputs.len());

        ProcessorPtr::create(Box::new(MergePartitionProcessor::<T> {
            output,
            inputs,
            inputs_data,
            exchange,
            inputs_status,
            waiting_inputs,
            initialize: false,
            finished_inputs: 0,
            wakeup_inputs,
        }))
    }
}

impl<T: Exchange> Processor for MergePartitionProcessor<T> {
    fn name(&self) -> String {
        match T::MULTIWAY_SORT {
            true => format!("ShuffleSortMergePartition({})", T::NAME),
            false => format!("ShuffleMergePartition({})", T::NAME),
        }
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            for input in &self.inputs {
                input.finish();
            }

            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        let mut all_inputs_finished = true;
        let mut need_pick_block_to_push = true;
        for (index, input) in self.inputs.iter().enumerate() {
            if input.is_finished() {
                continue;
            }

            all_inputs_finished = false;

            if input.has_data() && self.inputs_data[index].is_none() {
                self.inputs_data[index] = Some(input.pull_data().unwrap()?);
            }

            if self.inputs_data[index].is_none() {
                need_pick_block_to_push = false;
            }

            input.set_need_data();
        }

        if need_pick_block_to_push {
            if let Some(pick_index) = self.exchange.multiway_pick(&mut self.inputs_data) {
                if let Some(block) = self.inputs_data[pick_index].take() {
                    self.output.push_data(Ok(block));
                    return Ok(Event::NeedConsume);
                }
            }
        }

        if all_inputs_finished {
            self.output.finish();
            return Ok(Event::Finished);
        }

        Ok(Event::NeedData)
    }

    fn event_with_cause(&mut self, cause: EventCause) -> Result<Event> {
        if T::MULTIWAY_SORT {
            return self.event();
        }

        if let EventCause::Output(_) = cause {
            if self.output.is_finished() {
                for input in &self.inputs {
                    input.finish();
                }

                return Ok(Event::Finished);
            }

            if !self.output.can_push() {
                return Ok(Event::NeedConsume);
            }

            while let Some(idx) = self.wakeup_inputs.pop_front() {
                self.inputs[idx].set_need_data();
            }
        }

        if !self.initialize && self.waiting_inputs.is_empty() {
            self.initialize = true;

            for input in &self.inputs {
                input.set_need_data();
            }

            return Ok(Event::NeedData);
        }

        if let EventCause::Input(idx) = cause {
            if self.inputs[idx].is_finished() && self.inputs_status[idx] != PortStatus::Finished {
                self.finished_inputs += 1;
                self.inputs_status[idx] = PortStatus::Finished;
            }

            if self.inputs[idx].has_data() && self.inputs_status[idx] != PortStatus::HasData {
                self.waiting_inputs.push_back(idx);
                self.inputs_status[idx] = PortStatus::HasData;
            }
        }

        if self.finished_inputs == self.inputs.len() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        while !self.waiting_inputs.is_empty() && self.output.can_push() {
            let idx = self.waiting_inputs.pop_front().unwrap();
            self.output.push_data(self.inputs[idx].pull_data().unwrap());
            self.inputs_status[idx] = PortStatus::Idle;

            if self.inputs[idx].is_finished() {
                if self.inputs_status[idx] != PortStatus::Finished {
                    self.finished_inputs += 1;
                    self.inputs_status[idx] = PortStatus::Finished;
                }

                continue;
            }

            self.wakeup_inputs.push_back(idx);
        }

        match self.waiting_inputs.is_empty() {
            true => Ok(Event::NeedData),
            false => Ok(Event::NeedConsume),
        }
    }
}
