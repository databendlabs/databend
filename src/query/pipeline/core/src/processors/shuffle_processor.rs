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
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;

use crate::processors::Event;
use crate::processors::EventCause;
use crate::processors::InputPort;
use crate::processors::OutputPort;
use crate::processors::Processor;
use crate::processors::ProcessorPtr;

pub enum MultiwayStrategy {
    Random,
    Custom,
}

pub trait Exchange: Send + Sync + 'static {
    const NAME: &'static str;
    const SKIP_EMPTY_DATA_BLOCK: bool = false;
    const STRATEGY: MultiwayStrategy = MultiwayStrategy::Random;

    fn partition(&self, data_block: DataBlock, n: usize) -> Result<Vec<DataBlock>>;

    fn multiway_pick(&self, _partitions: &[Option<DataBlock>]) -> Result<usize> {
        unimplemented!()
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
}

impl<T: Exchange> PartitionProcessor<T> {
    pub fn create(
        input: Arc<InputPort>,
        outputs: Vec<Arc<OutputPort>>,
        exchange: Arc<T>,
    ) -> ProcessorPtr {
        let partitioned_data = vec![None; outputs.len()];
        ProcessorPtr::create(Box::new(PartitionProcessor {
            input,
            outputs,
            exchange,
            partitioned_data,
            input_data: None,
        }))
    }
}

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
                    output.push_data(Ok(block));

                    continue;
                }
            }

            if self.partitioned_data[index].is_some() {
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
            self.input_data = Some(self.input.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            for output in &self.outputs {
                output.finish();
            }

            return Ok(Event::Finished);
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

            for (index, block) in partitioned.into_iter().enumerate() {
                if block.is_empty() && block.get_meta().is_none() {
                    continue;
                }

                self.partitioned_data[index] = Some(block);
            }
        }

        Ok(())
    }
}

pub struct MergePartitionProcessor<T: Exchange> {
    exchange: Arc<T>,

    output: Arc<OutputPort>,
    inputs: Vec<Arc<InputPort>>,
    inputs_data: Vec<Option<DataBlock>>,
}

impl<T: Exchange> MergePartitionProcessor<T> {
    pub fn create(
        inputs: Vec<Arc<InputPort>>,
        output: Arc<OutputPort>,
        exchange: Arc<T>,
    ) -> ProcessorPtr {
        let inputs_data = vec![None; inputs.len()];
        ProcessorPtr::create(Box::new(MergePartitionProcessor {
            output,
            inputs,
            exchange,
            inputs_data,
        }))
    }
}

impl<T: Exchange> Processor for MergePartitionProcessor<T> {
    fn name(&self) -> String {
        format!("ShuffleMergePartition({})", T::NAME)
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
        let mut need_pick_block_to_push = matches!(T::STRATEGY, MultiwayStrategy::Custom);

        for (index, input) in self.inputs.iter().enumerate() {
            if input.is_finished() {
                continue;
            }

            all_inputs_finished = false;

            if input.has_data() {
                match T::STRATEGY {
                    MultiwayStrategy::Random => {
                        if self.output.can_push() {
                            self.output.push_data(Ok(input.pull_data().unwrap()?));
                        }
                    }
                    MultiwayStrategy::Custom => {
                        if self.inputs_data[index].is_none() {
                            self.inputs_data[index] = Some(input.pull_data().unwrap()?);
                        }
                    }
                }
            }

            if self.inputs_data[index].is_none() {
                need_pick_block_to_push = false;
            }

            input.set_need_data();
        }

        if all_inputs_finished
            && (!matches!(T::STRATEGY, MultiwayStrategy::Custom)
                || self.inputs_data.iter().all(Option::is_none))
        {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if need_pick_block_to_push {
            let pick_index = self.exchange.multiway_pick(&self.inputs_data)?;

            if let Some(block) = self.inputs_data[pick_index].take() {
                self.output.push_data(Ok(block));
                return Ok(Event::NeedConsume);
            }

            if all_inputs_finished && self.inputs_data.iter().all(Option::is_none) {
                self.output.finish();
                return Ok(Event::Finished);
            }
        }

        Ok(Event::NeedData)
    }
}
