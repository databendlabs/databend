// Copyright 2023 Datafuse Labs.
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
use std::collections::VecDeque;
use std::sync::Arc;

use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::pipe::Pipe;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_pipeline_core::Pipeline;

use crate::api::rpc::exchange::exchange_params::ShuffleExchangeParams;
use crate::api::rpc::flight_scatter::FlightScatter;

struct OutputBuffer {
    blocks: VecDeque<DataBlock>,
}

impl OutputBuffer {
    pub fn create(capacity: usize) -> OutputBuffer {
        OutputBuffer {
            blocks: VecDeque::with_capacity(capacity),
        }
    }
}

struct OutputsBuffer {
    capacity: usize,
    inner: Vec<OutputBuffer>,
}

impl OutputsBuffer {
    pub fn create(capacity: usize, outputs: usize) -> OutputsBuffer {
        let mut inner = Vec::with_capacity(outputs);

        for _index in 0..outputs {
            inner.push(OutputBuffer::create(capacity))
        }

        OutputsBuffer { inner, capacity }
    }

    pub fn is_empty(&self) -> bool {
        self.inner.iter().all(|x| x.blocks.is_empty())
    }

    pub fn is_fill(&self, index: usize) -> bool {
        self.inner[index].blocks.len() == self.capacity
    }

    pub fn pop(&mut self, index: usize) -> Option<DataBlock> {
        self.inner[index].blocks.pop_front()
    }

    pub fn push_back(&mut self, index: usize, block: DataBlock) {
        self.inner[index].blocks.push_back(block)
    }
}

pub struct ExchangeShuffleTransform {
    inputs: Vec<Arc<InputPort>>,
    outputs: Vec<Arc<OutputPort>>,

    buffer: OutputsBuffer,
    input_data: Option<DataBlock>,
    shuffle_scatter: Arc<Box<dyn FlightScatter>>,

    all_inputs_finished: bool,
    all_outputs_finished: bool,
}

impl ExchangeShuffleTransform {
    pub fn create(
        inputs: usize,
        outputs: usize,
        scatter: Arc<Box<dyn FlightScatter>>,
    ) -> ExchangeShuffleTransform {
        let mut inputs_port = Vec::with_capacity(inputs);
        let mut outputs_port = Vec::with_capacity(outputs);

        for _index in 0..inputs {
            inputs_port.push(InputPort::create());
        }

        for _index in 0..outputs {
            outputs_port.push(OutputPort::create());
        }

        ExchangeShuffleTransform {
            inputs: inputs_port,
            outputs: outputs_port,
            buffer: OutputsBuffer::create(10, outputs),
            input_data: None,
            shuffle_scatter: scatter,
            all_inputs_finished: false,
            all_outputs_finished: false,
        }
    }

    pub fn get_inputs(&self) -> Vec<Arc<InputPort>> {
        self.inputs.iter().cloned().collect()
    }

    pub fn get_outputs(&self) -> Vec<Arc<OutputPort>> {
        self.outputs.iter().cloned().collect()
    }
}

impl Processor for ExchangeShuffleTransform {
    fn name(&self) -> String {
        String::from("ExchangeShuffleProcessor")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if !self.try_push_outputs() {
            return Ok(Event::NeedConsume);
        }

        if self.all_outputs_finished {
            for input in &self.inputs {
                input.finish();
            }

            return Ok(Event::Finished);
        }

        if let Some(data_block) = self.try_pull_inputs()? {
            self.input_data = Some(data_block);
            return Ok(Event::Sync);
        }

        if self.all_inputs_finished && self.buffer.is_empty() {
            for output in &self.outputs {
                output.finish();
            }

            return Ok(Event::Finished);
        }

        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data_block) = self.input_data.take() {
            let blocks = self.shuffle_scatter.execute(&data_block)?;

            for (index, block) in blocks.into_iter().enumerate() {
                if !block.is_empty() {
                    self.buffer.push_back(index, block);
                }
            }
        }

        Ok(())
    }
}

impl ExchangeShuffleTransform {
    fn try_push_outputs(&mut self) -> bool {
        self.all_outputs_finished = true;
        let mut pushed_all_outputs = true;

        for (index, output) in self.outputs.iter().enumerate() {
            if output.is_finished() {
                continue;
            }

            self.all_outputs_finished = false;
            if !output.can_push() && self.buffer.is_fill(index) {
                pushed_all_outputs = false;
                continue;
            }

            if let Some(data_block) = self.buffer.pop(index) {
                output.push_data(Ok(data_block));
            }
        }

        pushed_all_outputs
    }

    fn try_pull_inputs(&mut self) -> Result<Option<DataBlock>> {
        let mut data_block = None;
        self.all_inputs_finished = true;

        for input_port in &self.inputs {
            if input_port.is_finished() {
                continue;
            }

            input_port.set_need_data();
            self.all_inputs_finished = false;
            if !input_port.has_data() || data_block.is_some() {
                continue;
            }

            data_block = input_port.pull_data().transpose()?;
        }

        Ok(data_block)
    }
}

pub fn exchange_shuffle(params: &ShuffleExchangeParams, pipeline: &mut Pipeline) -> Result<()> {
    let output_len = pipeline.output_len();
    let new_output_len = params.destination_ids.len();
    let shuffle_scatter = params.shuffle_scatter.clone();
    let transform = ExchangeShuffleTransform::create(output_len, new_output_len, shuffle_scatter);

    let inputs = transform.get_inputs();
    let outputs = transform.get_outputs();
    pipeline.add_pipe(Pipe::create(output_len, new_output_len, vec![
        PipeItem::create(ProcessorPtr::create(Box::new(transform)), inputs, outputs),
    ]));

    Ok(())
}
