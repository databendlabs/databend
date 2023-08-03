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
use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoDowncast;
use common_expression::BlockMetaInfoPtr;
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
use crate::api::rpc::exchange::exchange_sorting::ExchangeSorting;
use crate::api::rpc::exchange::exchange_sorting::TransformExchangeSorting;
use crate::api::rpc::exchange::exchange_transform_scatter::ScatterTransform;
use crate::api::rpc::exchange::serde::exchange_serializer::ExchangeSerializeMeta;

pub struct ExchangeShuffleMeta {
    pub blocks: Vec<DataBlock>,
}

impl ExchangeShuffleMeta {
    pub fn create(blocks: Vec<DataBlock>) -> BlockMetaInfoPtr {
        Box::new(ExchangeShuffleMeta { blocks })
    }
}

impl Debug for ExchangeShuffleMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExchangeShuffleMeta").finish()
    }
}

impl serde::Serialize for ExchangeShuffleMeta {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        unimplemented!("Unimplemented serialize ExchangeShuffleMeta")
    }
}

impl<'de> serde::Deserialize<'de> for ExchangeShuffleMeta {
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        unimplemented!("Unimplemented deserialize ExchangeShuffleMeta")
    }
}

#[typetag::serde(name = "exchange_shuffle")]
impl BlockMetaInfo for ExchangeShuffleMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals ExchangeShuffleMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone ExchangeShuffleMeta")
    }
}

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
    all_inputs_finished: bool,
    all_outputs_finished: bool,
}

impl ExchangeShuffleTransform {
    pub fn create(inputs: usize, outputs: usize) -> ExchangeShuffleTransform {
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
            all_inputs_finished: false,
            all_outputs_finished: false,
        }
    }

    pub fn get_inputs(&self) -> Vec<Arc<InputPort>> {
        self.inputs.to_vec()
    }

    pub fn get_outputs(&self) -> Vec<Arc<OutputPort>> {
        self.outputs.to_vec()
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
        loop {
            if !self.try_push_outputs() {
                for input in &self.inputs {
                    input.set_need_data();
                }

                return Ok(Event::NeedConsume);
            }

            if self.all_outputs_finished {
                for input in &self.inputs {
                    input.finish();
                }

                return Ok(Event::Finished);
            }

            if let Some(mut data_block) = self.try_pull_inputs()? {
                if let Some(block_meta) = data_block.take_meta() {
                    if let Some(shuffle_meta) = ExchangeShuffleMeta::downcast_from(block_meta) {
                        for (index, block) in shuffle_meta.blocks.into_iter().enumerate() {
                            if !block.is_empty() || block.get_meta().is_some() {
                                self.buffer.push_back(index, block);
                            }
                        }

                        // Try push again.
                        continue;
                    }
                }

                return Err(ErrorCode::Internal(
                    "ExchangeShuffleTransform only recv ExchangeShuffleMeta.",
                ));
            }

            if self.all_inputs_finished && self.buffer.is_empty() {
                for output in &self.outputs {
                    output.finish();
                }

                return Ok(Event::Finished);
            }

            return Ok(Event::NeedData);
        }
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

            if output.can_push() {
                if let Some(data_block) = self.buffer.pop(index) {
                    output.push_data(Ok(data_block));
                }

                continue;
            }

            if !output.can_push() && self.buffer.is_fill(index) {
                pushed_all_outputs = false;
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

// Scatter the data block and push it to the corresponding output port
pub fn exchange_shuffle(params: &ShuffleExchangeParams, pipeline: &mut Pipeline) -> Result<()> {
    // append scatter transform
    pipeline.add_transform(|input, output| {
        Ok(ScatterTransform::create(
            input,
            output,
            params.shuffle_scatter.clone(),
        ))
    })?;

    let exchange_injector = &params.exchange_injector;
    exchange_injector.apply_shuffle_serializer(params, pipeline)?;

    if let Some(exchange_sorting) = &exchange_injector.exchange_sorting() {
        let output_len = pipeline.output_len();
        let sorting = ShuffleExchangeSorting::create(exchange_sorting.clone());
        let transform = TransformExchangeSorting::create(output_len, sorting);

        let output = transform.get_output();
        let inputs = transform.get_inputs();
        pipeline.add_pipe(Pipe::create(output_len, 1, vec![PipeItem::create(
            ProcessorPtr::create(Box::new(transform)),
            inputs,
            vec![output],
        )]));
    }

    let output_len = pipeline.output_len();
    let new_output_len = params.destination_ids.len();
    let transform = ExchangeShuffleTransform::create(output_len, new_output_len);

    let inputs = transform.get_inputs();
    let outputs = transform.get_outputs();
    pipeline.add_pipe(Pipe::create(output_len, new_output_len, vec![
        PipeItem::create(ProcessorPtr::create(Box::new(transform)), inputs, outputs),
    ]));

    Ok(())
}

struct ShuffleExchangeSorting {
    inner: Arc<dyn ExchangeSorting>,
}

impl ShuffleExchangeSorting {
    pub fn create(inner: Arc<dyn ExchangeSorting>) -> Arc<dyn ExchangeSorting> {
        Arc::new(ShuffleExchangeSorting { inner })
    }
}

impl ExchangeSorting for ShuffleExchangeSorting {
    fn block_number(&self, data_block: &DataBlock) -> Result<isize> {
        let block_meta = data_block.get_meta();
        let shuffle_meta = block_meta
            .and_then(ExchangeShuffleMeta::downcast_ref_from)
            .unwrap();

        for block in &shuffle_meta.blocks {
            if let Some(block_meta) = block.get_meta() {
                if let Some(block_meta) = ExchangeSerializeMeta::downcast_ref_from(block_meta) {
                    return Ok(block_meta.block_number);
                }
            }

            if !block.is_empty() || block.get_meta().is_some() {
                return self.inner.block_number(block);
            }
        }

        Err(ErrorCode::Internal(
            "Internal, ShuffleExchangeSorting only recv ExchangeSerializeMeta.",
        ))
    }
}
