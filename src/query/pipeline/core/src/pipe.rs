// Copyright 2022 Datafuse Labs.
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

use std::sync::Arc;

use crate::processors::port::InputPort;
use crate::processors::port::OutputPort;
use crate::processors::processor::ProcessorPtr;

#[derive(Clone)]
pub struct PipeItem {
    pub processor: ProcessorPtr,
    pub inputs_port: Vec<Arc<InputPort>>,
    pub outputs_port: Vec<Arc<OutputPort>>,
}

#[derive(Clone)]
pub struct Pipe {
    pub items: Vec<PipeItem>,
    pub input_length: usize,
    pub output_length: usize,
}

impl Pipe {
    pub fn create_resize(
        processor: &ProcessorPtr,
        inputs: &[Arc<InputPort>],
        outputs: &[Arc<OutputPort>],
    ) -> Pipe {
        Pipe {
            items: vec![PipeItem {
                processor: processor.clone(),
                inputs_port: inputs.to_vec(),
                outputs_port: outputs.to_vec(),
            }],
            input_length: inputs.len(),
            output_length: outputs.len(),
        }
    }

    pub fn create_simple(
        processors: &[ProcessorPtr],
        inputs: &[Arc<InputPort>],
        outputs: &[Arc<OutputPort>],
    ) -> Pipe {
        let mut items = Vec::with_capacity(processors.len());

        for (index, processor) in processors.iter().enumerate() {
            if inputs.is_empty() {
                items.push(PipeItem {
                    processor: processor.clone(),
                    inputs_port: vec![],
                    outputs_port: vec![outputs[index].clone()],
                });
            } else if outputs.is_empty() {
                items.push(PipeItem {
                    processor: processor.clone(),
                    inputs_port: vec![inputs[index].clone()],
                    outputs_port: vec![],
                });
            } else {
                items.push(PipeItem {
                    processor: processor.clone(),
                    inputs_port: vec![inputs[index].clone()],
                    outputs_port: vec![outputs[index].clone()],
                });
            }
        }

        Pipe {
            items,
            input_length: inputs.len(),
            output_length: outputs.len(),
        }
    }
}

#[derive(Clone)]
pub struct SourcePipeBuilder {
    items: Vec<PipeItem>,
}

impl SourcePipeBuilder {
    pub fn create() -> SourcePipeBuilder {
        SourcePipeBuilder { items: vec![] }
    }

    pub fn finalize(self) -> Pipe {
        let outputs_length = self.items.len();

        Pipe {
            items: self.items,
            input_length: 0,
            output_length: outputs_length,
        }
    }

    pub fn add_source(&mut self, output_port: Arc<OutputPort>, source: ProcessorPtr) {
        self.items.push(PipeItem {
            processor: source,
            inputs_port: vec![],
            outputs_port: vec![output_port],
        });
    }
}

#[allow(dead_code)]
pub struct SinkPipeBuilder {
    items: Vec<PipeItem>,
}

#[allow(dead_code)]
impl SinkPipeBuilder {
    pub fn create() -> SinkPipeBuilder {
        SinkPipeBuilder { items: vec![] }
    }

    pub fn finalize(self) -> Pipe {
        let input_length = self.items.len();
        Pipe {
            input_length,
            items: self.items,
            output_length: 0,
        }
    }

    pub fn add_sink(&mut self, inputs_port: Arc<InputPort>, sink: ProcessorPtr) {
        self.items.push(PipeItem {
            processor: sink,
            inputs_port: vec![inputs_port],
            outputs_port: vec![],
        });
    }
}

pub struct TransformPipeBuilder {
    items: Vec<PipeItem>,
    // processors: Vec<ProcessorPtr>,
    // inputs_port: Vec<Arc<InputPort>>,
    // outputs_port: Vec<Arc<OutputPort>>,
}

impl TransformPipeBuilder {
    pub fn create() -> TransformPipeBuilder {
        TransformPipeBuilder { items: vec![] }
    }

    pub fn finalize(self) -> Pipe {
        let items_length = self.items.len();
        Pipe {
            items: self.items,
            input_length: items_length,
            output_length: items_length,
        }
    }

    pub fn add_transform(
        &mut self,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        proc: ProcessorPtr,
    ) {
        self.items.push(PipeItem {
            processor: proc,
            inputs_port: vec![input],
            outputs_port: vec![output],
        });
    }
}
