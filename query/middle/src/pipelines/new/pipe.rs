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

use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;

pub enum NewPipe {
    SimplePipe {
        processors: Vec<ProcessorPtr>,
        inputs_port: Vec<Arc<InputPort>>,
        outputs_port: Vec<Arc<OutputPort>>,
    },
    ResizePipe {
        processor: ProcessorPtr,
        inputs_port: Vec<Arc<InputPort>>,
        outputs_port: Vec<Arc<OutputPort>>,
    },
}

impl NewPipe {
    pub fn size(&self) -> usize {
        match self {
            NewPipe::ResizePipe { .. } => 1,
            NewPipe::SimplePipe { processors, .. } => processors.len(),
        }
    }

    pub fn input_size(&self) -> usize {
        match self {
            NewPipe::SimplePipe { inputs_port, .. } => inputs_port.len(),
            NewPipe::ResizePipe { inputs_port, .. } => inputs_port.len(),
        }
    }

    pub fn output_size(&self) -> usize {
        match self {
            NewPipe::SimplePipe { outputs_port, .. } => outputs_port.len(),
            NewPipe::ResizePipe { outputs_port, .. } => outputs_port.len(),
        }
    }
}

pub struct SourcePipeBuilder {
    processors: Vec<ProcessorPtr>,
    outputs_port: Vec<Arc<OutputPort>>,
}

impl SourcePipeBuilder {
    pub fn create() -> SourcePipeBuilder {
        SourcePipeBuilder {
            processors: vec![],
            outputs_port: vec![],
        }
    }

    pub fn finalize(self) -> NewPipe {
        assert_eq!(self.processors.len(), self.outputs_port.len());
        NewPipe::SimplePipe {
            processors: self.processors,
            inputs_port: vec![],
            outputs_port: self.outputs_port,
        }
    }

    pub fn add_source(&mut self, output_port: Arc<OutputPort>, source: ProcessorPtr) {
        self.processors.push(source);
        self.outputs_port.push(output_port);
    }
}

pub struct SinkPipeBuilder {
    processors: Vec<ProcessorPtr>,
    inputs_port: Vec<Arc<InputPort>>,
}

impl SinkPipeBuilder {
    pub fn create() -> SinkPipeBuilder {
        SinkPipeBuilder {
            processors: vec![],
            inputs_port: vec![],
        }
    }

    pub fn finalize(self) -> NewPipe {
        assert_eq!(self.processors.len(), self.inputs_port.len());
        NewPipe::SimplePipe {
            processors: self.processors,
            inputs_port: self.inputs_port,
            outputs_port: vec![],
        }
    }

    pub fn add_sink(&mut self, inputs_port: Arc<InputPort>, sink: ProcessorPtr) {
        self.processors.push(sink);
        self.inputs_port.push(inputs_port);
    }
}

pub struct TransformPipeBuilder {
    processors: Vec<ProcessorPtr>,
    inputs_port: Vec<Arc<InputPort>>,
    outputs_port: Vec<Arc<OutputPort>>,
}

impl TransformPipeBuilder {
    pub fn create() -> TransformPipeBuilder {
        TransformPipeBuilder {
            processors: vec![],
            inputs_port: vec![],
            outputs_port: vec![],
        }
    }

    pub fn finalize(self) -> NewPipe {
        assert_eq!(self.processors.len(), self.inputs_port.len());
        assert_eq!(self.processors.len(), self.outputs_port.len());
        NewPipe::SimplePipe {
            processors: self.processors,
            inputs_port: self.inputs_port,
            outputs_port: self.outputs_port,
        }
    }

    pub fn add_transform(
        &mut self,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        proc: ProcessorPtr,
    ) {
        self.processors.push(proc);
        self.inputs_port.push(input);
        self.outputs_port.push(output);
    }
}
