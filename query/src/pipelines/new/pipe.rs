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

    pub fn add_source(&mut self, source: ProcessorPtr, output_port: Arc<OutputPort>) {
        self.processors.push(source);
        self.outputs_port.push(output_port);
    }
}
