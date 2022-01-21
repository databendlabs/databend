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

use common_exception::ErrorCode;
use common_exception::Result;

use crate::pipelines::new::pipe::NewPipe;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::ResizeProcessor;

pub struct NewPipeline {
    pub pipes: Vec<NewPipe>,
}

impl NewPipeline {
    pub fn create() -> NewPipeline {
        NewPipeline { pipes: Vec::new() }
    }

    pub fn add_pipe(&mut self, pipe: NewPipe) {
        self.pipes.push(pipe);
    }

    pub fn pipe_len(&self) -> usize {
        match self.pipes.last() {
            None => 0,
            Some(NewPipe::SimplePipe { processors, .. }) => processors.len(),
            Some(NewPipe::ResizePipe { outputs_port, .. }) => outputs_port.len(),
        }
    }

    pub fn resize(&mut self, new_size: usize) -> Result<()> {
        match self.pipes.last() {
            None => Err(ErrorCode::LogicalError("")),
            Some(pipe) => {
                let processor = ResizeProcessor::create(pipe.size(), new_size);
                let inputs_port = processor.get_inputs().to_vec();
                let outputs_port = processor.get_outputs().to_vec();
                self.pipes.push(NewPipe::ResizePipe {
                    inputs_port,
                    outputs_port,
                    processor: ProcessorPtr::create(Box::new(processor)),
                });
                Ok(())
            }
        }
    }
}
