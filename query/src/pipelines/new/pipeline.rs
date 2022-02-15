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

use common_exception::ErrorCode;
use common_exception::Result;

use crate::pipelines::new::pipe::NewPipe;
use crate::pipelines::new::pipe::TransformPipeBuilder;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::ResizeProcessor;

pub struct NewPipeline {
    max_threads: usize,
    pub pipes: Vec<NewPipe>,
}

impl NewPipeline {
    pub fn create() -> NewPipeline {
        NewPipeline {
            max_threads: 0,
            pipes: Vec::new(),
        }
    }

    pub fn add_pipe(&mut self, pipe: NewPipe) {
        self.pipes.push(pipe);
    }

    pub fn output_len(&self) -> usize {
        match self.pipes.last() {
            None => 0,
            Some(NewPipe::SimplePipe { outputs_port, .. }) => outputs_port.len(),
            Some(NewPipe::ResizePipe { outputs_port, .. }) => outputs_port.len(),
        }
    }

    pub fn graph_size(&self) -> (usize, usize) {
        let mut nodes_size = 0;
        let mut edges_size = 0;
        for pipe in &self.pipes {
            nodes_size += match pipe {
                NewPipe::ResizePipe { .. } => 1,
                NewPipe::SimplePipe { processors, .. } => processors.len(),
            };

            edges_size += match pipe {
                NewPipe::ResizePipe { inputs_port, .. } => inputs_port.len(),
                NewPipe::SimplePipe { inputs_port, .. } => inputs_port.len(),
            };
        }

        (nodes_size, edges_size)
    }

    pub fn set_max_threads(&mut self, max_threads: usize) {
        let mut max_pipe_size = 0;
        for pipe in &self.pipes {
            max_pipe_size = std::cmp::max(max_pipe_size, pipe.size());
        }

        self.max_threads = std::cmp::min(max_pipe_size, max_threads);
    }

    pub fn get_max_threads(&self) -> usize {
        self.max_threads
    }

    pub fn add_transform<F>(&mut self, f: F) -> Result<()>
    where F: Fn(Arc<InputPort>, Arc<OutputPort>) -> Result<ProcessorPtr> {
        let mut transform_builder = TransformPipeBuilder::create();
        for _index in 0..self.output_len() {
            let input_port = InputPort::create();
            let output_port = OutputPort::create();

            let processor = f(input_port.clone(), output_port.clone())?;
            transform_builder.add_transform(input_port, output_port, processor);
        }

        self.add_pipe(transform_builder.finalize());
        Ok(())
    }

    pub fn resize(&mut self, new_size: usize) -> Result<()> {
        match self.pipes.last() {
            None => Err(ErrorCode::LogicalError("")),
            Some(pipe) if pipe.output_size() == 0 => Err(ErrorCode::LogicalError("")),
            Some(pipe) if pipe.output_size() == new_size => Ok(()),
            Some(pipe) => {
                let processor = ResizeProcessor::create(pipe.output_size(), new_size);
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
