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

use crate::pipelines::executor::FinishedCallback;
use crate::pipelines::pipe::Pipe;
use crate::pipelines::pipe::TransformPipeBuilder;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::ResizeProcessor;

/// The struct of new pipeline
///                                                                              +----------+
///                                                                         +--->|Processors|
///                                                                         |    +----------+
///                                                          +----------+   |
///                                                      +-->|SimplePipe|---+
///                                                      |   +----------+   |                  +-----------+
///                           +-----------+              |                  |              +-->|inputs_port|
///                   +------>|max threads|              |                  |     +-----+  |   +-----------+
///                   |       +-----------+              |                  +--->>|ports|--+
/// +----------+      |                       +-----+    |                  |     +-----+  |   +------------+
/// | pipeline |------+                       |pipe1|----+                  |              +-->|outputs_port|
/// +----------+      |       +-------+       +-----+    |   +----------+   |                  +------------+
///                   +------>| pipes |------>| ... |    +-->|ResizePipe|---+
///                           +-------+       +-----+        +----------+   |
///                                           |pipeN|                       |    +---------+
///                                           +-----+                       +--->|Processor|
///                                                                              +---------+
///
#[derive(Clone)]
pub struct Pipeline {
    max_threads: usize,
    pub pipes: Vec<Pipe>,
    on_finished: Option<FinishedCallback>,

    /// The pipelines depended by this pipeline, e.g. build side of
    /// hash join is depended by probe side.
    ///
    /// TODO(leiysky): Maybe we should launch the current pipeline when all
    /// the depended pipelines are finished?
    /// For now, we just launch the source pipelines before lauching
    /// current pipeline. It is fine since we only have hash join that
    /// depends on other pipelines now, which implement a blocking mechanism
    /// to wait until the depended pipelines are finished.
    pub source_pipelines: Vec<Pipeline>,
}

impl Pipeline {
    pub fn create() -> Pipeline {
        Pipeline {
            max_threads: 0,
            pipes: Vec::new(),
            on_finished: None,
            source_pipelines: Vec::new(),
        }
    }

    // We need to push data to executor
    pub fn is_pushing_pipeline(&self) -> Result<bool> {
        match self.pipes.first() {
            Some(pipe) => Ok(pipe.input_size() != 0),
            None => Err(ErrorCode::LogicalError(
                "Logical error, call is_pushing on empty pipeline.",
            )),
        }
    }

    // We need to pull data from executor
    pub fn is_pulling_pipeline(&self) -> Result<bool> {
        match self.pipes.last() {
            Some(pipe) => Ok(pipe.output_size() != 0),
            None => Err(ErrorCode::LogicalError(
                "Logical error, call is_pulling on empty pipeline.",
            )),
        }
    }

    // We just need to execute it.
    pub fn is_complete_pipeline(&self) -> Result<bool> {
        Ok(
            !self.pipes.is_empty()
                && !self.is_pushing_pipeline()?
                && !self.is_pulling_pipeline()?,
        )
    }

    pub fn add_pipe(&mut self, pipe: Pipe) {
        self.pipes.push(pipe);
    }

    pub fn input_len(&self) -> usize {
        match self.pipes.first() {
            None => 0,
            Some(Pipe::SimplePipe { inputs_port, .. }) => inputs_port.len(),
            Some(Pipe::ResizePipe { inputs_port, .. }) => inputs_port.len(),
        }
    }

    pub fn output_len(&self) -> usize {
        match self.pipes.last() {
            None => 0,
            Some(Pipe::SimplePipe { outputs_port, .. }) => outputs_port.len(),
            Some(Pipe::ResizePipe { outputs_port, .. }) => outputs_port.len(),
        }
    }

    pub fn set_max_threads(&mut self, max_threads: usize) {
        let mut max_pipe_size = 0;
        for pipe in &self.pipes {
            max_pipe_size = std::cmp::max(max_pipe_size, pipe.size());
        }

        self.max_threads = std::cmp::min(max_pipe_size, max_threads);

        // TODO(leiysky): Set max_threads for each source pipelines seperately?
        for source_pipeline in self.source_pipelines.iter_mut() {
            source_pipeline.set_max_threads(max_threads);
        }
    }

    pub fn get_max_threads(&self) -> usize {
        self.max_threads
    }

    pub fn add_source_pipeline(&mut self, pipeline: Pipeline) {
        self.source_pipelines.push(pipeline);
    }

    /// Get flattened pipeline with `source_pipelines` removed.
    /// The pipelines will be added into result vector in post-order,
    /// i.e. the pipelines in `source_pipelines` will be added first and
    /// the last element is the root pipeline(current pipeline).
    ///
    /// This is helpful when we want to launch the pipelines with depended
    /// pipelines launched first.
    pub fn flatten(&self) -> Vec<Pipeline> {
        let mut pipelines = Vec::new();
        self.flatten_into(&mut pipelines);

        assert!(!pipelines.is_empty());

        pipelines
    }

    fn flatten_into(&self, pipelines: &mut Vec<Pipeline>) {
        for source_pipeline in self.source_pipelines.iter() {
            source_pipeline.flatten_into(pipelines);
        }
        let pipeline = Pipeline {
            // clear the source_pipelines for flattened pipeline
            source_pipelines: vec![],
            max_threads: self.max_threads,
            pipes: self.pipes.clone(),
            on_finished: self.on_finished.clone(),
        };
        pipelines.push(pipeline);
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

    /// Add a ResizePipe to pipes
    pub fn resize(&mut self, new_size: usize) -> Result<()> {
        match self.pipes.last() {
            None => Err(ErrorCode::LogicalError("Cannot resize empty pipe.")),
            Some(pipe) if pipe.output_size() == 0 => {
                Err(ErrorCode::LogicalError("Cannot resize empty pipe."))
            }
            Some(pipe) if pipe.output_size() == new_size => Ok(()),
            Some(pipe) => {
                let processor = ResizeProcessor::create(pipe.output_size(), new_size);
                let inputs_port = processor.get_inputs().to_vec();
                let outputs_port = processor.get_outputs().to_vec();
                self.pipes.push(Pipe::ResizePipe {
                    inputs_port,
                    outputs_port,
                    processor: ProcessorPtr::create(Box::new(processor)),
                });
                Ok(())
            }
        }
    }

    pub fn set_on_finished<F: Fn(&Option<ErrorCode>) + Send + Sync + 'static>(&mut self, f: F) {
        if let Some(on_finished) = &self.on_finished {
            let old_finished = on_finished.clone();

            self.on_finished = Some(Arc::new(move |may_error| {
                old_finished(may_error);

                f(may_error);
            }));

            return;
        }

        self.on_finished = Some(Arc::new(Box::new(f)));
    }

    pub fn take_on_finished(&mut self) -> FinishedCallback {
        match self.on_finished.take() {
            None => Arc::new(|_may_error| {}),
            Some(on_finished) => on_finished,
        }
    }
}

impl Drop for Pipeline {
    fn drop(&mut self) {
        // An error may have occurred before the executor was created.
        if let Some(on_finished) = self.on_finished.take() {
            (on_finished)(&None);
        }
    }
}
