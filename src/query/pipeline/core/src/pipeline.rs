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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::base::tokio::sync::Barrier;
use databend_common_base::runtime::defer;
use databend_common_base::runtime::drop_guard;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use log::info;
use petgraph::matrix_graph::Zero;

use crate::finished_chain::Callback;
use crate::finished_chain::ExecutionInfo;
use crate::finished_chain::FinishedCallbackChain;
use crate::pipe::Pipe;
use crate::pipe::PipeItem;
use crate::processors::DuplicateProcessor;
use crate::processors::Exchange;
use crate::processors::InputPort;
use crate::processors::MergePartitionProcessor;
use crate::processors::OnePartitionProcessor;
use crate::processors::OutputPort;
use crate::processors::PartitionProcessor;
use crate::processors::PlanScope;
use crate::processors::PlanScopeGuard;
use crate::processors::ProcessorPtr;
use crate::processors::ResizeProcessor;
use crate::processors::ShuffleProcessor;
use crate::LockGuard;
use crate::SinkPipeBuilder;
use crate::SourcePipeBuilder;
use crate::TransformPipeBuilder;

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
pub struct Pipeline {
    max_threads: usize,
    pub pipes: Vec<Pipe>,
    on_init: Option<InitCallback>,
    lock_guards: Vec<Arc<LockGuard>>,

    on_finished_chain: FinishedCallbackChain,

    plans_scope: Vec<PlanScope>,
    scope_size: Arc<AtomicUsize>,
}

impl Debug for Pipeline {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{:?}", &self.pipes)
    }
}

pub type InitCallback = Box<dyn FnOnce() -> Result<()> + Send + Sync + 'static>;

pub type DynTransformBuilder = Box<dyn Fn(Arc<InputPort>, Arc<OutputPort>) -> Result<ProcessorPtr>>;

impl Pipeline {
    pub fn create() -> Pipeline {
        Pipeline {
            max_threads: 0,
            pipes: Vec::new(),
            on_init: None,
            lock_guards: vec![],
            plans_scope: vec![],
            on_finished_chain: FinishedCallbackChain::create(),
            scope_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn with_scopes(scope: Vec<PlanScope>) -> Pipeline {
        let scope_size = Arc::new(AtomicUsize::new(scope.len()));
        Pipeline {
            scope_size,
            max_threads: 0,
            pipes: Vec::new(),
            on_init: None,
            lock_guards: vec![],
            on_finished_chain: FinishedCallbackChain::create(),
            plans_scope: scope,
        }
    }

    pub fn reset_scopes(&mut self, other: &Self) {
        self.scope_size = other.scope_size.clone();
        self.plans_scope = other.plans_scope.clone();
    }

    pub fn is_empty(&self) -> bool {
        self.pipes.is_empty()
    }

    // We need to push data to executor
    pub fn is_pushing_pipeline(&self) -> Result<bool> {
        match self.pipes.first() {
            Some(pipe) => Ok(pipe.input_length != 0),
            None => Err(ErrorCode::Internal(
                "Logical error: Attempted to call 'is_pushing_pipeline' on an empty pipeline.",
            )),
        }
    }

    // We need to pull data from executor
    pub fn is_pulling_pipeline(&self) -> Result<bool> {
        match self.pipes.last() {
            Some(pipe) => Ok(pipe.output_length != 0),
            None => Err(ErrorCode::Internal(
                "Logical error: 'is_pulling_pipeline' called on an empty pipeline.",
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

    pub fn finalize(mut self) -> Pipeline {
        for pipe in &mut self.pipes {
            if let Some(uninitialized_scope) = &mut pipe.scope {
                if uninitialized_scope.parent_id.is_none() {
                    for (index, scope) in self.plans_scope.iter().enumerate() {
                        if scope.id == uninitialized_scope.id && index != 0 {
                            if let Some(parent_scope) = self.plans_scope.get(index - 1) {
                                uninitialized_scope.parent_id = Some(parent_scope.id);
                            }
                        }
                    }
                }
            }
        }

        self
    }

    pub fn get_scopes(&self) -> Vec<PlanScope> {
        let scope_size = self.scope_size.load(Ordering::SeqCst);
        self.plans_scope[..scope_size].to_vec()
    }

    pub fn add_pipe(&mut self, mut pipe: Pipe) {
        let (scope_idx, _) = self.scope_size.load(Ordering::SeqCst).overflowing_sub(1);

        if let Some(scope) = self.plans_scope.get_mut(scope_idx) {
            // stack, new plan is always the parent node of previous node.
            // set the parent node in 'add_pipe' helps skip empty plans(no pipeline).
            for pipe in &mut self.pipes {
                if let Some(children) = &mut pipe.scope {
                    if children.parent_id.is_none() && children.id != scope.id {
                        children.parent_id = Some(scope.id);
                    }
                }
            }

            pipe.scope = Some(scope.clone());
        }

        self.pipes.push(pipe);
    }

    pub fn input_len(&self) -> usize {
        match self.pipes.first() {
            None => 0,
            Some(pipe) => pipe.input_length,
        }
    }

    pub fn output_len(&self) -> usize {
        match self.pipes.last() {
            None => 0,
            Some(pipe) => pipe.output_length,
        }
    }

    pub fn add_lock_guard(&mut self, guard: Option<Arc<LockGuard>>) {
        if let Some(guard) = guard {
            self.lock_guards.push(guard);
        }
    }

    pub fn take_lock_guards(&mut self) -> Vec<Arc<LockGuard>> {
        std::mem::take(&mut self.lock_guards)
    }

    pub fn set_max_threads(&mut self, max_threads: usize) {
        let mut max_pipe_size = 0;
        for pipe in &self.pipes {
            max_pipe_size = std::cmp::max(max_pipe_size, pipe.items.len());
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

    /// Add a pipe to the pipeline, which contains `n` processors. The processors are created by the given m `builders`, and each builder will create `n / m` processors.
    pub fn add_transforms_by_chunk(&mut self, builders: Vec<DynTransformBuilder>) -> Result<()> {
        let mut transform_builder = TransformPipeBuilder::create();
        assert_eq!(self.output_len() % builders.len(), 0);
        let chunk_size = self.output_len() / builders.len();
        for f in builders {
            for _index in 0..chunk_size {
                let input_port = InputPort::create();
                let output_port = OutputPort::create();

                let processor = f(input_port.clone(), output_port.clone())?;
                transform_builder.add_transform(input_port, output_port, processor);
            }
        }

        self.add_pipe(transform_builder.finalize());
        Ok(())
    }

    pub fn add_transform_with_specified_len<F>(
        &mut self,
        f: F,
        transform_len: usize,
    ) -> Result<TransformPipeBuilder>
    where
        F: Fn(Arc<InputPort>, Arc<OutputPort>) -> Result<ProcessorPtr>,
    {
        let mut transform_builder = TransformPipeBuilder::create();
        for _index in 0..transform_len {
            let input_port = InputPort::create();
            let output_port = OutputPort::create();

            let processor = f(input_port.clone(), output_port.clone())?;
            transform_builder.add_transform(input_port, output_port, processor);
        }
        Ok(transform_builder)
    }

    // Add source processor to pipeline.
    // numbers: how many output pipe numbers.
    pub fn add_source<F>(&mut self, f: F, numbers: usize) -> Result<()>
    where F: Fn(Arc<OutputPort>) -> Result<ProcessorPtr> {
        if numbers == 0 {
            return Err(ErrorCode::Internal(
                "Source output port numbers cannot be zero.",
            ));
        }

        let mut source_builder = SourcePipeBuilder::create();
        for _index in 0..numbers {
            let output = OutputPort::create();
            source_builder.add_source(output.clone(), f(output)?);
        }
        self.add_pipe(source_builder.finalize());
        Ok(())
    }

    // Add sink processor to pipeline.
    pub fn add_sink<F>(&mut self, f: F) -> Result<()>
    where F: Fn(Arc<InputPort>) -> Result<ProcessorPtr> {
        let mut sink_builder = SinkPipeBuilder::create();
        for _ in 0..self.output_len() {
            let input = InputPort::create();
            sink_builder.add_sink(input.clone(), f(input)?);
        }
        self.add_pipe(sink_builder.finalize());
        Ok(())
    }

    /// Add a ResizePipe to pipes
    pub fn try_resize(&mut self, new_size: usize) -> Result<()> {
        self.resize(new_size, false)
    }

    pub fn resize(&mut self, new_size: usize, force: bool) -> Result<()> {
        match self.pipes.last() {
            None => Err(ErrorCode::Internal("Cannot resize empty pipe.")),
            Some(pipe) if pipe.output_length == 0 => {
                Err(ErrorCode::Internal("Cannot resize empty pipe."))
            }
            Some(pipe) if !force && pipe.output_length == new_size => Ok(()),
            Some(pipe) => {
                let processor = ResizeProcessor::create(pipe.output_length, new_size);
                let inputs_port = processor.get_inputs();
                let outputs_port = processor.get_outputs();
                self.add_pipe(Pipe::create(inputs_port.len(), outputs_port.len(), vec![
                    PipeItem::create(
                        ProcessorPtr::create(Box::new(processor)),
                        inputs_port,
                        outputs_port,
                    ),
                ]));
                Ok(())
            }
        }
    }

    /// resize_partial will merge pipe_item into one reference to each range of ranges
    /// WARN!!!: you must make sure the order. for example:
    /// if there are 5 pipe_ports, given pipe_port0,pipe_port1,pipe_port2,pipe_port3,pipe_port4
    /// you can give ranges and last as [0,1],[2,3],[4]
    /// but you can't give [0,3],[1,4],[2]
    /// that says the number is successive.
    pub fn resize_partial_one(&mut self, ranges: Vec<Vec<usize>>) -> Result<()> {
        let widths = ranges.iter().map(|r| r.len()).collect::<Vec<_>>();
        self.resize_partial_one_with_width(widths)
    }

    pub fn resize_partial_one_with_width(&mut self, widths: Vec<usize>) -> Result<()> {
        match self.pipes.last() {
            None => Err(ErrorCode::Internal("Cannot resize empty pipe.")),
            Some(pipe) if pipe.output_length == 0 => {
                Err(ErrorCode::Internal("Cannot resize empty pipe."))
            }
            Some(_) => {
                let mut input_len = 0;
                let mut output_len = 0;
                let mut pipe_items = Vec::new();
                for width in widths {
                    if width.is_zero() {
                        return Err(ErrorCode::Internal("Cannot resize empty pipe."));
                    }
                    output_len += 1;
                    input_len += width;

                    let processor = ResizeProcessor::create(width, 1);
                    let inputs_port = processor.get_inputs().to_vec();
                    let outputs_port = processor.get_outputs().to_vec();
                    pipe_items.push(PipeItem::create(
                        ProcessorPtr::create(Box::new(processor)),
                        inputs_port,
                        outputs_port,
                    ));
                }
                self.add_pipe(Pipe::create(input_len, output_len, pipe_items));
                Ok(())
            }
        }
    }

    /// Duplicate a pipe input to `n` outputs.
    ///
    /// If `force_finish_together` enabled, once one output is finished, the other output will be finished too.
    pub fn duplicate(&mut self, force_finish_together: bool, n: usize) -> Result<()> {
        match self.pipes.last() {
            Some(pipe) if pipe.output_length > 0 => {
                let mut items = Vec::with_capacity(pipe.output_length);
                for _ in 0..pipe.output_length {
                    let input = InputPort::create();
                    let outputs = (0..n).map(|_| OutputPort::create()).collect::<Vec<_>>();
                    let processor = DuplicateProcessor::create(
                        input.clone(),
                        outputs.clone(),
                        force_finish_together,
                    );
                    items.push(PipeItem::create(
                        ProcessorPtr::create(Box::new(processor)),
                        vec![input],
                        outputs,
                    ));
                }
                self.add_pipe(Pipe::create(
                    pipe.output_length,
                    pipe.output_length * n,
                    items,
                ));
                Ok(())
            }
            _ => Err(ErrorCode::Internal("Cannot duplicate empty pipe.")),
        }
    }

    /// Used to re-order the input data according to the rule.
    ///
    /// `rule` is a vector of [usize], each element is the index of the output port.
    ///
    /// For example, if the rule is `[1, 2, 0]`, the data flow will be:
    ///
    /// - input 0 -> output 1
    /// - input 1 -> output 2
    /// - input 2 -> output 0
    pub fn reorder_inputs(&mut self, rule: Vec<usize>) {
        match self.pipes.last() {
            Some(pipe) if pipe.output_length > 1 => {
                debug_assert!(rule.len() == pipe.output_length);
                let mut inputs = Vec::with_capacity(pipe.output_length);
                let mut outputs = Vec::with_capacity(pipe.output_length);
                for _ in 0..pipe.output_length {
                    inputs.push(InputPort::create());
                    outputs.push(OutputPort::create());
                }
                let processor = ShuffleProcessor::create(inputs.clone(), outputs.clone(), rule);
                self.add_pipe(Pipe::create(inputs.len(), outputs.len(), vec![
                    PipeItem::create(ProcessorPtr::create(Box::new(processor)), inputs, outputs),
                ]));
            }
            _ => {}
        }
    }

    pub fn exchange<T: Exchange>(&mut self, n: usize, exchange: Arc<T>) {
        debug_assert_ne!(n, 0);

        if let Some(pipe) = self.pipes.last() {
            if pipe.output_length < 1 {
                return;
            }

            let input_len = pipe.output_length;
            let barrier = Arc::new(Barrier::new(input_len));
            let mut items = Vec::with_capacity(input_len);

            for index in 0..input_len {
                let input = InputPort::create();
                let outputs = (0..n).map(|_| OutputPort::create()).collect::<Vec<_>>();
                let partition_processor = match n {
                    1 => OnePartitionProcessor::create(
                        input.clone(),
                        outputs[0].clone(),
                        exchange.clone(),
                        index,
                        barrier.clone(),
                    ),
                    _ => PartitionProcessor::create(
                        input.clone(),
                        outputs.clone(),
                        exchange.clone(),
                        index,
                        barrier.clone(),
                    ),
                };

                items.push(PipeItem::create(partition_processor, vec![input], outputs));
            }

            // partition data block
            self.add_pipe(Pipe::create(input_len, input_len * n, items));

            let mut reorder_edges = Vec::with_capacity(input_len * n);
            for index in 0..input_len * n {
                reorder_edges.push((index % n) * input_len + (index / n));
            }

            self.reorder_inputs(reorder_edges);

            let mut items = Vec::with_capacity(input_len);
            for _index in 0..n {
                let output = OutputPort::create();
                let inputs: Vec<_> = (0..input_len).map(|_| InputPort::create()).collect();
                items.push(PipeItem::create(
                    MergePartitionProcessor::<T>::create(
                        inputs.clone(),
                        output.clone(),
                        exchange.clone(),
                    ),
                    inputs,
                    vec![output],
                ));
            }

            // merge partition
            self.add_pipe(Pipe::create(input_len * n, n, items))
        }
    }

    #[track_caller]
    pub fn set_on_init<F: FnOnce() -> Result<()> + Send + Sync + 'static>(&mut self, f: F) {
        let location = std::panic::Location::caller();
        if let Some(old_on_init) = self.on_init.take() {
            self.on_init = Some(Box::new(move || {
                old_on_init()?;
                let instants = Instant::now();

                let _guard = defer(move || {
                    info!(
                        "OnFinished callback elapsed: {:?} while in {}:{}:{}",
                        instants.elapsed(),
                        location.file(),
                        location.line(),
                        location.column()
                    );
                });

                f()
            }));

            return;
        }

        self.on_init = Some(Box::new(f));
    }

    // Set param into last
    #[track_caller]
    pub fn set_on_finished<F: Callback>(&mut self, f: F) {
        let location = std::panic::Location::caller();
        self.on_finished_chain.push_back(location, Box::new(f));
    }

    // Lift current and set param into first
    #[track_caller]
    pub fn lift_on_finished<F: Callback>(&mut self, f: F) {
        let location = std::panic::Location::caller();
        self.on_finished_chain.push_front(location, Box::new(f));
    }

    pub fn take_on_init(&mut self) -> InitCallback {
        match self.on_init.take() {
            None => Box::new(|| Ok(())),
            Some(on_init) => on_init,
        }
    }

    pub fn take_on_finished(&mut self) -> FinishedCallbackChain {
        let mut chain = FinishedCallbackChain::create();
        std::mem::swap(&mut self.on_finished_chain, &mut chain);
        chain
    }

    pub fn add_plan_scope(&mut self, scope: PlanScope) -> PlanScopeGuard {
        let scope_idx = self.scope_size.fetch_add(1, Ordering::SeqCst);

        if self.plans_scope.len() > scope_idx {
            self.plans_scope[scope_idx] = scope;
            self.plans_scope.truncate(scope_idx + 1);
            return PlanScopeGuard::create(self.scope_size.clone(), scope_idx);
        }

        assert_eq!(self.plans_scope.len(), scope_idx);
        self.plans_scope.push(scope);
        PlanScopeGuard::create(self.scope_size.clone(), scope_idx)
    }
}

impl Drop for Pipeline {
    fn drop(&mut self) {
        drop_guard(move || {
            // An error may have occurred before the executor was created.
            let cause = Err(ErrorCode::Internal(
                "Pipeline illegal state: not successfully shutdown.",
            ));

            let _ = self
                .on_finished_chain
                .apply(ExecutionInfo::create(cause, HashMap::new()));
        })
    }
}
