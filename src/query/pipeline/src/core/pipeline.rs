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
use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::runtime::defer;
use databend_common_base::runtime::drop_guard;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use log::info;
use petgraph::Direction;
use petgraph::graph::EdgeIndex;
use petgraph::matrix_graph::Zero;
use petgraph::prelude::StableGraph;
use petgraph::stable_graph::NodeIndex;

use crate::basic::duplicate_processor::DuplicateProcessor;
use crate::basic::resize_processor::ResizeProcessor;
use crate::basic::sequence_group::SequenceGroupProcessor;
use crate::basic::shuffle_processor::Exchange;
use crate::basic::shuffle_processor::MergePartitionProcessor;
use crate::basic::shuffle_processor::PartitionProcessor;
use crate::core::Callback;
use crate::core::ExecutionInfo;
use crate::core::FinishedCallbackChain;
use crate::core::InputPort;
use crate::core::LockGuard;
use crate::core::OutputPort;
use crate::core::Pipe;
use crate::core::PipeItem;
use crate::core::SinkPipeBuilder;
use crate::core::SourcePipeBuilder;
use crate::core::TransformPipeBuilder;
use crate::core::processor::ProcessorPtr;
use crate::core::profile::PlanScope;

#[derive(Clone)]
pub struct Node {
    pub proc: ProcessorPtr,
    pub inputs: Vec<Arc<InputPort>>,
    pub outputs: Vec<Arc<OutputPort>>,
    pub scope: Option<Arc<PlanScope>>,
}

impl Debug for Node {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", unsafe { self.proc.name() })
    }
}

#[derive(Clone)]
pub struct Edge {
    pub input_index: usize,
    pub output_index: usize,
    pub single_input_and_single_output: bool,
}

impl Debug for Edge {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if !self.single_input_and_single_output {
            write!(f, "from: {}, to: {}", self.output_index, self.input_index)?;
        }

        Ok(())
    }
}

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
    sinks: VecDeque<(NodeIndex, usize)>,
    pub graph: StableGraph<Node, Edge>,
    on_init: Option<InitCallback>,
    lock_guards: Vec<Arc<LockGuard>>,

    on_finished_chain: FinishedCallbackChain,
}

pub type InitCallback = Box<dyn FnOnce() -> Result<()> + Send + Sync + 'static>;

pub type DynTransformBuilder = Box<dyn Fn(Arc<InputPort>, Arc<OutputPort>) -> Result<ProcessorPtr>>;

impl Pipeline {
    pub fn create() -> Pipeline {
        Pipeline {
            max_threads: 0,
            sinks: VecDeque::new(),
            graph: Default::default(),
            on_init: None,
            lock_guards: vec![],
            on_finished_chain: FinishedCallbackChain::create(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.graph.node_count() == 0
    }

    // We need to pull data from executor
    pub fn is_pulling_pipeline(&self) -> Result<bool> {
        Ok(!self.sinks.is_empty())
    }

    // We just need to execute it.
    pub fn is_complete_pipeline(&self) -> Result<bool> {
        Ok(!self.is_empty() && !self.is_pulling_pipeline()?)
    }

    pub fn finalize(mut self, root_scope: Option<Arc<PlanScope>>) -> Pipeline {
        let Some(root_scope) = root_scope else {
            return self;
        };

        for node in self.graph.node_weights_mut() {
            let Some(scope) = node.scope.as_mut() else {
                node.scope = Some(root_scope.clone());
                continue;
            };

            if scope.parent_id.is_none() && scope.id != root_scope.id {
                unsafe {
                    let scope = Arc::get_mut_unchecked(scope);
                    scope.parent_id = Some(root_scope.id);
                }
            }
        }
        self
    }

    // groups: (group_count, ignore_output)
    // outputs: new_output_size
    pub fn sequence_group(&mut self, groups: Vec<(usize, bool)>, outputs: usize) -> Result<()> {
        if self.sinks.is_empty() {
            return Err(ErrorCode::Internal("Cannot resize empty pipe."));
        }

        let input_size = groups.iter().map(|x| x.0).sum::<usize>();

        if self.sinks.len() != input_size {
            return Err(ErrorCode::Internal(
                "Sequence group sum must be eq sink length",
            ));
        }

        if groups.is_empty() {
            return Err(ErrorCode::Internal("Cannot sequence empty groups"));
        }

        if groups.len() == 1 && !groups[0].1 {
            return self.resize(outputs, false);
        }

        let processor = SequenceGroupProcessor::create(groups, outputs)?;
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

    pub fn add_pipe(&mut self, pipe: Pipe) {
        let plan_scope = PlanScope::get_plan_scope();
        let mut new_sinks = VecDeque::with_capacity(pipe.output_length);
        for item in &pipe.items {
            let index = self.graph.add_node(Node {
                proc: item.processor.clone(),
                inputs: item.inputs_port.clone(),
                outputs: item.outputs_port.clone(),
                scope: plan_scope.clone(),
            });

            for (input_port_index, _) in item.inputs_port.iter().enumerate() {
                let Some((out_index, output_port_index)) = self.sinks.pop_front() else {
                    unreachable!();
                };

                let single_input = item.inputs_port.len() == 1;
                let single_output = self.graph[out_index].outputs.len() == 1;

                self.graph.add_edge(out_index, index, Edge {
                    input_index: input_port_index,
                    output_index: output_port_index,
                    single_input_and_single_output: single_input && single_output,
                });
            }

            for idx in 0..item.outputs_port.len() {
                new_sinks.push_back((index, idx));
            }
        }
        self.max_threads = self.max_threads.max(new_sinks.len());
        self.sinks = new_sinks;
    }

    pub fn output_len(&self) -> usize {
        self.sinks.len()
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
        let sinks = self.graph.externals(Direction::Outgoing).count();
        let sources = self.graph.externals(Direction::Incoming).count();

        self.max_threads = std::cmp::max(self.max_threads, std::cmp::max(sinks, sources));
        self.max_threads = std::cmp::min(self.max_threads, max_threads);
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
        if self.sinks.is_empty() {
            return Err(ErrorCode::Internal("Cannot resize empty pipe."));
        }

        if !force && self.sinks.len() == new_size {
            return Ok(());
        }

        let processor = ResizeProcessor::create(self.sinks.len(), new_size);
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
        if self.sinks.is_empty() {
            return Err(ErrorCode::Internal("Cannot resize empty pipe."));
        }

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

    /// Duplicate a pipe input to `n` outputs.
    ///
    /// If `force_finish_together` enabled, once one output is finished, the other output will be finished too.
    pub fn duplicate(&mut self, force_finish_together: bool, n: usize) -> Result<()> {
        if self.sinks.is_empty() {
            return Err(ErrorCode::Internal("Cannot duplicate empty pipe."));
        }

        let mut items = Vec::with_capacity(self.sinks.len());
        for _ in 0..self.sinks.len() {
            let input = InputPort::create();
            let outputs = (0..n).map(|_| OutputPort::create()).collect::<Vec<_>>();
            let processor =
                DuplicateProcessor::create(input.clone(), outputs.clone(), force_finish_together);
            items.push(PipeItem::create(
                ProcessorPtr::create(Box::new(processor)),
                vec![input],
                outputs,
            ));
        }
        self.add_pipe(Pipe::create(self.sinks.len(), self.sinks.len() * n, items));
        Ok(())
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
        let idx_mapping = rule
            .iter()
            .enumerate()
            .map(|(before_idx, after_idx)| (*after_idx, before_idx))
            .collect::<HashMap<_, _>>();

        let mut new_sinks = VecDeque::with_capacity(self.sinks.len());

        for index in 0..self.sinks.len() {
            new_sinks.push_back(self.sinks[idx_mapping[&index]]);
        }

        self.sinks = new_sinks;
    }

    pub fn exchange<T: Exchange>(&mut self, n: usize, exchange: Arc<T>) -> Result<()> {
        self.exchange_with_merge(n, exchange.clone(), |inputs, output| {
            Ok(MergePartitionProcessor::create(
                inputs,
                output,
                exchange.clone(),
            ))
        })
    }

    pub fn exchange_with_merge<T, F>(&mut self, n: usize, exchange: Arc<T>, f: F) -> Result<()>
    where
        T: Exchange,
        F: Fn(Vec<Arc<InputPort>>, Arc<OutputPort>) -> Result<ProcessorPtr>,
    {
        if self.sinks.is_empty() {
            return Ok(());
        }

        let input_len = self.sinks.len();
        let mut items = Vec::with_capacity(input_len);

        for _ in 0..input_len {
            let input = InputPort::create();
            let outputs: Vec<_> = (0..n).map(|_| OutputPort::create()).collect();
            items.push(PipeItem::create(
                PartitionProcessor::create(input.clone(), outputs.clone(), exchange.clone()),
                vec![input],
                outputs,
            ));
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
                f(inputs.clone(), output.clone())?,
                inputs,
                vec![output],
            ));
        }

        // merge partition
        self.add_pipe(Pipe::create(input_len * n, n, items));
        Ok(())
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

    pub fn take_sinks(&mut self) -> VecDeque<(NodeIndex, usize)> {
        std::mem::take(&mut self.sinks)
    }

    pub fn extend_sinks(&mut self, sinks: impl IntoIterator<Item = (NodeIndex, usize)>) {
        self.sinks.extend(sinks)
    }

    pub fn merge(&mut self, mut other: Self) -> Result<VecDeque<(NodeIndex, usize)>> {
        self.max_threads = std::cmp::max(self.max_threads, other.max_threads);
        let offset = self.graph.node_count();

        let mut other_sinks = VecDeque::with_capacity(other.output_len());
        for (index, v) in other.take_sinks() {
            other_sinks.push_back((NodeIndex::new(offset + index.index()), v));
        }

        for node in other.graph.node_weights() {
            self.graph.add_node(node.clone());
        }

        for edge in other.graph.edge_indices() {
            let index = EdgeIndex::new(edge.index());
            if let Some((source, target)) = other.graph.edge_endpoints(index) {
                let source = NodeIndex::new(offset + source.index());
                let target = NodeIndex::new(offset + target.index());
                let edge_weight = other
                    .graph
                    .edge_weight(index)
                    .expect("Edge weight must exist for valid edge index");
                self.graph.add_edge(source, target, edge_weight.clone());
            }
        }

        self.lock_guards.extend(other.take_lock_guards());
        self.on_finished_chain.extend(other.take_on_finished());

        let self_on_init = self.take_on_init();
        let other_on_init = other.take_on_init();
        self.on_init = Some(Box::new(move || {
            self_on_init().and_then(move |_| other_on_init())
        }));

        Ok(other_sinks)
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
