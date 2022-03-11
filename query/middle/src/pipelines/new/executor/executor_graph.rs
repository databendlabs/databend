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

use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_exception::Result;
use common_infallible::RwLock;
use common_infallible::RwLockUpgradableReadGuard;
use common_tracing::tracing;
use petgraph::dot::Config;
use petgraph::dot::Dot;
use petgraph::prelude::EdgeIndex;
use petgraph::prelude::NodeIndex;
use petgraph::prelude::StableGraph;
use petgraph::Direction;

use crate::pipelines::new::executor::executor_tasks::ExecutorTasksQueue;
use crate::pipelines::new::executor::executor_worker_context::ExecutorTask;
use crate::pipelines::new::executor::executor_worker_context::ExecutorWorkerContext;
use crate::pipelines::new::pipe::NewPipe;
use crate::pipelines::new::pipeline::NewPipeline;
use crate::pipelines::new::processors::connect;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::Event;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::DirectedEdge;
use crate::pipelines::new::processors::UpdateList;
use crate::pipelines::new::processors::UpdateTrigger;

enum State {
    Idle,
    // Preparing,
    Processing,
    Finished,
}

struct Node {
    state: std::sync::Mutex<State>,
    processor: ProcessorPtr,

    updated_list: Arc<UpdateList>,
    #[allow(dead_code)]
    inputs_port: Vec<Arc<InputPort>>,
    #[allow(dead_code)]
    outputs_port: Vec<Arc<OutputPort>>,
}

impl Node {
    pub fn create(
        processor: &ProcessorPtr,
        inputs_port: &[Arc<InputPort>],
        outputs_port: &[Arc<OutputPort>],
    ) -> Arc<Node> {
        Arc::new(Node {
            state: std::sync::Mutex::new(State::Idle),
            processor: processor.clone(),
            updated_list: UpdateList::create(),
            inputs_port: inputs_port.to_vec(),
            outputs_port: outputs_port.to_vec(),
        })
    }

    pub unsafe fn trigger(&self, queue: &mut VecDeque<DirectedEdge>) {
        self.updated_list.trigger(queue)
    }

    pub unsafe fn create_trigger(&self, index: EdgeIndex) -> *mut UpdateTrigger {
        self.updated_list.create_trigger(index)
    }
}

struct ExecutingGraph {
    graph: StableGraph<Arc<Node>, ()>,
}

type StateLockGuard<'a> = RwLockUpgradableReadGuard<'a, ExecutingGraph>;

impl ExecutingGraph {
    pub fn create(pipeline: NewPipeline) -> Result<ExecutingGraph> {
        // let (nodes_size, edges_size) = pipeline.graph_size();
        let mut graph = StableGraph::new();

        let mut node_stack = Vec::new();
        let mut edge_stack: Vec<Arc<OutputPort>> = Vec::new();
        for query_pipe in &pipeline.pipes {
            match query_pipe {
                NewPipe::ResizePipe {
                    processor,
                    inputs_port,
                    outputs_port,
                } => unsafe {
                    assert_eq!(node_stack.len(), inputs_port.len());

                    let resize_node = Node::create(processor, inputs_port, outputs_port);
                    let target_index = graph.add_node(resize_node.clone());
                    processor.set_id(target_index);

                    for index in 0..node_stack.len() {
                        let source_index = node_stack[index];
                        let edge_index = graph.add_edge(source_index, target_index, ());

                        let input_trigger = resize_node.create_trigger(edge_index);
                        inputs_port[index].set_trigger(input_trigger);
                        edge_stack[index]
                            .set_trigger(graph[source_index].create_trigger(edge_index));
                        connect(&inputs_port[index], &edge_stack[index]);
                    }

                    node_stack.clear();
                    edge_stack.clear();
                    for output_port in outputs_port {
                        node_stack.push(target_index);
                        edge_stack.push(output_port.clone());
                    }
                },
                NewPipe::SimplePipe {
                    processors,
                    inputs_port,
                    outputs_port,
                } => unsafe {
                    assert_eq!(node_stack.len(), inputs_port.len());
                    assert!(inputs_port.is_empty() || inputs_port.len() == processors.len());
                    assert!(outputs_port.is_empty() || outputs_port.len() == processors.len());

                    let mut new_node_stack = Vec::with_capacity(outputs_port.len());
                    let mut new_edge_stack = Vec::with_capacity(outputs_port.len());

                    for index in 0..processors.len() {
                        let mut p_inputs_port = Vec::with_capacity(1);
                        let mut p_outputs_port = Vec::with_capacity(1);

                        if !inputs_port.is_empty() {
                            p_inputs_port.push(inputs_port[index].clone());
                        }

                        if !outputs_port.is_empty() {
                            p_outputs_port.push(outputs_port[index].clone());
                        }

                        let target_node =
                            Node::create(&processors[index], &p_inputs_port, &p_outputs_port);
                        let target_index = graph.add_node(target_node.clone());
                        processors[index].set_id(target_index);

                        if !node_stack.is_empty() {
                            let source_index = node_stack[index];
                            let edge_index = graph.add_edge(source_index, target_index, ());

                            inputs_port[index].set_trigger(target_node.create_trigger(edge_index));
                            edge_stack[index]
                                .set_trigger(graph[source_index].create_trigger(edge_index));
                            connect(&inputs_port[index], &edge_stack[index]);
                        }

                        if !outputs_port.is_empty() {
                            new_node_stack.push(target_index);
                            new_edge_stack.push(outputs_port[index].clone());
                        }
                    }

                    node_stack = new_node_stack;
                    edge_stack = new_edge_stack;
                },
            };
        }

        // Assert no output.
        assert_eq!(node_stack.len(), 0);
        Ok(ExecutingGraph { graph })
    }

    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    pub unsafe fn init_schedule_queue(locker: &StateLockGuard) -> Result<ScheduleQueue> {
        let mut schedule_queue = ScheduleQueue::create();
        for sink_index in locker.graph.externals(Direction::Outgoing) {
            ExecutingGraph::schedule_queue(locker, sink_index, &mut schedule_queue)?;
        }

        Ok(schedule_queue)
    }

    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    pub unsafe fn schedule_queue(
        locker: &StateLockGuard,
        index: NodeIndex,
        schedule_queue: &mut ScheduleQueue,
    ) -> Result<()> {
        let mut need_schedule_nodes = VecDeque::new();
        let mut need_schedule_edges = VecDeque::new();

        need_schedule_nodes.push_back(index);
        while !need_schedule_nodes.is_empty() || !need_schedule_edges.is_empty() {
            // To avoid lock too many times, we will try to cache lock.
            let mut state_guard_cache = None;

            if need_schedule_nodes.is_empty() {
                let edge = need_schedule_edges.pop_front().unwrap();
                let target_index = DirectedEdge::get_target(&edge, &locker.graph);

                let node = &locker.graph[target_index];
                let node_state = node.state.lock().unwrap();

                if matches!(*node_state, State::Idle) {
                    state_guard_cache = Some(node_state);
                    need_schedule_nodes.push_back(target_index);
                }
            }

            if let Some(schedule_index) = need_schedule_nodes.pop_front() {
                let node = &locker.graph[schedule_index];

                if state_guard_cache.is_none() {
                    state_guard_cache = Some(node.state.lock().unwrap());
                }

                *state_guard_cache.unwrap() = match node.processor.event()? {
                    Event::Finished => State::Finished,
                    Event::NeedData | Event::NeedConsume => State::Idle,
                    Event::Sync => {
                        schedule_queue.push_sync(node.processor.clone());
                        State::Processing
                    }
                    Event::Async => {
                        schedule_queue.push_async(node.processor.clone());
                        State::Processing
                    }
                };

                node.trigger(&mut need_schedule_edges);
            }
        }

        Ok(())
    }
}

pub struct ScheduleQueue {
    sync_queue: VecDeque<ProcessorPtr>,
    async_queue: VecDeque<ProcessorPtr>,
}

impl ScheduleQueue {
    pub fn create() -> ScheduleQueue {
        ScheduleQueue {
            sync_queue: VecDeque::new(),
            async_queue: VecDeque::new(),
        }
    }

    #[inline]
    pub fn push_sync(&mut self, processor: ProcessorPtr) {
        self.sync_queue.push_back(processor);
    }

    #[inline]
    pub fn push_async(&mut self, processor: ProcessorPtr) {
        self.async_queue.push_back(processor);
    }

    pub fn pop_task(&mut self) -> Option<ExecutorTask> {
        match self.sync_queue.pop_front() {
            Some(processor) => Some(ExecutorTask::Sync(processor)),
            None => self.async_queue.pop_front().map(ExecutorTask::Async),
        }
    }

    pub fn schedule_tail(mut self, global: &ExecutorTasksQueue, ctx: &mut ExecutorWorkerContext) {
        let mut tasks = VecDeque::with_capacity(self.sync_queue.len());

        while let Some(processor) = self.sync_queue.pop_front() {
            tasks.push_back(ExecutorTask::Sync(processor));
        }

        while let Some(processor) = self.async_queue.pop_front() {
            tasks.push_back(ExecutorTask::Async(processor));
        }

        global.push_tasks(ctx, tasks)
    }

    pub fn schedule(mut self, global: &ExecutorTasksQueue, context: &mut ExecutorWorkerContext) {
        debug_assert!(!context.has_task());

        match self.sync_queue.is_empty() {
            false => self.schedule_sync(global, context),
            true if !self.async_queue.is_empty() => self.schedule_async(global, context),
            true => { /* do nothing*/ }
        }

        if !self.sync_queue.is_empty() || !self.async_queue.is_empty() {
            self.schedule_tail(global, context);
        }
    }

    fn schedule_sync(&mut self, _: &ExecutorTasksQueue, ctx: &mut ExecutorWorkerContext) {
        if let Some(processor) = self.sync_queue.pop_front() {
            ctx.set_task(ExecutorTask::Sync(processor));
        }
    }

    fn schedule_async(&mut self, _: &ExecutorTasksQueue, ctx: &mut ExecutorWorkerContext) {
        if let Some(processor) = self.async_queue.pop_front() {
            ctx.set_task(ExecutorTask::Async(processor));
        }
    }
}

pub struct RunningGraph(RwLock<ExecutingGraph>);

impl RunningGraph {
    pub fn create(pipeline: NewPipeline) -> Result<RunningGraph> {
        let graph_state = ExecutingGraph::create(pipeline)?;
        tracing::debug!("Create running graph:{:?}", graph_state);
        Ok(RunningGraph(RwLock::new(graph_state)))
    }

    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    pub unsafe fn init_schedule_queue(&self) -> Result<ScheduleQueue> {
        ExecutingGraph::init_schedule_queue(&self.0.upgradable_read())
    }

    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    pub unsafe fn schedule_queue(&self, node_index: NodeIndex) -> Result<ScheduleQueue> {
        let mut schedule_queue = ScheduleQueue::create();
        ExecutingGraph::schedule_queue(&self.0.upgradable_read(), node_index, &mut schedule_queue)?;
        Ok(schedule_queue)
    }
}

impl Debug for Node {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        unsafe { write!(f, "{}", self.processor.name()) }
    }
}

impl Debug for ExecutingGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "{:?}",
            Dot::with_config(&self.graph, &[Config::EdgeNoLabel])
        )
    }
}

impl Debug for RunningGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        let graph = self.0.read();
        write!(f, "{:?}", graph)
    }
}

impl Debug for ScheduleQueue {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        #[derive(Debug)]
        #[allow(dead_code)]
        struct QueueItem {
            id: usize,
            name: String,
        }

        unsafe {
            let mut sync_queue = Vec::with_capacity(self.sync_queue.len());
            let mut async_queue = Vec::with_capacity(self.async_queue.len());

            for item in &self.sync_queue {
                sync_queue.push(QueueItem {
                    id: item.id().index(),
                    name: item.name().to_string(),
                })
            }

            for item in &self.async_queue {
                async_queue.push(QueueItem {
                    id: item.id().index(),
                    name: item.name().to_string(),
                })
            }

            f.debug_struct("ScheduleQueue")
                .field("sync_queue", &sync_queue)
                .field("async_queue", &async_queue)
                .finish()
        }
    }
}
