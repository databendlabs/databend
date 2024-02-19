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

use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_base::runtime::TrackedFuture;
use databend_common_base::runtime::TrySpawn;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline_core::processors::EventCause;
use databend_common_pipeline_core::processors::Profile;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_core::PlanScope;
use log::debug;
use log::trace;
use minitrace::prelude::*;
use petgraph::dot::Config;
use petgraph::dot::Dot;
use petgraph::prelude::EdgeIndex;
use petgraph::prelude::NodeIndex;
use petgraph::prelude::StableGraph;
use petgraph::Direction;

use crate::pipelines::executor::ExecutorTask;
use crate::pipelines::executor::ExecutorTasksQueue;
use crate::pipelines::executor::ExecutorWorkerContext;
use crate::pipelines::executor::PipelineExecutor;
use crate::pipelines::executor::ProcessorAsyncTask;
use crate::pipelines::executor::WorkersCondvar;
use crate::pipelines::processors::connect;
use crate::pipelines::processors::DirectedEdge;
use crate::pipelines::processors::Event;
use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::OutputPort;
use crate::pipelines::processors::ProcessorPtr;
use crate::pipelines::processors::UpdateList;
use crate::pipelines::processors::UpdateTrigger;

enum State {
    Idle,
    Processing,
    Finished,
}

#[derive(Debug, Clone)]
struct EdgeInfo {
    input_index: usize,
    output_index: usize,
}

pub(crate) struct Node {
    state: std::sync::Mutex<State>,
    pub(crate) processor: ProcessorPtr,

    pub(crate) profile: Arc<Profile>,
    updated_list: Arc<UpdateList>,
    inputs_port: Vec<Arc<InputPort>>,
    outputs_port: Vec<Arc<OutputPort>>,
}

impl Node {
    pub fn create(
        pid: usize,
        scope: Option<PlanScope>,
        processor: &ProcessorPtr,
        inputs_port: &[Arc<InputPort>],
        outputs_port: &[Arc<OutputPort>],
    ) -> Arc<Node> {
        let p_name = unsafe { processor.name() };
        Arc::new(Node {
            state: std::sync::Mutex::new(State::Idle),
            processor: processor.clone(),
            updated_list: UpdateList::create(),
            inputs_port: inputs_port.to_vec(),
            outputs_port: outputs_port.to_vec(),
            profile: Arc::new(Profile::create(pid, p_name, scope)),
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
    finished_nodes: AtomicUsize,
    graph: StableGraph<Arc<Node>, EdgeInfo>,
}

type StateLockGuard = ExecutingGraph;

impl ExecutingGraph {
    pub fn create(mut pipeline: Pipeline) -> Result<ExecutingGraph> {
        let mut graph = StableGraph::new();
        Self::init_graph(&mut pipeline, &mut graph);
        Ok(ExecutingGraph {
            graph,
            finished_nodes: AtomicUsize::new(0),
        })
    }

    pub fn from_pipelines(mut pipelines: Vec<Pipeline>) -> Result<ExecutingGraph> {
        let mut graph = StableGraph::new();

        for pipeline in &mut pipelines {
            Self::init_graph(pipeline, &mut graph);
        }

        Ok(ExecutingGraph {
            finished_nodes: AtomicUsize::new(0),
            graph,
        })
    }

    fn init_graph(pipeline: &mut Pipeline, graph: &mut StableGraph<Arc<Node>, EdgeInfo>) {
        #[derive(Debug)]
        struct Edge {
            source_port: usize,
            source_node: NodeIndex,
            target_port: usize,
            target_node: NodeIndex,
        }

        let mut pipes_edges: Vec<Vec<Edge>> = Vec::new();
        for pipe in &pipeline.pipes {
            assert_eq!(
                pipe.input_length,
                pipes_edges.last().map(|x| x.len()).unwrap_or_default()
            );

            let mut edge_index = 0;
            let mut pipe_edges = Vec::with_capacity(pipe.output_length);

            for item in &pipe.items {
                let pid = graph.node_count();
                let node = Node::create(
                    pid,
                    pipe.scope.clone(),
                    &item.processor,
                    &item.inputs_port,
                    &item.outputs_port,
                );

                let graph_node_index = graph.add_node(node.clone());
                unsafe {
                    item.processor.set_id(graph_node_index);
                }

                for offset in 0..item.inputs_port.len() {
                    let last_edges = pipes_edges.last_mut().unwrap();

                    last_edges[edge_index].target_port = offset;
                    last_edges[edge_index].target_node = graph_node_index;
                    edge_index += 1;
                }

                for offset in 0..item.outputs_port.len() {
                    pipe_edges.push(Edge {
                        source_port: offset,
                        source_node: graph_node_index,
                        target_port: 0,
                        target_node: Default::default(),
                    });
                }
            }

            pipes_edges.push(pipe_edges);
        }

        // The last pipe cannot contain any output edge.
        assert!(pipes_edges.last().map(|x| x.is_empty()).unwrap_or_default());
        pipes_edges.pop();

        for pipe_edges in &pipes_edges {
            for edge in pipe_edges {
                let edge_index = graph.add_edge(edge.source_node, edge.target_node, EdgeInfo {
                    input_index: edge.target_port,
                    output_index: edge.source_port,
                });

                unsafe {
                    let (target_node, target_port) = (edge.target_node, edge.target_port);
                    let input_trigger = graph[target_node].create_trigger(edge_index);
                    graph[target_node].inputs_port[target_port].set_trigger(input_trigger);

                    let (source_node, source_port) = (edge.source_node, edge.source_port);
                    let output_trigger = graph[source_node].create_trigger(edge_index);
                    graph[source_node].outputs_port[source_port].set_trigger(output_trigger);

                    if graph[source_node].profile.plan_id.is_some()
                        && graph[source_node].profile.plan_id != graph[target_node].profile.plan_id
                    {
                        graph[source_node].outputs_port[source_port].record_profile();
                    }

                    connect(
                        &graph[target_node].inputs_port[target_port],
                        &graph[source_node].outputs_port[source_port],
                    );
                }
            }
        }
    }

    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    pub unsafe fn init_schedule_queue(
        locker: &StateLockGuard,
        capacity: usize,
        graph: &Arc<RunningGraph>
    ) -> Result<ScheduleQueue> {
        let mut schedule_queue = ScheduleQueue::with_capacity(capacity);
        for sink_index in locker.graph.externals(Direction::Outgoing) {
            ExecutingGraph::schedule_queue(locker, sink_index, &mut schedule_queue, graph)?;
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
        graph: &Arc<RunningGraph>,
    ) -> Result<()> {
        let mut need_schedule_nodes = VecDeque::new();
        let mut need_schedule_edges = VecDeque::new();

        need_schedule_nodes.push_back(index);

        while !need_schedule_nodes.is_empty() || !need_schedule_edges.is_empty() {
            // To avoid lock too many times, we will try to cache lock.
            let mut state_guard_cache = None;
            let mut event_cause = EventCause::Other;

            if need_schedule_nodes.is_empty() {
                let edge = need_schedule_edges.pop_front().unwrap();
                let target_index = DirectedEdge::get_target(&edge, &locker.graph)?;

                event_cause = match edge {
                    DirectedEdge::Source(index) => {
                        EventCause::Input(locker.graph.edge_weight(index).unwrap().input_index)
                    }
                    DirectedEdge::Target(index) => {
                        EventCause::Output(locker.graph.edge_weight(index).unwrap().output_index)
                    }
                };

                let node = &locker.graph[target_index];
                let node_state = node.state.lock().unwrap();

                if matches!(*node_state, State::Idle) {
                    state_guard_cache = Some(node_state);
                    need_schedule_nodes.push_back(target_index);
                } else {
                    node.processor.un_reacted(event_cause.clone())?;
                }
            }

            if let Some(schedule_index) = need_schedule_nodes.pop_front() {
                let node = &locker.graph[schedule_index];

                Profile::track_profile(&node.profile);

                if state_guard_cache.is_none() {
                    state_guard_cache = Some(node.state.lock().unwrap());
                }
                let event = node.processor.event(event_cause)?;
                trace!(
                    "node id: {:?}, name: {:?}, event: {:?}",
                    node.processor.id(),
                    node.processor.name(),
                    event
                );
                let processor_state = match event {
                    Event::Finished => {
                        if !matches!(state_guard_cache.as_deref(), Some(State::Finished)) {
                            locker.finished_nodes.fetch_add(1, Ordering::SeqCst);
                        }

                        State::Finished
                    }
                    Event::NeedData | Event::NeedConsume => State::Idle,
                    Event::Sync => {
                        schedule_queue.push_sync(ProcessorWrapper{
                            processor: node.processor.clone(),
                            graph: graph.clone(),
                        });
                        State::Processing
                    }
                    Event::Async => {
                        schedule_queue.push_async(ProcessorWrapper{
                            processor: node.processor.clone(),
                            graph: graph.clone()
                        });
                        State::Processing
                    }
                };

                node.trigger(&mut need_schedule_edges);
                *state_guard_cache.unwrap() = processor_state;
            }
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct ProcessorWrapper {
    pub processor: ProcessorPtr,
    pub graph: Arc<RunningGraph>
}

pub struct ScheduleQueue {
    pub sync_queue: VecDeque<ProcessorWrapper>,
    pub async_queue: VecDeque<ProcessorWrapper>,
}

impl ScheduleQueue {
    pub fn with_capacity(capacity: usize) -> ScheduleQueue {
        ScheduleQueue {
            sync_queue: VecDeque::with_capacity(capacity),
            async_queue: VecDeque::with_capacity(capacity),
        }
    }

    #[inline]
    pub fn push_sync(&mut self, processor: ProcessorWrapper) {
        self.sync_queue.push_back(processor);
    }

    #[inline]
    pub fn push_async(&mut self, processor: ProcessorWrapper) {
        self.async_queue.push_back(processor);
    }

    pub fn schedule(
        mut self,
        global: &Arc<ExecutorTasksQueue>,
        context: &mut ExecutorWorkerContext,
        executor: &Arc<PipelineExecutor>,
    ) {
        debug_assert!(!context.has_task());

        while let Some(processor) = self.async_queue.pop_front() {
            Self::schedule_async_task(
                processor,
                context.query_id.clone(),
                executor,
                context.get_worker_id(),
                context.get_workers_condvar().clone(),
                global.clone(),
            )
        }

        if !self.sync_queue.is_empty() {
            self.schedule_sync(global, context);
        }

        if !self.sync_queue.is_empty() {
            self.schedule_tail(global, context);
        }
    }

    pub fn schedule_async_task(
        proc: ProcessorWrapper,
        query_id: Arc<String>,
        executor: &Arc<PipelineExecutor>,
        wakeup_worker_id: usize,
        workers_condvar: Arc<WorkersCondvar>,
        global_queue: Arc<ExecutorTasksQueue>,
    ) {
        unsafe {
            workers_condvar.inc_active_async_worker();
            let weak_executor = Arc::downgrade(executor);
            let graph = proc.graph;
            let node_profile = executor.graph.get_node_profile(proc.processor.id()).clone();
            let process_future = proc.processor.async_process();
            executor.async_runtime.spawn(
                query_id.as_ref().clone(),
                TrackedFuture::create(ProcessorAsyncTask::create(
                    query_id,
                    wakeup_worker_id,
                    proc.processor.clone(),
                    global_queue,
                    workers_condvar,
                    weak_executor,
                    node_profile,
                    graph,
                    process_future,
                ))
                .in_span(Span::enter_with_local_parent(std::any::type_name::<
                    ProcessorAsyncTask,
                >())),
            );
        }
    }

    fn schedule_sync(&mut self, _: &ExecutorTasksQueue, ctx: &mut ExecutorWorkerContext) {
        if let Some(processor) = self.sync_queue.pop_front() {
            ctx.set_task(ExecutorTask::Sync(processor));
        }
    }

    pub fn schedule_tail(mut self, global: &ExecutorTasksQueue, ctx: &mut ExecutorWorkerContext) {
        let mut tasks = VecDeque::with_capacity(self.sync_queue.len());

        while let Some(processor) = self.sync_queue.pop_front() {
            tasks.push_back(ExecutorTask::Sync(processor));
        }

        global.push_tasks(ctx, tasks)
    }
}

pub struct RunningGraph(ExecutingGraph);

impl RunningGraph {
    pub fn create(pipeline: Pipeline) -> Result<Arc<RunningGraph>> {
        let graph_state = ExecutingGraph::create(pipeline)?;
        debug!("Create running graph:{:?}", graph_state);
        Ok(Arc::new(RunningGraph(graph_state)))
    }

    pub fn from_pipelines(pipelines: Vec<Pipeline>) -> Result<Arc<RunningGraph>> {
        let graph_state = ExecutingGraph::from_pipelines(pipelines)?;
        debug!("Create running graph:{:?}", graph_state);
        Ok(Arc::new(RunningGraph(graph_state)))
    }

    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    pub unsafe fn init_schedule_queue(self: Arc<Self>, capacity: usize) -> Result<ScheduleQueue> {
        ExecutingGraph::init_schedule_queue(&self.0, capacity, &self)
    }

    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    pub unsafe fn schedule_queue(self: Arc<Self>, node_index: NodeIndex) -> Result<ScheduleQueue> {
        let mut schedule_queue = ScheduleQueue::with_capacity(0);
        ExecutingGraph::schedule_queue(&self.0, node_index, &mut schedule_queue, &self)?;
        Ok(schedule_queue)
    }

    pub(crate) fn get_node_profile(&self, pid: NodeIndex) -> &Arc<Profile> {
        &self.0.graph[pid].profile
    }

    pub fn get_proc_profiles(&self) -> Vec<Arc<Profile>> {
        self.0
            .graph
            .node_weights()
            .map(|x| x.profile.clone())
            .collect::<Vec<_>>()
    }

    pub fn interrupt_running_nodes(&self) {
        unsafe {
            for node_index in self.0.graph.node_indices() {
                self.0.graph[node_index].processor.interrupt();
            }
        }
    }

    pub fn assert_finished_graph(&self) -> Result<()> {
        let finished_nodes = self.0.finished_nodes.load(Ordering::SeqCst);

        match finished_nodes >= self.0.graph.node_count() {
            true => Ok(()),
            false => Err(ErrorCode::Internal(format!(
                "Pipeline graph is not finished, details: {}",
                self.format_graph_nodes()
            ))),
        }
    }

    pub fn format_graph_nodes(&self) -> String {
        pub struct NodeDisplay {
            id: usize,
            name: String,
            state: String,
            details_status: Option<String>,
            inputs_status: Vec<(&'static str, &'static str, &'static str)>,
            outputs_status: Vec<(&'static str, &'static str, &'static str)>,
        }

        impl Debug for NodeDisplay {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                match &self.details_status {
                    None => f
                        .debug_struct("Node")
                        .field("name", &self.name)
                        .field("id", &self.id)
                        .field("state", &self.state)
                        .field("inputs_status", &self.inputs_status)
                        .field("outputs_status", &self.outputs_status)
                        .finish(),
                    Some(details_status) => f
                        .debug_struct("Node")
                        .field("name", &self.name)
                        .field("id", &self.id)
                        .field("state", &self.state)
                        .field("inputs_status", &self.inputs_status)
                        .field("outputs_status", &self.outputs_status)
                        .field("details", details_status)
                        .finish(),
                }
            }
        }

        let mut nodes_display = Vec::with_capacity(self.0.graph.node_count());

        for node_index in self.0.graph.node_indices() {
            unsafe {
                let state = self.0.graph[node_index].state.lock().unwrap();
                let inputs_status = self.0.graph[node_index]
                    .inputs_port
                    .iter()
                    .map(|x| {
                        let finished = match x.is_finished() {
                            true => "Finished",
                            false => "Unfinished",
                        };

                        let has_data = match x.has_data() {
                            true => "HasData",
                            false => "Nodata",
                        };

                        let need_data = match x.is_need_data() {
                            true => "NeedData",
                            false => "UnNeeded",
                        };

                        (finished, has_data, need_data)
                    })
                    .collect::<Vec<_>>();

                let outputs_status = self.0.graph[node_index]
                    .outputs_port
                    .iter()
                    .map(|x| {
                        let finished = match x.is_finished() {
                            true => "Finished",
                            false => "Unfinished",
                        };

                        let has_data = match x.has_data() {
                            true => "HasData",
                            false => "Nodata",
                        };

                        let need_data = match x.is_need_data() {
                            true => "NeedData",
                            false => "UnNeeded",
                        };

                        (finished, has_data, need_data)
                    })
                    .collect::<Vec<_>>();

                nodes_display.push(NodeDisplay {
                    inputs_status,
                    outputs_status,
                    id: self.0.graph[node_index].processor.id().index(),
                    name: self.0.graph[node_index].processor.name(),
                    details_status: self.0.graph[node_index].processor.details_status(),
                    state: String::from(match *state {
                        State::Idle => "Idle",
                        State::Processing => "Processing",
                        State::Finished => "Finished",
                    }),
                });
            }
        }

        format!("{:?}", nodes_display)
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
        // let graph = self.0.read();
        write!(f, "{:?}", self.0)
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
                    id: item.processor.id().index(),
                    name: item.processor.name().to_string(),
                })
            }

            for item in &self.async_queue {
                async_queue.push(QueueItem {
                    id: item.processor.id().index(),
                    name: item.processor.name().to_string(),
                })
            }

            f.debug_struct("ScheduleQueue")
                .field("sync_queue", &sync_queue)
                .field("async_queue", &async_queue)
                .finish()
        }
    }
}
