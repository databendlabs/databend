// running graph
use std::collections::VecDeque;
use std::sync::Arc;
use petgraph::prelude::{EdgeIndex, NodeIndex, StableGraph};

use common_exception::Result;
use common_infallible::Mutex;
use common_infallible::RwLock;
use common_infallible::RwLockUpgradableReadGuard;

use crate::pipelines::new::executor::executor_tasks::ExecutorTasksQueue;
use crate::pipelines::new::executor::executor_worker_context::{ExecutorTask, ExecutorWorkerContext};
use crate::pipelines::new::pipe::NewPipe;
use crate::pipelines::new::pipeline::NewPipeline;
use crate::pipelines::new::processors::processor::Event;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::{connect, UpdateList, UpdateTrigger, DirectedEdge};
use crate::pipelines::new::processors::port::{InputPort, OutputPort};

enum State {
    Idle,
    Preparing,
    Processing,
    Finished,
}

struct Node {
    state: Mutex<State>,
    processor: ProcessorPtr,

    updated_list: Arc<UpdateList>,
    inputs_port: Vec<Arc<InputPort>>,
    outputs_port: Vec<Arc<OutputPort>>,
}

impl Node {
    pub fn create(processor: &ProcessorPtr, inputs_port: &[Arc<InputPort>], outputs_port: &[Arc<OutputPort>]) -> Arc<Node> {
        Arc::new(Node {
            state: Mutex::new(State::Idle),
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
    pub fn create(mut pipeline: NewPipeline) -> Result<ExecutingGraph> {
        let mut graph = StableGraph::new();

        let mut node_stack = Vec::new();
        let mut edge_stack: Vec<Arc<OutputPort>> = Vec::new();
        for query_pipe in &pipeline.pipes {
            match query_pipe {
                NewPipe::ResizePipe { processor: processor, inputs_port, outputs_port } => unsafe {
                    assert_eq!(node_stack.len(), inputs_port.len());

                    let resize_node = Node::create(processor, inputs_port, outputs_port);
                    let target_index = graph.add_node(resize_node.clone());

                    for index in 0..node_stack.len() {
                        let source_index = node_stack[index];
                        let edge_index = graph.add_edge(source_index, target_index, ());

                        inputs_port[index].set_trigger(resize_node.create_trigger(edge_index));
                        edge_stack[index].set_trigger(graph[source_index].create_trigger(edge_index));
                        connect(&inputs_port[index], &edge_stack[index]);
                    }

                    node_stack.clear();
                    edge_stack.clear();
                    for output_port in outputs_port {
                        node_stack.push(target_index);
                        edge_stack.push(output_port.clone());
                    }
                }
                NewPipe::SimplePipe { processors, inputs_port, outputs_port } => unsafe {
                    assert_eq!(node_stack.len(), inputs_port.len());
                    assert!(inputs_port.is_empty() || inputs_port.len() == processors.len());
                    assert!(outputs_port.is_empty() || outputs_port.len() == processors.len());

                    let mut new_node_stack = Vec::with_capacity(outputs_port.len());
                    let mut new_edge_stack = Vec::with_capacity(outputs_port.len());

                    for index in 0..processors.len() {
                        let target_node = Node::create(
                            &processors[index],
                            &[inputs_port[index].clone()],
                            &[outputs_port[index].clone()],
                        );

                        let target_index = graph.add_node(target_node.clone());

                        let source_index = node_stack[index];
                        let edge_index = graph.add_edge(source_index, target_index, ());

                        inputs_port[index].set_trigger(target_node.create_trigger(edge_index));
                        edge_stack[index].set_trigger(graph[source_index].create_trigger(edge_index));
                        connect(&inputs_port[index], &edge_stack[index]);

                        if !outputs_port.is_empty() {
                            new_node_stack.push(target_index);
                            new_edge_stack.push(outputs_port[index].clone());
                        }
                    }

                    node_stack = new_node_stack;
                    edge_stack = new_edge_stack;
                }
            };
        }

        // Assert no output.
        assert_eq!(node_stack.len(), 0);
        Ok(ExecutingGraph { graph })
    }

    pub unsafe fn schedule_queue(locker: &StateLockGuard, index: NodeIndex) -> Result<ScheduleQueue> {
        let mut need_schedule_nodes = VecDeque::new();
        let mut need_schedule_edges = VecDeque::new();
        let mut schedule_queue = ScheduleQueue::create();

        need_schedule_nodes.push_back(index);
        while !need_schedule_nodes.is_empty() || !need_schedule_edges.is_empty() {

            // To avoid lock too many times, we will try to cache lock.
            let mut state_guard_cache = None;

            if need_schedule_nodes.is_empty() {
                let edge = need_schedule_edges.pop_front().unwrap();
                let target_index = DirectedEdge::get_target(&edge, &locker.graph);

                let node = &locker.graph[target_index];
                let node_state = node.state.lock();

                if matches!(*node_state, State::Idle) {
                    state_guard_cache = Some(node_state);
                    need_schedule_nodes.push_back(target_index);
                }
            }

            if let Some(schedule_index) = need_schedule_nodes.pop_front() {
                let node = &locker.graph[schedule_index];
                let mut node_status = match state_guard_cache.take() {
                    None => node.state.lock(),
                    Some(status_guard) => status_guard
                };

                *node_status = match node.processor.event()? {
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

        Ok(schedule_queue)
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

    pub fn schedule_tail(mut self, worker_id: usize, global: &ExecutorTasksQueue) {
        let mut tasks = VecDeque::with_capacity(self.sync_queue.len());

        while let Some(processor) = self.sync_queue.pop_front() {
            tasks.push_back(ExecutorTask::Sync(processor));
        }

        while let Some(processor) = self.async_queue.pop_front() {
            tasks.push_back(ExecutorTask::Async(processor));
        }

        global.push_tasks(worker_id, tasks)
    }

    pub fn schedule(mut self, global: &ExecutorTasksQueue, context: &mut ExecutorWorkerContext) {
        debug_assert!(!context.has_task());

        match self.sync_queue.is_empty() {
            true => self.schedule_async(global, context),
            false if !self.async_queue.is_empty() => self.schedule_sync(global, context),
            false => { /* do nothing*/ }
        }

        self.schedule_tail(context.get_worker_num(), global)
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
        let mut graph_state = ExecutingGraph::create(pipeline)?;
        // graph_state.initialize_tasks()?;
        Ok(RunningGraph(RwLock::new(graph_state)))
    }
}

impl RunningGraph {
    pub unsafe fn schedule_queue(&self, node_index: NodeIndex) -> Result<ScheduleQueue> {
        ExecutingGraph::schedule_queue(&self.0.upgradable_read(), node_index)
    }
}
