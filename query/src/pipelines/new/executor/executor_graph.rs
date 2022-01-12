// running graph
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::ops::Index;
use std::sync::Arc;
use petgraph::Direction;
use petgraph::prelude::{EdgeRef, NodeIndex, StableGraph};

use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::Mutex;
use common_infallible::RwLock;
use common_infallible::RwLockUpgradableReadGuard;

use crate::pipelines::new::executor::executor_tasks::ExecutorTasksQueue;
use crate::pipelines::new::executor::executor_worker_context::{ExecutorTask, ExecutorWorkerContext};
use crate::pipelines::new::pipeline::NewPipeline;
use crate::pipelines::new::processors::processor::Event;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::{ActivePort, UpdateList};
use crate::pipelines::new::processors::port::{InputPort, OutputPort};
use crate::pipelines::new::processors::Processor;
use crate::pipelines::new::processors::Processors;

pub enum RunningState {
    Idle,
    Preparing,
    Processing,
    Finished,
}

struct Node {
    state: RunningState,
    processor: ProcessorPtr,
    wakeup_inputs_list: UpdateList,
    wakeup_outputs_list: UpdateList,
}

unsafe impl Send for Node {}

impl Node {
    pub fn new(processor: &ProcessorPtr) -> Node {}

    // pub fn create(processor: &ProcessorPtr, inputs: &Arc<UpdateList>, outputs: &Arc<UpdateList>) -> Arc<Mutex<Node>> {
    //     Arc::new(Mutex::new(Node {
    //         state: RunningState::Idle,
    //         processor: processor.clone(),
    //         inputs: inputs.clone(),
    //         outputs: outputs.clone(),
    //     }))
    // }
}

struct ExecutingGraph {
    graph: StableGraph<Arc<Mutex<Node>>, ()>,
}

impl ExecutingGraph {
    pub fn create(mut graph: NewPipeline) -> Result<ExecutingGraph> {
        let mut executing_graph = StableGraph::with_capacity(graph.node_count(), graph.edge_count());

        let mut node_map = HashMap::with_capacity(graph.node_count());
        for node_index in graph.node_indices() {
            let processor = &graph[node_index];
            let new_node_index = executing_graph.add_node(Node::new(processor));
            node_map.insert(node_index, new_node_index);
        }

        for edge_index in graph.edge_indices() {
            let (source, target) = graph.edge_endpoints(edge_index).unwrap();

            let new_source = &node_map[&source];
            let new_target = &node_map[&target];
            executing_graph.add_edge(new_source.clone(), new_target.clone(), ());
        }

        Ok(ExecutingGraph { graph: executing_graph })
    }

    pub unsafe fn schedule_queue(&self, index: NodeIndex) -> Result<ScheduleQueue> {
        let mut need_schedule_node = vec![index];
        let mut need_schedule_edges = vec![];
        let mut schedule_queue = ScheduleQueue::create();

        while !need_schedule_node.is_empty() || !need_schedule_edges.is_empty() {
            if let Some(schedule_index) = need_schedule_node.pop() {
                let mut node = self.graph[schedule_index].lock();

                node.state = match node.processor.event()? {
                    Event::Finished => RunningState::Finished,
                    Event::NeedData | Event::NeedConsume => RunningState::Idle,
                    Event::Sync => schedule_queue.push_sync(node.processor.clone()),
                    Event::Async => schedule_queue.push_async(node.processor.clone()),
                };

                // node.updated_inputs_list
                // node.updated_outputs_list
            }
        }

        Ok(schedule_queue)
    }
}

struct RunningGraphState {
    nodes: Vec<Arc<Mutex<Node>>>,
    graph: NewPipeline,
}

type StateLockGuard<'a> = RwLockUpgradableReadGuard<'a, RunningGraphState>;

// impl RunningGraphState {
//     pub fn create(mut graph: NewPipeline) -> Result<RunningGraphState> {
//         let mut nodes = Vec::with_capacity(graph.node_count());
//
//         for index in graph.node_indices() {
//             let node = &graph[index];
//             let inputs = UpdateList::create();
//             let outputs = UpdateList::create();
//             nodes.push(Node::create(node, &inputs, &outputs));
//
//             for input in graph.edges_directed(index, Direction::Incoming) {
//                 input.weight().set_input_trigger(input.source().index(), &inputs);
//             }
//
//             for output in graph.edges_directed(index, Direction::Outgoing) {
//                 output.weight().set_output_trigger(output.target().index(), &outputs);
//             }
//         }
//
//         Ok(RunningGraphState { nodes, graph })
//     }
//
//     pub fn initialize_tasks(&mut self) -> Result<()> {
//         // TODO:
//         // self.graph.
//         unimplemented!()
//     }
//
//     pub unsafe fn schedule_queue(graph: &StateLockGuard, pid: usize) -> Result<ScheduleQueue> {
//         let mut need_schedule_nodes = vec![pid];
//         let mut schedule_queue = ScheduleQueue::create();
//
//         while !need_schedule_nodes.is_empty() {
//             if let Some(need_schedule_pid) = need_schedule_nodes.pop() {
//                 let mut node = graph.nodes[need_schedule_pid].lock();
//
//                 node.state = match node.processor.event()? {
//                     Event::Finished => RunningState::Finished,
//                     Event::NeedData | Event::NeedConsume => RunningState::Idle,
//                     Event::Sync => schedule_queue.push_sync(node.processor.clone()),
//                     Event::Async => schedule_queue.push_async(node.processor.clone()),
//                 };
//
//                 // for output in node.outputs.list() {
//                 //     need_schedule_nodes.push(*output);
//                 // }
//                 //
//                 // for input in node.inputs.list() {
//                 //     need_schedule_nodes.push(*input);
//                 // }
//                 //
//                 // node.inputs.clear();
//                 // node.outputs.clear();
//             }
//         }
//
//         Ok(schedule_queue)
//     }
// }

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
    pub fn push_sync(&mut self, processor: ProcessorPtr) -> RunningState {
        self.sync_queue.push_back(processor);
        RunningState::Processing
    }

    #[inline]
    pub fn push_async(&mut self, processor: ProcessorPtr) -> RunningState {
        self.async_queue.push_back(processor);
        RunningState::Processing
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

pub struct RunningGraph(RwLock<RunningGraphState>);

impl RunningGraph {
    pub fn create(pipeline: NewPipeline) -> Result<RunningGraph> {
        let mut graph_state = RunningGraphState::create(pipeline)?;
        graph_state.initialize_tasks()?;
        Ok(RunningGraph(RwLock::new(graph_state)))
    }
}

impl RunningGraph {
    pub unsafe fn schedule_queue(&self, pid: usize) -> Result<ScheduleQueue> {
        RunningGraphState::schedule_queue(&self.0.upgradable_read(), pid)
    }
}
