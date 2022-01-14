// running graph
use std::collections::VecDeque;
use std::sync::Arc;
use petgraph::prelude::{NodeIndex, StableGraph};

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
use crate::pipelines::new::processors::{connect, UpdateList, UpdateTrigger};
use crate::pipelines::new::processors::port::{OutputPort};

pub enum RunningState {
    Idle,
    Preparing,
    Processing,
    Finished,
}

struct Node {
    state: RunningState,
    processor: ProcessorPtr,
    inputs_update_list: UpdateList,
    outputs_update_list: UpdateList,
}

unsafe impl Send for Node {}

impl Node {
    pub fn new(processor: &ProcessorPtr, updated_inputs_list: UpdateList, updated_outputs_list: UpdateList) -> Arc<Mutex<Node>> {
        // Arc::new()
        unimplemented!()
    }
}

struct ExecutingGraph {
    graph: StableGraph<Arc<Mutex<Node>>, ()>,
}

type StateLockGuard<'a> = RwLockUpgradableReadGuard<'a, ExecutingGraph>;

impl ExecutingGraph {
    pub fn create(mut pipeline: NewPipeline) -> Result<ExecutingGraph> {
        let mut graph = StableGraph::new();

        let mut top_nodes_index: Vec<(NodeIndex, OutputPort, UpdateList)> = Vec::new();
        for pipes in &pipeline.pipes {
            match pipes {
                NewPipe::ResizePipe { process: processor, inputs_port, outputs_port } => unsafe {
                    let updated_inputs_list = UpdateList::create();
                    let updated_outputs_list = UpdateList::create();
                    let node_index = graph.add_node(Node::new(&processor, updated_inputs_list.clone(), updated_outputs_list.clone()));

                    assert_eq!(top_nodes_index.len(), inputs_port.len());
                    for (i, (source_index, source_port, source_update_list)) in top_nodes_index.iter().enumerate() {
                        let edge_index = graph.add_edge(*source_index, node_index, ());

                        inputs_port[i].set_trigger(UpdateTrigger::create(edge_index, updated_inputs_list.clone()));
                        source_port.set_trigger(UpdateTrigger::create(edge_index, source_update_list.clone()));
                        connect(&inputs_port[i], source_port);
                    }

                    top_nodes_index.clear();
                    for output_port in outputs_port {
                        top_nodes_index.push((node_index, output_port.clone(), updated_outputs_list.clone()));
                    }
                }
                NewPipe::SimplePipe { processors, inputs_port, outputs_port } => unsafe {
                    assert_eq!(top_nodes_index.len(), inputs_port.len());
                    assert!(inputs_port.is_empty() || inputs_port.len() == processors.len());
                    assert!(outputs_port.is_empty() || outputs_port.len() == processors.len());

                    let mut new_top_nodes_index = Vec::with_capacity(outputs_port.len());
                    for (index, processor) in processors.iter().enumerate() {
                        let updated_inputs_list = UpdateList::create();
                        let updated_outputs_list = UpdateList::create();
                        let node_index = graph.add_node(Node::new(processor, updated_inputs_list.clone(), updated_outputs_list.clone()));

                        if !top_nodes_index.is_empty() {
                            let source_index = top_nodes_index[index].0;
                            let source_port = &top_nodes_index[index].1;
                            let source_update_list = top_nodes_index[index].2.clone();
                            let edge_index = graph.add_edge(source_index, node_index, ());

                            source_port.set_trigger(UpdateTrigger::create(edge_index, source_update_list));
                            inputs_port[index].set_trigger(UpdateTrigger::create(edge_index, updated_inputs_list.clone()));
                            connect(&inputs_port[index], source_port);
                        }

                        if !outputs_port.is_empty() {
                            new_top_nodes_index.push((node_index, outputs_port[index].clone(), updated_outputs_list.clone()));
                        }
                    }

                    top_nodes_index = new_top_nodes_index;
                }
            };
        }

        // Assert no output.
        assert_eq!(top_nodes_index.len(), 0);
        Ok(ExecutingGraph { graph })
    }

    pub unsafe fn schedule_queue(locker: &StateLockGuard, index: NodeIndex) -> Result<ScheduleQueue> {
        let mut need_schedule_node = vec![index];
        let mut need_schedule_edges = VecDeque::new();
        let mut schedule_queue = ScheduleQueue::create();

        while !need_schedule_node.is_empty() || !need_schedule_edges.is_empty() {
            if let Some(schedule_index) = need_schedule_node.pop() {
                let mut node = locker.graph[schedule_index].lock();

                node.state = match node.processor.event()? {
                    Event::Finished => RunningState::Finished,
                    Event::NeedData | Event::NeedConsume => RunningState::Idle,
                    Event::Sync => schedule_queue.push_sync(node.processor.clone()),
                    Event::Async => schedule_queue.push_async(node.processor.clone()),
                };

                node.inputs_update_list.trigger(&mut need_schedule_edges);
                node.outputs_update_list.trigger(&mut need_schedule_edges);
            }
        }

        Ok(schedule_queue)
    }
}

struct RunningGraphState {
    nodes: Vec<Arc<Mutex<Node>>>,
    graph: NewPipeline,
}

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
