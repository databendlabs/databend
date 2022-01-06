// running graph

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use petgraph::Direction;
use petgraph::prelude::EdgeRef;

use common_clickhouse_srv::protocols::Stage::Default;
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
use crate::pipelines::new::processors::{PortReactor, UpdateList};
use crate::pipelines::new::processors::Processor;
use crate::pipelines::new::processors::Processors;

pub enum RunningState {
    Idle,
    Preparing,
    Processing,
    Finished,
}

pub struct RunningProcessor {
    state: RunningState,
    processor: ProcessorPtr,
    inputs: Arc<UpdateList>,
    outputs: Arc<UpdateList>,
}

unsafe impl Send for RunningProcessor {}

impl RunningProcessor {
    pub fn create(processor: &ProcessorPtr, input_list: &Arc<UpdateList>, output_list: &Arc<UpdateList>) -> Arc<Mutex<RunningProcessor>> {
        Arc::new(Mutex::new(RunningProcessor {
            state: RunningState::Idle,
            processor: processor.clone(),
            inputs: input_list.clone(),
            outputs: output_list.clone(),
        }))
    }
}

struct RunningGraphState {
    nodes: Vec<Arc<Mutex<RunningProcessor>>>,
}

type StateLockGuard<'a> = RwLockUpgradableReadGuard<'a, RunningGraphState>;

impl RunningGraphState {
    pub fn create(mut graph: NewPipeline) -> Result<RunningGraphState> {
        let mut nodes = Vec::with_capacity(graph.node_count());

        for index in graph.node_indices() {
            if let Some(node) = graph.node_weight(index) {
                let inputs = UpdateList::create();
                let outputs = UpdateList::create();
                nodes.push(RunningProcessor::create(node, &inputs, &outputs));

                for input in graph.edges_directed(index, Direction::Incoming) {
                    input.weight().set_input_trigger(input.source().index(), &inputs);
                }

                for output in graph.edges_directed(index, Direction::Outgoing) {
                    output.weight().set_output_trigger(output.target().index(), &outputs);
                }
            }
        }

        Ok(RunningGraphState { nodes })
    }

    pub fn initialize_executor(state: &RwLock<RunningGraphState>) -> Result<()> {
        let graph = state.upgradable_read();
        unimplemented!()
    }

    pub unsafe fn schedule_next(graph: &StateLockGuard, pid: usize) -> Result<ScheduleQueue> {
        let mut need_schedule_nodes = vec![pid];
        let mut schedule_queue = ScheduleQueue::create();

        while !need_schedule_nodes.is_empty() {
            if let Some(need_schedule_pid) = need_schedule_nodes.pop() {
                let mut node = graph.nodes[need_schedule_pid].lock();

                match (*node.processor.get()).event()? {
                    Event::Finished => {
                        node.state = RunningState::Finished;
                    }
                    Event::NeedData | Event::NeedConsume => {
                        node.state = RunningState::Idle;
                    }
                    Event::Sync => {
                        node.state = RunningState::Processing;
                        schedule_queue.push_sync(node.processor.clone());
                    }
                    Event::Async => {
                        node.state = RunningState::Processing;
                        schedule_queue.push_async(node.processor.clone());
                    }
                }
            }
        }
        unimplemented!()
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

    pub fn push_sync(&mut self, processor: ProcessorPtr) {
        self.sync_queue.push_back(processor)
    }

    pub fn push_async(&mut self, processor: ProcessorPtr) {
        self.async_queue.push_back(processor)
    }

    pub fn schedule_tail(mut self, worker_id: usize, global: &ExecutorTasksQueue) {
        let mut tasks = VecDeque::with_capacity(self.sync_queue.len());

        if !self.sync_queue.is_empty() {
            while let Some(processor) = self.sync_queue.pop_front() {
                tasks.push_back(ExecutorTask::Sync(processor));
            }
        }

        if !self.async_queue.is_empty() {
            while let Some(processor) = self.async_queue.pop_front() {
                tasks.push_back(ExecutorTask::Async(processor));
            }
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
        Ok(RunningGraph(RwLock::new(RunningGraphState::create(pipeline)?)))
    }
}

impl RunningGraph {
    pub fn schedule_next(&self, pid: usize) -> Result<ScheduleQueue> {
        unsafe { RunningGraphState::schedule_next(&self.0.upgradable_read(), pid) }
    }

    pub fn initialize_executor(&self) -> Result<()> {
        RunningGraphState::initialize_executor(&self.0)
    }
}
