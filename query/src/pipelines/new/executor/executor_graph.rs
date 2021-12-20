// running graph

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

use common_clickhouse_srv::protocols::Stage::Default;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::Mutex;
use common_infallible::RwLock;
use common_infallible::RwLockUpgradableReadGuard;

use crate::pipelines::new::executor::executor_tasks::ExecutorTasksQueue;
use crate::pipelines::new::executor::executor_worker_context::ExecutorWorkerContext;
use crate::pipelines::new::processors::create_port;
use crate::pipelines::new::processors::processor::PrepareState;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::PortReactor;
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
    inputs: UnsafeCell<Vec<usize>>,
    outputs: UnsafeCell<Vec<usize>>,
}

unsafe impl Send for RunningProcessor {}

impl RunningProcessor {
    pub fn create(processor: &ProcessorPtr) -> Arc<Mutex<RunningProcessor>> {
        Arc::new(Mutex::new(RunningProcessor {
            state: RunningState::Idle,
            processor: processor.clone(),
            inputs: UnsafeCell::new(vec![]),
            outputs: UnsafeCell::new(vec![]),
        }))
    }
}

struct RunningGraphState {
    nodes: Vec<Arc<Mutex<RunningProcessor>>>,
}

type StateLockGuard<'a> = RwLockUpgradableReadGuard<'a, RunningGraphState>;

impl RunningGraphState {
    pub fn create(
        mut processors: Processors,
        edges: Vec<(usize, usize)>,
    ) -> Result<RunningGraphState> {
        let mut nodes = Vec::with_capacity(processors.len());

        for processor in &processors {
            nodes.push(RunningProcessor::create(processor));
        }

        for (input, output) in edges {
            if input == output {
                return Err(ErrorCode::IllegalPipelineState(""));
            }

            // let (input_port, output_port) = create_port(&nodes, input, output);
            // processors[input].connect_input(input_port)?;
            // processors[output].connect_output(output_port)?;
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

                match (*node.processor.get()).prepare()? {
                    PrepareState::Finished => {
                        node.state = RunningState::Finished;
                    }
                    PrepareState::NeedData | PrepareState::NeedConsume => {
                        node.state = RunningState::Idle;
                    }
                    PrepareState::Sync => {
                        node.state = RunningState::Processing;
                        schedule_queue.push_sync(node.processor.clone());
                    }
                    PrepareState::Async => {
                        node.state = RunningState::Processing;
                        schedule_queue.push_async(node.processor.clone());
                    }
                }

                let need_schedule_inputs = &mut *node.inputs.get();
                let need_schedule_outputs = &mut *node.outputs.get();

                while let Some(need_schedule) = need_schedule_inputs.pop() {}
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

    pub fn schedule(self, global: &ExecutorTasksQueue, context: &mut ExecutorWorkerContext) {
        match self.sync_queue.is_empty() {
            true => self.schedule_async(global, context),
            false => self.schedule_sync(global, context),
        }
        // if !self.sync_queue.is_empty() || !self.async_queue.is_empty() {
        //     match self.sync_queue.is_empty() {
        //         true => {}
        //         false => {}
        //     }
        // }
        //
        // if !self.sync_queue.is_empty() {
        //     // TODO:
        // }
        //
        // if !self.async_queue.is_empty() {
        //     // TODO:
        // }
    }
    fn schedule_sync(mut self, queue: &ExecutorTasksQueue, context: &mut ExecutorWorkerContext) {
        if let Some(processor) = self.sync_queue.pop_front() {
            context.set_sync_task(processor);
        }
    }

    fn schedule_async(mut self, queue: &ExecutorTasksQueue, context: &mut ExecutorWorkerContext) {}
}

pub struct RunningGraph(RwLock<RunningGraphState>);

impl RunningGraph {
    pub fn create(nodes: Processors, edges: Vec<(usize, usize)>) -> Result<RunningGraph> {
        Ok(RunningGraph(RwLock::new(RunningGraphState::create(
            nodes, edges,
        )?)))
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

// Thread safe: because locked before call prepare
// We always push and pull ports in processor prepare method.
impl PortReactor<usize> for RunningProcessor {
    #[inline(always)]
    fn on_push(&self, push_to: usize) {
        unsafe {
            (*self.outputs.get()).push(push_to);
        }
    }

    #[inline(always)]
    fn on_pull(&self, pull_from: usize) {
        unsafe {
            (*self.inputs.get()).push(pull_from);
        }
    }
}
