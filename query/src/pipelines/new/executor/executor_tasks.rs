use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::{Arc};
use futures::future::BoxFuture;
use petgraph::prelude::NodeIndex;

use common_exception::Result;
use common_infallible::Mutex;

use crate::pipelines::new::executor::executor_worker_context::{ExecutorWorkerContext, ExecutorTask};
use crate::pipelines::new::processors::processor::ProcessorPtr;

pub struct ExecutorTasksQueue {
    finished: AtomicBool,
    workers_tasks: Mutex<ExecutorTasks>,
}

impl ExecutorTasksQueue {
    pub fn create(workers_size: usize) -> ExecutorTasksQueue {
        ExecutorTasksQueue {
            finished: AtomicBool::new(false),
            workers_tasks: Mutex::new(ExecutorTasks::create(workers_size)),
        }
    }

    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Relaxed)
    }

    // Pull task from the global task queue
    pub fn steal_task_to_context(&self, context: &mut ExecutorWorkerContext) {
        unsafe {
            let mut workers_tasks = self.workers_tasks.lock();
            if !workers_tasks.is_empty() {
                let task = workers_tasks.pop_task(context.get_worker_num());
                context.set_task(task);

                if !workers_tasks.is_empty() {
                    // TODO:
                    // match context.get_worker_num() {
                    // }
                }

                return;
            }
        }

        self.wait_wakeup(context)
    }

    pub fn init_tasks(&self, mut tasks: VecDeque<ExecutorTask>) {
        unsafe {
            let mut worker_id = 0;
            let mut workers_tasks = self.workers_tasks.lock();
            while let Some(task) = tasks.pop_front() {
                workers_tasks.push_task(worker_id, task);

                worker_id += 1;
                if worker_id == workers_tasks.workers_sync_tasks.len() {
                    worker_id = 0;
                }
            }
        }
    }

    pub fn push_tasks(&self, worker_id: usize, mut tasks: VecDeque<ExecutorTask>) {
        unsafe {
            let mut workers_tasks = self.workers_tasks.lock();
            while let Some(task) = tasks.pop_front() {
                workers_tasks.push_task(worker_id, task);
            }
        }
    }

    pub fn push_executing_async_task(&self, worker_id: usize, task: ExecutingAsyncTask) -> Option<ExecutingAsyncTask> {
        unsafe {
            let mut workers_tasks = self.workers_tasks.lock();

            // The finished when wait the lock tasks. TODO: maybe use try lock.
            match task.finished.load(Ordering::Relaxed) {
                true => Some(task),
                false => {
                    workers_tasks.push_executing_async_task(worker_id, task);
                    None
                }
            }
        }
    }

    pub fn wait_wakeup(&self, thread: &mut ExecutorWorkerContext) {
        // condvar.wait(guard);
        // TODO:
        unimplemented!()
    }
}

pub struct ExecutingAsyncTask {
    pub id: NodeIndex,
    pub finished: Arc<AtomicBool>,
    pub future: BoxFuture<'static, Result<()>>,
}

struct ExecutorTasks {
    tasks_size: usize,
    workers_sync_tasks: Vec<VecDeque<ProcessorPtr>>,
    workers_async_tasks: Vec<VecDeque<ProcessorPtr>>,
    workers_executing_async_tasks: Vec<VecDeque<ExecutingAsyncTask>>,
}

unsafe impl Send for ExecutorTasks {}

impl ExecutorTasks {
    pub fn create(workers_size: usize) -> ExecutorTasks {
        let mut workers_sync_tasks = Vec::with_capacity(workers_size);
        let mut workers_async_tasks = Vec::with_capacity(workers_size);
        let mut workers_executing_async_tasks = Vec::with_capacity(workers_size);

        for _index in 0..workers_size {
            workers_sync_tasks.push(VecDeque::new());
            workers_async_tasks.push(VecDeque::new());
            workers_executing_async_tasks.push(VecDeque::new());
        }

        ExecutorTasks {
            tasks_size: 0,
            workers_sync_tasks,
            workers_async_tasks,
            workers_executing_async_tasks,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.tasks_size == 0
    }

    #[inline]
    fn pop_worker_task(&mut self, worker_id: usize) -> ExecutorTask {
        if let Some(processor) = self.workers_sync_tasks[worker_id].pop_front() {
            return ExecutorTask::Sync(processor);
        }

        if let Some(processor) = self.workers_async_tasks[worker_id].pop_front() {
            return ExecutorTask::Async(processor);
        }

        if !self.workers_executing_async_tasks[worker_id].is_empty() {
            let async_tasks = &mut self.workers_executing_async_tasks[worker_id];
            for index in 0..async_tasks.len() {
                if async_tasks[index].finished.load(Ordering::Relaxed) {
                    return ExecutorTask::AsyncSchedule(async_tasks.swap_remove_front(index).unwrap());
                }
            }
        }

        ExecutorTask::None
    }

    pub unsafe fn pop_task(&mut self, mut worker_id: usize) -> ExecutorTask {
        for _index in 0..self.workers_sync_tasks.len() {
            match self.pop_worker_task(worker_id) {
                ExecutorTask::None => {
                    worker_id += 1;
                    if worker_id >= self.workers_sync_tasks.len() {
                        worker_id = 0;
                    }
                }
                other => { return other; }
            }
        }

        ExecutorTask::None
    }

    pub unsafe fn push_task(&mut self, worker_id: usize, task: ExecutorTask) {
        self.tasks_size += 1;
        let sync_queue = &mut self.workers_sync_tasks[worker_id];
        let async_queue = &mut self.workers_async_tasks[worker_id];
        let executing_queue = &mut self.workers_executing_async_tasks[worker_id];

        match task {
            ExecutorTask::None => unreachable!(),
            ExecutorTask::Sync(processor) => sync_queue.push_back(processor),
            ExecutorTask::Async(processor) => async_queue.push_back(processor),
            ExecutorTask::AsyncSchedule(task) => executing_queue.push_back(task),
        }
    }

    pub unsafe fn push_executing_async_task(&mut self, worker: usize, task: ExecutingAsyncTask) {
        self.tasks_size += 1;
        self.workers_executing_async_tasks[worker].push_back(task)
    }
}
