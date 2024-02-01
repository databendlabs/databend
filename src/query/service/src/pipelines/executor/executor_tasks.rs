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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use minitrace::future::FutureExt;
use minitrace::Span;

use databend_common_exception::Result;
use parking_lot::Mutex;
use petgraph::prelude::NodeIndex;
use databend_common_base::runtime::{TrackedFuture, TrySpawn};

use crate::pipelines::executor::{ExecutorTask, PipelineExecutor, ProcessorAsyncTask, RunningGraph};
use crate::pipelines::executor::executor_graph::ProcessorWrapper;
use crate::pipelines::executor::ExecutorWorkerContext;
use crate::pipelines::executor::WatchNotify;
use crate::pipelines::executor::WorkersCondvar;
use crate::pipelines::executor::WorkersWaitingStatus;

pub struct WorkersTasks {
    workers_waiting_status: Mutex<WorkersWaitingStatus>,
    /// current_tasks is the task queue we are using in current time slice
    current_tasks: Mutex<ExecutorTasks>,
    /// next_tasks is the task queue we will use in next time slice
    next_tasks: Mutex<ExecutorTasks>,
}

impl WorkersTasks {
    pub fn create(workers_size: usize) -> Self {
        WorkersTasks {
            workers_waiting_status: Mutex::new(WorkersWaitingStatus::create(workers_size)),
            current_tasks: Mutex::new(ExecutorTasks::create(workers_size)),
            next_tasks: Mutex::new(ExecutorTasks::create(workers_size)),
        }
    }
}

pub struct ExecutorTasksQueue {
    finished: Arc<AtomicBool>,
    finished_notify: Arc<WatchNotify>,
    workers_tasks: WorkersTasks,
}

impl ExecutorTasksQueue {
    pub fn create(workers_size: usize) -> Arc<ExecutorTasksQueue> {
        Arc::new(ExecutorTasksQueue {
            finished: Arc::new(AtomicBool::new(false)),
            finished_notify: Arc::new(WatchNotify::new()),
            workers_tasks: WorkersTasks::create(workers_size),
        })
    }

    pub fn finish(&self, workers_condvar: Arc<WorkersCondvar>) {
        self.finished.store(true, Ordering::SeqCst);
        self.finished_notify.notify_waiters();

        let mut workers_waiting_status = self.workers_tasks.workers_waiting_status.lock();
        let mut wakeup_workers =
            Vec::with_capacity(workers_waiting_status.waiting_size());

        while workers_waiting_status.waiting_size() != 0 {
            let worker_id = workers_waiting_status.wakeup_any_worker();
            wakeup_workers.push(worker_id);
        }

        drop(workers_waiting_status);
        for wakeup_worker in wakeup_workers {
            workers_condvar.wakeup(wakeup_worker);
        }
    }

    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::SeqCst)
    }

    /// Pull task from the global task queue
    /// Method is thread unsafe and require thread safe call
    pub fn steal_task_to_context(self: &Arc<Self>, context: &mut ExecutorWorkerContext, executor: &Arc<PipelineExecutor>) {
        let mut current_tasks = self.workers_tasks.current_tasks.lock();
        let mut workers_waiting_status = self.workers_tasks.workers_waiting_status.lock();
        if !current_tasks.is_empty() {
            while !current_tasks.is_empty(){
                let task = current_tasks.pop_task(context.get_worker_id());
                match task {
                    ExecutorTask::Async(processor) => {
                        Self::handle_async_task(
                            processor,
                            context.query_id.clone(),
                            executor,
                            context.get_worker_id(),
                            context.get_workers_condvar().clone(),
                            self.clone()
                        )
                    },
                    other => {
                        context.set_task(other);
                        break;
                    }
                }
            }

            let workers_condvar = context.get_workers_condvar();
            if !current_tasks.is_empty() && workers_waiting_status.waiting_size() != 0
            {
                let worker_id = context.get_worker_id();
                let mut wakeup_worker_id = current_tasks.best_worker_id(worker_id + 1);

                if workers_waiting_status
                    .is_waiting(wakeup_worker_id)
                {
                    workers_waiting_status
                        .wakeup_worker(wakeup_worker_id);
                } else {
                    wakeup_worker_id = workers_waiting_status.wakeup_any_worker();
                }

                drop(workers_waiting_status);
                drop(current_tasks);
                workers_condvar.wakeup(wakeup_worker_id);
            }

            return;
        }

        let workers_condvar = context.get_workers_condvar();
        if !workers_condvar.has_waiting_async_task()
            && workers_waiting_status.is_last_active_worker()
        {
            drop(current_tasks);
            drop(workers_waiting_status);
            if self.switch_queue() {
                executor.increase_global_epoch();
            }else{
                self.finish(workers_condvar.clone());
            }
            return;
        }

        let worker_id = context.get_worker_id();
        workers_waiting_status.wait_worker(worker_id);
        drop(current_tasks);
        drop(workers_waiting_status);
        workers_condvar.wait(worker_id, self.finished.clone());

    }

    pub fn handle_async_task(
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
            let process_future = proc.processor.async_process();
            let graph = proc.graph;
            executor.async_runtime.spawn(
                query_id.as_ref().clone(),
                TrackedFuture::create(ProcessorAsyncTask::create(
                    query_id,
                    wakeup_worker_id,
                    proc.processor.clone(),
                    global_queue,
                    workers_condvar,
                    weak_executor,
                    graph,
                    process_future,
                ))
                    .in_span(Span::enter_with_local_parent(std::any::type_name::<
                        ProcessorAsyncTask,
                    >())),
            );
        }
    }

    pub fn init_sync_tasks(&self, tasks: VecDeque<ProcessorWrapper>) {
        let mut next_tasks = self.workers_tasks.next_tasks.lock();

        let mut worker_id = 0;
        for proc in tasks.into_iter() {
            next_tasks.push_task(worker_id, ExecutorTask::Sync(proc));

            worker_id += 1;
            if worker_id == next_tasks.workers_sync_tasks.len() {
                worker_id = 0;
            }
        }
    }

    pub fn completed_async_task(&self, condvar: Arc<WorkersCondvar>, task: CompletedAsyncTask) {
        let mut current_tasks = self.workers_tasks.current_tasks.lock();
        let mut workers_waiting_status = self.workers_tasks.workers_waiting_status.lock();
        let mut worker_id = task.worker_id;
        current_tasks.tasks_size += 1;
        current_tasks.workers_completed_async_tasks[worker_id].push_back(task);

        condvar.dec_active_async_worker();

        if workers_waiting_status.waiting_size() != 0 {
            if workers_waiting_status.is_waiting(worker_id) {
                workers_waiting_status
                    .wakeup_worker(worker_id);
            } else {
                worker_id = workers_waiting_status.wakeup_any_worker();
            }

            drop(current_tasks);
            drop(workers_waiting_status);
            condvar.wakeup(worker_id);
        }
    }

    pub fn get_finished_notify(&self) -> Arc<WatchNotify> {
        self.finished_notify.clone()
    }

    pub fn active_workers(&self) -> usize {
        let workers_waiting_status = self.workers_tasks.workers_waiting_status.lock();
        workers_waiting_status.total_size()
            - workers_waiting_status.waiting_size()
    }

    pub fn push_tasks_to_next_queue(&self, worker_id: usize, mut tasks: VecDeque<ExecutorTask>) {
        let mut next_tasks= self.workers_tasks.next_tasks.lock();

        while let Some(task) = tasks.pop_front() {
            next_tasks.push_task(worker_id, task);
        }
    }

    pub fn push_tasks_to_current_queue(&self, worker_id: usize, mut tasks: VecDeque<ExecutorTask>) {
        let mut next_tasks= self.workers_tasks.next_tasks.lock();

        while let Some(task) = tasks.pop_front() {
            next_tasks.push_task(worker_id, task);
        }
    }

    /// Switch the task queue between workers_tasks and next_tasks
    /// When we enter a new time slice, we need to switch them
    pub fn switch_queue(&self) -> bool{
        let mut current_tasks = self.workers_tasks.current_tasks.lock();
        let mut next_tasks = self.workers_tasks.next_tasks.lock();
        if current_tasks.is_empty() && next_tasks.is_empty(){
            return false;
        }
        std::mem::swap(&mut *current_tasks, &mut *next_tasks);
        return true;
    }

}

pub struct CompletedAsyncTask {
    pub id: NodeIndex,
    pub worker_id: usize,
    pub res: Result<()>,
    pub graph: Arc<RunningGraph>
}

impl CompletedAsyncTask {
    pub fn create(
        id: NodeIndex,
        worker_id: usize,
        res: Result<()>,
        graph: Arc<RunningGraph>
    ) -> Self {
        CompletedAsyncTask {
            id,
            worker_id,
            res,
            graph
        }
    }
}

struct ExecutorTasks {
    tasks_size: usize,
    workers_sync_tasks: Vec<VecDeque<ProcessorWrapper>>,
    workers_async_tasks: Vec<VecDeque<ProcessorWrapper>>,
    workers_completed_async_tasks: Vec<VecDeque<CompletedAsyncTask>>,
}

unsafe impl Send for ExecutorTasks {}

impl ExecutorTasks {
    pub fn create(workers_size: usize) -> ExecutorTasks {
        let mut workers_sync_tasks = Vec::with_capacity(workers_size);
        let mut workers_completed_async_tasks = Vec::with_capacity(workers_size);
        let mut workers_async_tasks = Vec::with_capacity(workers_size);
        for _index in 0..workers_size {
            workers_sync_tasks.push(VecDeque::new());
            workers_async_tasks.push(VecDeque::new());
            workers_completed_async_tasks.push(VecDeque::new());
        }

        ExecutorTasks {
            tasks_size: 0,
            workers_sync_tasks,
            workers_async_tasks,
            workers_completed_async_tasks,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.tasks_size == 0
    }

    #[inline]
    fn pop_worker_task(&mut self, worker_id: usize) -> ExecutorTask {
        if let Some(processor) = self.workers_async_tasks[worker_id].pop_front() {
            return ExecutorTask::Async(processor);
        }

        if let Some(task) = self.workers_completed_async_tasks[worker_id].pop_front() {
            return ExecutorTask::AsyncCompleted(task);
        }

        if let Some(processor) = self.workers_sync_tasks[worker_id].pop_front() {
            return ExecutorTask::Sync(processor);
        }

        ExecutorTask::None
    }

    pub fn best_worker_id(&self, mut worker_id: usize) -> usize {
        for _index in 0..self.workers_sync_tasks.len() {
            if worker_id >= self.workers_sync_tasks.len() {
                worker_id = 0;
            }

            if !self.workers_sync_tasks[worker_id].is_empty() {
                return worker_id;
            }

            if !self.workers_completed_async_tasks[worker_id].is_empty() {
                return worker_id;
            }

            if !self.workers_async_tasks[worker_id].is_empty() {
                return worker_id;
            }

            worker_id += 1;
        }

        worker_id
    }

    pub fn pop_task(&mut self, mut worker_id: usize) -> ExecutorTask {
        for _index in 0..self.workers_sync_tasks.len() {
            match self.pop_worker_task(worker_id) {
                ExecutorTask::None => {
                    worker_id += 1;
                    if worker_id >= self.workers_sync_tasks.len() {
                        worker_id = 0;
                    }
                }
                other => {
                    self.tasks_size -= 1;
                    return other;
                }
            }
        }

        ExecutorTask::None
    }

    pub fn push_task(&mut self, worker_id: usize, task: ExecutorTask) {
        self.tasks_size += 1;
        debug_assert!(
            worker_id < self.workers_sync_tasks.len(),
            "out of index, {}, {}",
            worker_id,
            self.workers_sync_tasks.len()
        );
        let sync_queue = &mut self.workers_sync_tasks[worker_id];
        let completed_queue = &mut self.workers_completed_async_tasks[worker_id];
        let async_queue = &mut self.workers_async_tasks[worker_id];
        match task {
            ExecutorTask::None => unreachable!(),
            ExecutorTask::Sync(processor) => sync_queue.push_back(processor),
            ExecutorTask::Async(processor) => async_queue.push_back(processor),
            ExecutorTask::AsyncCompleted(task) => completed_queue.push_back(task),
        }
    }
}
