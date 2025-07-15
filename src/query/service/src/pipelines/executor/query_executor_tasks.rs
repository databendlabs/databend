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

use parking_lot::Mutex;

use crate::pipelines::executor::executor_graph::ProcessorWrapper;
use crate::pipelines::executor::executor_worker_context::CompletedAsyncTask;
use crate::pipelines::executor::ExecutorTask;
use crate::pipelines::executor::ExecutorWorkerContext;
use crate::pipelines::executor::WatchNotify;
use crate::pipelines::executor::WorkersCondvar;
use crate::pipelines::executor::WorkersWaitingStatus;

pub struct QueryExecutorTasksQueue {
    finished: Arc<AtomicBool>,
    finished_notify: Arc<WatchNotify>,
    workers_tasks: Mutex<ExecutorTasks>,
}

impl QueryExecutorTasksQueue {
    pub fn create(workers_size: usize) -> Arc<QueryExecutorTasksQueue> {
        Arc::new(QueryExecutorTasksQueue {
            finished: Arc::new(AtomicBool::new(false)),
            finished_notify: Arc::new(WatchNotify::new()),
            workers_tasks: Mutex::new(ExecutorTasks::create(workers_size)),
        })
    }

    pub fn finish(&self, workers_condvar: Arc<WorkersCondvar>) {
        self.finished.store(true, Ordering::SeqCst);
        self.finished_notify.notify_waiters();

        let mut workers_tasks = self.workers_tasks.lock();
        let mut wakeup_workers =
            Vec::with_capacity(workers_tasks.workers_waiting_status.waiting_size());

        while workers_tasks.workers_waiting_status.waiting_size() != 0 {
            let worker_id = workers_tasks.workers_waiting_status.wakeup_any_worker();
            wakeup_workers.push(worker_id);
        }

        drop(workers_tasks);
        for wakeup_worker in wakeup_workers {
            workers_condvar.wakeup(wakeup_worker);
        }
    }

    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::SeqCst)
    }

    /// Pull task from the global task queue
    /// Method is thread unsafe and require thread safe call
    pub fn steal_task_to_context(&self, context: &mut ExecutorWorkerContext) {
        let mut workers_tasks = self.workers_tasks.lock();

        if !workers_tasks.is_empty() {
            let task = workers_tasks.pop_task(context.get_worker_id());

            context.set_task(task);

            let workers_condvar = context.get_workers_condvar();

            if !workers_tasks.is_empty() && workers_tasks.workers_waiting_status.waiting_size() != 0
            {
                let worker_id = context.get_worker_id();
                let mut wakeup_worker_id = workers_tasks.best_worker_id(worker_id + 1);

                if workers_tasks
                    .workers_waiting_status
                    .is_waiting(wakeup_worker_id)
                {
                    workers_tasks
                        .workers_waiting_status
                        .wakeup_worker(wakeup_worker_id);
                } else {
                    wakeup_worker_id = workers_tasks.workers_waiting_status.wakeup_any_worker();
                }

                drop(workers_tasks);
                workers_condvar.wakeup(wakeup_worker_id);
            }

            return;
        }

        // When tasks queue is empty and all workers are waiting, no new tasks will be generated.
        let workers_condvar = context.get_workers_condvar();
        if !workers_condvar.has_waiting_async_task()
            && workers_tasks.workers_waiting_status.is_last_active_worker()
        {
            drop(workers_tasks);
            self.finish(workers_condvar.clone());
            return;
        }

        let worker_id = context.get_worker_id();
        workers_tasks.workers_waiting_status.wait_worker(worker_id);
        drop(workers_tasks);
        workers_condvar.wait(worker_id, self.finished.clone());
    }

    pub fn init_sync_tasks(&self, tasks: VecDeque<ProcessorWrapper>) {
        let mut workers_tasks = self.workers_tasks.lock();

        let mut worker_id = 0;
        for proc in tasks.into_iter() {
            workers_tasks.push_task(worker_id, ExecutorTask::Sync(proc));

            worker_id += 1;
            if worker_id == workers_tasks.workers_sync_tasks.len() {
                worker_id = 0;
            }
        }
    }

    #[allow(unused_assignments)]
    pub fn push_tasks(&self, ctx: &mut ExecutorWorkerContext, mut tasks: VecDeque<ExecutorTask>) {
        let mut workers_tasks = self.workers_tasks.lock();

        let worker_id = ctx.get_worker_id();
        while let Some(task) = tasks.pop_front() {
            workers_tasks.push_task(worker_id, task);
        }

        if workers_tasks.workers_waiting_status.waiting_size() != 0 {
            let mut wake_worker_id = workers_tasks.best_worker_id(worker_id + 1);

            if workers_tasks
                .workers_waiting_status
                .is_waiting(wake_worker_id)
            {
                workers_tasks
                    .workers_waiting_status
                    .wakeup_worker(wake_worker_id);
            } else {
                wake_worker_id = workers_tasks.workers_waiting_status.wakeup_any_worker();
            }

            drop(workers_tasks);
            ctx.get_workers_condvar().wakeup(wake_worker_id);
        }
    }

    pub fn completed_async_task(&self, condvar: Arc<WorkersCondvar>, task: CompletedAsyncTask) {
        let mut workers_tasks = self.workers_tasks.lock();

        let mut worker_id = task.worker_id;
        workers_tasks.tasks_size += 1;
        workers_tasks.workers_completed_async_tasks[worker_id].push_back(task);

        condvar.dec_active_async_worker();

        if workers_tasks.workers_waiting_status.waiting_size() != 0 {
            if workers_tasks.workers_waiting_status.is_waiting(worker_id) {
                workers_tasks
                    .workers_waiting_status
                    .wakeup_worker(worker_id);
            } else {
                worker_id = workers_tasks.workers_waiting_status.wakeup_any_worker();
            }

            drop(workers_tasks);
            condvar.wakeup(worker_id);
        }
    }

    pub fn get_finished_notify(&self) -> Arc<WatchNotify> {
        self.finished_notify.clone()
    }

    pub fn active_workers(&self) -> usize {
        let workers_tasks = self.workers_tasks.lock();
        workers_tasks.workers_waiting_status.total_size()
            - workers_tasks.workers_waiting_status.waiting_size()
    }
}

struct ExecutorTasks {
    tasks_size: usize,
    workers_waiting_status: WorkersWaitingStatus,
    workers_sync_tasks: Vec<VecDeque<ProcessorWrapper>>,
    workers_completed_async_tasks: Vec<VecDeque<CompletedAsyncTask>>,
}

unsafe impl Send for ExecutorTasks {}

impl ExecutorTasks {
    pub fn create(workers_size: usize) -> ExecutorTasks {
        let mut workers_sync_tasks = Vec::with_capacity(workers_size);
        let mut workers_completed_async_tasks = Vec::with_capacity(workers_size);

        for _index in 0..workers_size {
            workers_sync_tasks.push(VecDeque::new());
            workers_completed_async_tasks.push(VecDeque::new());
        }

        ExecutorTasks {
            tasks_size: 0,
            workers_sync_tasks,
            workers_completed_async_tasks,
            workers_waiting_status: WorkersWaitingStatus::create(workers_size),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.tasks_size == 0
    }

    #[inline]
    fn pop_worker_task(&mut self, worker_id: usize) -> ExecutorTask {
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

        match task {
            ExecutorTask::None => unreachable!(),
            ExecutorTask::Sync(processor) => sync_queue.push_back(processor),
            ExecutorTask::Async(_) => unreachable!("used for new executor"),
            ExecutorTask::AsyncCompleted(task) => completed_queue.push_back(task),
        }
    }
}
