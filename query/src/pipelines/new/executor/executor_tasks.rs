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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_exception::Result;
use common_infallible::Mutex;
use futures::future::BoxFuture;
use petgraph::prelude::NodeIndex;

use crate::pipelines::new::executor::executor_worker_context::ExecutorTask;
use crate::pipelines::new::executor::executor_worker_context::ExecutorWorkerContext;
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

    pub fn finish(&self) {
        self.finished.store(true, Ordering::Relaxed);
    }

    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Relaxed)
    }

    /// Pull task from the global task queue
    ///
    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    pub unsafe fn steal_task_to_context(&self, context: &mut ExecutorWorkerContext) {
        {
            let mut workers_tasks = self.workers_tasks.lock();
            if !workers_tasks.is_empty() {
                let task = workers_tasks.pop_task(context.get_worker_num());
                context.set_task(task);

                let workers_notify = context.get_workers_notify();
                if !workers_tasks.is_empty() && !workers_notify.is_empty() {
                    let worker_id = context.get_worker_num();
                    let wakeup_worker_id = workers_tasks.best_worker_id(worker_id + 1);
                    drop(workers_tasks);
                    workers_notify.wakeup(wakeup_worker_id);
                }

                return;
            }
        }

        context.get_workers_notify().wait(context.get_worker_num());
    }

    pub unsafe fn init_tasks(&self, mut tasks: VecDeque<ExecutorTask>) {
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

    #[allow(unused_assignments)]
    pub fn push_tasks(&self, ctx: &mut ExecutorWorkerContext, mut tasks: VecDeque<ExecutorTask>) {
        unsafe {
            let mut wake_worker_id = None;
            {
                let worker_id = ctx.get_worker_num();
                let mut workers_tasks = self.workers_tasks.lock();
                while let Some(task) = tasks.pop_front() {
                    workers_tasks.push_task(worker_id, task);
                }

                wake_worker_id = Some(workers_tasks.best_worker_id(worker_id + 1));
            }

            if let Some(wake_worker_id) = wake_worker_id {
                ctx.get_workers_notify().wakeup(wake_worker_id);
            }
        }
    }

    pub fn push_executing_async_task(
        &self,
        worker_id: usize,
        task: ExecutingAsyncTask,
    ) -> Option<ExecutingAsyncTask> {
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
}

pub struct ExecutingAsyncTask {
    pub id: NodeIndex,
    pub worker_id: usize,
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
                    return ExecutorTask::AsyncSchedule(
                        async_tasks.swap_remove_front(index).unwrap(),
                    );
                }
            }
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

            if !self.workers_async_tasks[worker_id].is_empty() {
                return worker_id;
            }

            if !self.workers_executing_async_tasks[worker_id].is_empty() {
                let async_tasks = &self.workers_executing_async_tasks[worker_id];

                for task_item in async_tasks {
                    if task_item.finished.load(Ordering::Relaxed) {
                        return worker_id;
                    }
                }
            }

            worker_id += 1;
        }

        worker_id
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
                other => {
                    return other;
                }
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
