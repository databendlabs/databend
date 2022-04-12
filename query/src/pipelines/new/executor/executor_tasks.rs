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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_exception::Result;
use crossbeam_queue::SegQueue;
use petgraph::prelude::NodeIndex;

use crate::pipelines::new::executor::executor_worker_context::ExecutorTask;
use crate::pipelines::new::executor::executor_worker_context::ExecutorWorkerContext;
use crate::pipelines::new::processors::processor::ProcessorPtr;

pub struct ExecutorTasksQueue {
    finished: AtomicBool,
    workers_tasks: ExecutorTasks,
}

impl ExecutorTasksQueue {
    pub fn create(workers_size: usize) -> Arc<ExecutorTasksQueue> {
        Arc::new(ExecutorTasksQueue {
            finished: AtomicBool::new(false),
            workers_tasks: ExecutorTasks::create(workers_size),
        })
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
            if !self.workers_tasks.is_empty() {
                let task = self.workers_tasks.pop_task(context.get_worker_num());
                context.set_task(task);

                let workers_notify = context.get_workers_notify();
                if !self.workers_tasks.is_empty() && !workers_notify.is_empty() {
                    let worker_id = context.get_worker_num();
                    let wakeup_worker_id = self.workers_tasks.best_worker_id(worker_id + 1);
                    workers_notify.wakeup(wakeup_worker_id);
                }

                return;
            }
        }

        // When tasks queue is empty and all workers are waiting, no new tasks will be generated.
        let workers_notify = context.get_workers_notify();
        if workers_notify.active_workers() <= 1 {
            self.finish();
            workers_notify.wakeup_all();
            return;
        }

        context.get_workers_notify().wait(context.get_worker_num());
    }

    pub unsafe fn init_tasks(&self, mut tasks: VecDeque<ExecutorTask>) {
        let mut worker_id = 0;
        while let Some(task) = tasks.pop_front() {
            self.workers_tasks.push_task(worker_id, task);

            worker_id += 1;
            if worker_id == self.workers_tasks.workers_sync_tasks.len() {
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
                while let Some(task) = tasks.pop_front() {
                    self.workers_tasks.push_task(worker_id, task);
                }

                wake_worker_id = Some(self.workers_tasks.best_worker_id(worker_id + 1));
            }

            if let Some(wake_worker_id) = wake_worker_id {
                ctx.get_workers_notify().wakeup(wake_worker_id);
            }
        }
    }

    pub fn completed_async_task(&self, task: CompletedAsyncTask) {
        let worker_id = task.worker_id;
        self.workers_tasks.tasks_size.fetch_add(1, Ordering::SeqCst);
        self.workers_tasks.workers_completed_async_tasks[worker_id].push(task);
    }
}

pub struct CompletedAsyncTask {
    pub id: NodeIndex,
    pub worker_id: usize,
    pub res: Result<()>,
}

impl CompletedAsyncTask {
    pub fn create(proc: ProcessorPtr, worker_id: usize, res: Result<()>) -> Self {
        CompletedAsyncTask {
            id: unsafe { proc.id() },
            worker_id,
            res,
        }
    }
}

struct ExecutorTasks {
    tasks_size: AtomicUsize,
    workers_sync_tasks: Vec<SegQueue<ProcessorPtr>>,
    workers_async_tasks: Vec<SegQueue<ProcessorPtr>>,
    workers_completed_async_tasks: Vec<SegQueue<CompletedAsyncTask>>,
}

unsafe impl Send for ExecutorTasks {}

impl ExecutorTasks {
    pub fn create(workers_size: usize) -> ExecutorTasks {
        let mut workers_sync_tasks = Vec::with_capacity(workers_size);
        let mut workers_async_tasks = Vec::with_capacity(workers_size);
        let mut workers_completed_async_tasks = Vec::with_capacity(workers_size);

        for _index in 0..workers_size {
            workers_sync_tasks.push(SegQueue::new());
            workers_async_tasks.push(SegQueue::new());
            workers_completed_async_tasks.push(SegQueue::new());
        }

        ExecutorTasks {
            tasks_size: AtomicUsize::new(0),
            workers_sync_tasks,
            workers_async_tasks,
            workers_completed_async_tasks,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.tasks_size.load(Ordering::Relaxed) == 0
    }

    #[inline]
    fn pop_worker_task(&self, worker_id: usize) -> ExecutorTask {
        if let Some(processor) = self.workers_sync_tasks[worker_id].pop() {
            return ExecutorTask::Sync(processor);
        }

        if let Some(task) = self.workers_completed_async_tasks[worker_id].pop() {
            return ExecutorTask::AsyncCompleted(task);
        }

        if let Some(processor) = self.workers_async_tasks[worker_id].pop() {
            return ExecutorTask::Async(processor);
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

            if !self.workers_completed_async_tasks[worker_id].is_empty() {
                return worker_id;
            }

            worker_id += 1;
        }

        worker_id
    }

    pub unsafe fn pop_task(&self, mut worker_id: usize) -> ExecutorTask {
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

    pub unsafe fn push_task(&self, worker_id: usize, task: ExecutorTask) {
        self.tasks_size.fetch_add(1, Ordering::SeqCst);
        let sync_queue = &self.workers_sync_tasks[worker_id];
        let async_queue = &self.workers_async_tasks[worker_id];
        let completed_queue = &self.workers_completed_async_tasks[worker_id];

        match task {
            ExecutorTask::None => unreachable!(),
            ExecutorTask::Sync(processor) => sync_queue.push(processor),
            ExecutorTask::Async(processor) => async_queue.push(processor),
            ExecutorTask::AsyncCompleted(task) => completed_queue.push(task),
        }
    }
}
