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

use std::fmt::Debug;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use common_arrow::arrow_format::ipc::flatbuffers::bitflags::_core::fmt::Formatter;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::task::ArcWake;
use petgraph::prelude::NodeIndex;

use crate::pipelines::new::executor::executor_notify::WorkersNotify;
use crate::pipelines::new::executor::executor_tasks::ExecutingAsyncTask;
use crate::pipelines::new::executor::executor_tasks::ExecutorTasksQueue;
use crate::pipelines::new::processors::processor::ProcessorPtr;

pub enum ExecutorTask {
    None,
    Sync(ProcessorPtr),
    Async(ProcessorPtr),
    AsyncSchedule(ExecutingAsyncTask),
}

pub struct ExecutorWorkerContext {
    worker_num: usize,
    task: ExecutorTask,
    workers_notify: Arc<WorkersNotify>,
}

impl ExecutorWorkerContext {
    pub fn create(worker_num: usize, workers_notify: Arc<WorkersNotify>) -> Self {
        ExecutorWorkerContext {
            worker_num,
            workers_notify,
            task: ExecutorTask::None,
        }
    }

    pub fn has_task(&self) -> bool {
        !matches!(&self.task, ExecutorTask::None)
    }

    pub fn get_worker_num(&self) -> usize {
        self.worker_num
    }

    pub fn set_task(&mut self, task: ExecutorTask) {
        self.task = task
    }

    pub unsafe fn execute_task(&mut self, queue: &ExecutorTasksQueue) -> Result<NodeIndex> {
        // println!("{} execute task {:?}", std::thread::current().name().unwrap(), self.task);
        match std::mem::replace(&mut self.task, ExecutorTask::None) {
            ExecutorTask::None => Err(ErrorCode::LogicalError("Execute none task.")),
            ExecutorTask::Sync(processor) => self.execute_sync_task(processor),
            ExecutorTask::Async(processor) => self.execute_async_task(processor, queue),
            ExecutorTask::AsyncSchedule(boxed_future) => {
                self.schedule_async_task(boxed_future, queue)
            }
        }
    }

    unsafe fn execute_sync_task(&mut self, processor: ProcessorPtr) -> Result<NodeIndex> {
        processor.process()?;
        Ok(processor.id())
    }

    unsafe fn execute_async_task(
        &mut self,
        processor: ProcessorPtr,
        queue: &ExecutorTasksQueue,
    ) -> Result<NodeIndex> {
        let id = processor.id();
        let worker_id = self.worker_num;
        let finished = Arc::new(AtomicBool::new(false));
        let future = processor.async_process();
        self.schedule_async_task(
            ExecutingAsyncTask {
                id,
                finished,
                future,
                worker_id,
            },
            queue,
        )
    }

    unsafe fn schedule_async_task(
        &mut self,
        mut task: ExecutingAsyncTask,
        queue: &ExecutorTasksQueue,
    ) -> Result<NodeIndex> {
        task.finished.store(false, Ordering::Relaxed);

        let id = task.id;
        loop {
            let workers_notify = self.get_workers_notify().clone();
            let waker =
                ExecutingAsyncTaskWaker::create(&task.finished, task.worker_id, workers_notify);

            let waker = futures::task::waker_ref(&waker);
            let mut cx = Context::from_waker(&waker);

            match task.future.as_mut().poll(&mut cx) {
                Poll::Ready(Ok(_)) => {
                    return Ok(id);
                }
                Poll::Ready(Err(cause)) => {
                    return Err(cause);
                }
                Poll::Pending => {
                    match queue.push_executing_async_task(task.worker_id, task) {
                        None => {
                            return Ok(id);
                        }
                        Some(t) => {
                            task = t;
                        }
                    };
                }
            };
        }
    }

    pub fn get_workers_notify(&self) -> &Arc<WorkersNotify> {
        &self.workers_notify
    }
}

struct ExecutingAsyncTaskWaker(usize, Arc<AtomicBool>, Arc<WorkersNotify>);

impl ExecutingAsyncTaskWaker {
    pub fn create(
        flag: &Arc<AtomicBool>,
        worker_id: usize,
        workers_notify: Arc<WorkersNotify>,
    ) -> Arc<ExecutingAsyncTaskWaker> {
        Arc::new(ExecutingAsyncTaskWaker(
            worker_id,
            flag.clone(),
            workers_notify,
        ))
    }
}

impl ArcWake for ExecutingAsyncTaskWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.1.store(true, Ordering::Release);
        arc_self.2.wakeup(arc_self.0);
    }
}

impl Debug for ExecutorTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        unsafe {
            match self {
                ExecutorTask::None => write!(f, "ExecutorTask::None"),
                ExecutorTask::Sync(p) => write!(
                    f,
                    "ExecutorTask::Sync {{ id: {}, name: {}}}",
                    p.id().index(),
                    p.name()
                ),
                ExecutorTask::Async(p) => write!(
                    f,
                    "ExecutorTask::Async {{ id: {}, name: {}}}",
                    p.id().index(),
                    p.name()
                ),
                ExecutorTask::AsyncSchedule(t) => {
                    write!(f, "ExecutorTask::AsyncSchedule {{ id: {}}}", t.id.index())
                }
            }
        }
    }
}
