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
use std::fmt::Formatter;
use std::sync::Arc;

use common_base::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;
use petgraph::prelude::NodeIndex;

use crate::pipelines::new::executor::executor_notify::WorkersNotify;
use crate::pipelines::new::executor::executor_tasks::CompletedAsyncTask;
use crate::pipelines::new::executor::PipelineExecutor;
use crate::pipelines::new::processors::processor::ProcessorPtr;

pub enum ExecutorTask {
    None,
    Sync(ProcessorPtr),
    Async(ProcessorPtr),
    // AsyncSchedule(ExecutingAsyncTask),
    AsyncCompleted(CompletedAsyncTask),
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

    pub fn take_task(&mut self) -> ExecutorTask {
        std::mem::replace(&mut self.task, ExecutorTask::None)
    }

    pub unsafe fn execute_task(&mut self, exec: &PipelineExecutor) -> Result<Option<NodeIndex>> {
        match std::mem::replace(&mut self.task, ExecutorTask::None) {
            ExecutorTask::None => Err(ErrorCode::LogicalError("Execute none task.")),
            ExecutorTask::Sync(processor) => self.execute_sync_task(processor),
            ExecutorTask::Async(processor) => self.execute_async_task(processor, exec),
            ExecutorTask::AsyncCompleted(task) => Ok(Some(task.id)),
        }
    }

    unsafe fn execute_sync_task(&mut self, processor: ProcessorPtr) -> Result<Option<NodeIndex>> {
        processor.process()?;
        Ok(Some(processor.id()))
    }

    unsafe fn execute_async_task(
        &mut self,
        processor: ProcessorPtr,
        executor: &PipelineExecutor,
    ) -> Result<Option<NodeIndex>> {
        let worker_id = self.worker_num;
        let tasks_queue = executor.global_tasks_queue.clone();
        executor.async_runtime.spawn(async move {
            let res = processor.async_process().await;
            let task = CompletedAsyncTask::create(processor, worker_id, res);
            tasks_queue.completed_async_task(task);
        });

        Ok(None)
    }

    pub fn get_workers_notify(&self) -> &Arc<WorkersNotify> {
        &self.workers_notify
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
                ExecutorTask::AsyncCompleted(_) => write!(f, "ExecutorTask::CompletedAsync"),
            }
        }
    }
}
