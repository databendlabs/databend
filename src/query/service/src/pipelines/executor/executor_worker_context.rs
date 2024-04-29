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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::intrinsics::assume;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::runtime::error_info::NodeErrorType;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrySpawn;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use minitrace::future::FutureExt;
use minitrace::Span;
use petgraph::prelude::NodeIndex;

use crate::pipelines::executor::executor_graph::ProcessorWrapper;
use crate::pipelines::executor::processor_async_task::ExecutorTasksQueue;
use crate::pipelines::executor::ProcessorAsyncTask;
use crate::pipelines::executor::QueriesExecutorTasksQueue;
use crate::pipelines::executor::QueriesPipelineExecutor;
use crate::pipelines::executor::RunningGraph;
use crate::pipelines::executor::WorkersCondvar;

pub enum ExecutorTask {
    None,
    Sync(ProcessorWrapper),
    Async(ProcessorWrapper),
    AsyncCompleted(CompletedAsyncTask),
}

impl ExecutorTask{
    pub fn get_graph(&self) -> Option<Arc<RunningGraph>> {
            match self {
                ExecutorTask::None => None,
                ExecutorTask::Sync(p) => Some(p.graph.clone()),
                ExecutorTask::Async(p) => Some(p.graph.clone()),
                ExecutorTask::AsyncCompleted(p) => Some(p.graph.clone()),
            }

    }
}

pub struct CompletedAsyncTask {
    pub id: NodeIndex,
    pub worker_id: usize,
    pub res: Result<()>,
    pub graph: Arc<RunningGraph>,
}

impl CompletedAsyncTask {
    pub fn create(
        id: NodeIndex,
        worker_id: usize,
        res: Result<()>,
        graph: Arc<RunningGraph>,
    ) -> Self {
        CompletedAsyncTask {
            id,
            worker_id,
            res,
            graph,
        }
    }
}

pub struct ExecutorWorkerContext {
    worker_id: usize,
    task: ExecutorTask,
    workers_condvar: Arc<WorkersCondvar>,
}

impl ExecutorWorkerContext {
    pub fn create(worker_id: usize, workers_condvar: Arc<WorkersCondvar>) -> Self {
        ExecutorWorkerContext {
            worker_id,
            workers_condvar,
            task: ExecutorTask::None,
        }
    }

    pub fn has_task(&self) -> bool {
        !matches!(&self.task, ExecutorTask::None)
    }

    pub fn get_worker_id(&self) -> usize {
        self.worker_id
    }

    pub fn set_task(&mut self, task: ExecutorTask) {
        self.task = task
    }

    pub fn take_task(&mut self) -> ExecutorTask {
        std::mem::replace(&mut self.task, ExecutorTask::None)
    }

    pub fn get_task_info(&self) -> Option<(Arc<RunningGraph>, NodeIndex)> {
        unsafe {
            match &self.task {
                ExecutorTask::None => None,
                ExecutorTask::Sync(p) => Some((p.graph.clone(), p.processor.id())),
                ExecutorTask::Async(p) => Some((p.graph.clone(), p.processor.id())),
                ExecutorTask::AsyncCompleted(p) => Some((p.graph.clone(), p.id)),
            }
        }
    }

    /// # Safety
    pub unsafe fn execute_task(
        &mut self,
        executor: Option<&Arc<QueriesPipelineExecutor>>,
    ) -> Result<Option<(NodeIndex, Arc<RunningGraph>)>, Box<NodeErrorType>> {
        match std::mem::replace(&mut self.task, ExecutorTask::None) {
            ExecutorTask::None => Err(Box::new(NodeErrorType::LocalError(ErrorCode::Internal(
                "Execute none task.",
            )))),
            ExecutorTask::Sync(processor) => match self.execute_sync_task(processor) {
                Ok(res) => Ok(res),
                Err(cause) => Err(Box::new(NodeErrorType::SyncProcessError(cause))),
            },
            ExecutorTask::Async(processor) => {
                if let Some(executor) = executor {
                    match self.execute_async_task(
                        processor,
                        executor,
                        executor.global_tasks_queue.clone(),
                    ) {
                        Ok(res) => Ok(res),
                        Err(cause) => Err(Box::new(NodeErrorType::AsyncProcessError(cause))),
                    }
                } else {
                    Err(Box::new(NodeErrorType::LocalError(ErrorCode::Internal(
                        "Async task should only be executed on queries executor",
                    ))))
                }
            }
            ExecutorTask::AsyncCompleted(task) => match task.res {
                Ok(_) => Ok(Some((task.id, task.graph))),
                Err(cause) => Err(Box::new(NodeErrorType::AsyncProcessError(cause))),
            },
        }
    }

    /// # Safety
    unsafe fn execute_sync_task(
        &mut self,
        proc: ProcessorWrapper,
    ) -> Result<Option<(NodeIndex, Arc<RunningGraph>)>> {
        let payload = proc.graph.get_node_tracking_payload(proc.processor.id());
        let _guard = ThreadTracker::tracking(payload.clone());

        let instant = Instant::now();

        proc.processor.process()?;
        let nanos = instant.elapsed().as_nanos();
        assume(nanos < 18446744073709551615_u128);
        Profile::record_usize_profile(ProfileStatisticsName::CpuTime, nanos as usize);
        Ok(Some((proc.processor.id(), proc.graph)))
    }

    pub fn execute_async_task(
        &mut self,
        proc: ProcessorWrapper,
        executor: &Arc<QueriesPipelineExecutor>,
        global_queue: Arc<QueriesExecutorTasksQueue>,
    ) -> Result<Option<(NodeIndex, Arc<RunningGraph>)>> {
        unsafe {
            let workers_condvar = self.workers_condvar.clone();
            workers_condvar.inc_active_async_worker();
            let query_id = proc.graph.get_query_id().clone();
            let wakeup_worker_id = self.worker_id;
            let process_future = proc.processor.async_process();
            let graph = proc.graph;
            let node_index = proc.processor.id();
            let tracking_payload = graph.get_node_tracking_payload(node_index);
            let _guard = ThreadTracker::tracking(tracking_payload.clone());
            executor.async_runtime.spawn(
                query_id.as_ref().clone(),
                ProcessorAsyncTask::create(
                    query_id,
                    wakeup_worker_id,
                    proc.processor.clone(),
                    Arc::new(ExecutorTasksQueue::QueriesExecutorTasksQueue(global_queue)),
                    workers_condvar,
                    graph,
                    process_future,
                )
                .in_span(Span::enter_with_local_parent(std::any::type_name::<
                    ProcessorAsyncTask,
                >())),
            );
        }
        Ok(None)
    }

    pub fn get_workers_condvar(&self) -> &Arc<WorkersCondvar> {
        &self.workers_condvar
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
                    p.processor.id().index(),
                    p.processor.name()
                ),
                ExecutorTask::Async(p) => write!(
                    f,
                    "ExecutorTask::Async {{ id: {}, name: {}}}",
                    p.processor.id().index(),
                    p.processor.name()
                ),
                ExecutorTask::AsyncCompleted(_) => write!(f, "ExecutorTask::CompletedAsync"),
            }
        }
    }
}
