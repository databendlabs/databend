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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline_core::processors::Profile;
use databend_common_pipeline_core::processors::ProfileStatisticsName;
use petgraph::prelude::NodeIndex;

use crate::pipelines::executor::{CompletedAsyncTask, PipelineExecutor};
use crate::pipelines::executor::executor_graph::ProcessorWrapper;
use crate::pipelines::executor::RunningGraph;
use crate::pipelines::executor::WorkersCondvar;
use crate::pipelines::processors::ProcessorPtr;

pub enum ExecutorTask {
    None,
    Sync(ProcessorWrapper),
    AsyncCompleted(CompletedAsyncTask),
}

pub struct ExecutorWorkerContext {
    pub query_id: Arc<String>,
    worker_id: usize,
    task: ExecutorTask,
    workers_condvar: Arc<WorkersCondvar>,
}

impl ExecutorWorkerContext {
    pub fn create(
        worker_id: usize,
        workers_condvar: Arc<WorkersCondvar>,
        query_id: Arc<String>,
    ) -> Self {
        ExecutorWorkerContext {
            query_id,
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

    /// # Safety
    pub unsafe fn execute_task(&mut self, executor: &Arc<PipelineExecutor>) -> Result<Option<(NodeIndex, Arc<RunningGraph>)>> {
        match std::mem::replace(&mut self.task, ExecutorTask::None) {
            ExecutorTask::None => Err(ErrorCode::Internal("Execute none task.")),
            ExecutorTask::Sync(processor) => self.execute_sync_task(processor),
            ExecutorTask::AsyncCompleted(task) => match task.res {
                Ok(_) => Ok(Some((task.id, task.graph))),
                Err(cause) => Err(cause),
            },
        }
    }

    /// # Safety
    unsafe fn execute_sync_task(
        &mut self,
        proc: ProcessorWrapper,
    ) -> Result<Option<(NodeIndex, Arc<RunningGraph>)>> {
        Profile::track_profile(proc.graph.get_node_profile(proc.processor.id()));

        let instant = Instant::now();

        proc.processor.process()?;

        let nanos = instant.elapsed().as_nanos();
        assume(nanos < 18446744073709551615_u128);
        Profile::record_usize_profile(ProfileStatisticsName::CpuTime, nanos as usize);
        Ok(Some((proc.processor.id(), proc.graph)))
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
                ExecutorTask::AsyncCompleted(_) => write!(f, "ExecutorTask::CompletedAsync"),
            }
        }
    }
}
