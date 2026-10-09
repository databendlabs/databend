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
use std::time::SystemTime;

use databend_common_base::runtime::PerfCounters;
use databend_common_base::runtime::PerfEvent;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrackingPayloadExt;
use databend_common_base::runtime::error_info::NodeErrorType;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use fastrace::Span;
use fastrace::future::FutureExt;
use fastrace::local::LocalSpan;
use petgraph::prelude::NodeIndex;

use crate::pipelines::executor::ProcessorAsyncTask;
use crate::pipelines::executor::QueriesExecutorTasksQueue;
use crate::pipelines::executor::QueriesPipelineExecutor;
use crate::pipelines::executor::RunningGraph;
use crate::pipelines::executor::WorkersCondvar;
use crate::pipelines::executor::executor_graph::ProcessorWrapper;
use crate::pipelines::executor::executor_graph::Reschedule;
use crate::pipelines::executor::memory_limit_diagnostics::out_of_limit_error;
use crate::pipelines::executor::processor_async_task::ExecutorTasksQueue;

pub enum ExecutorTask {
    None,
    Sync(ProcessorWrapper),
    Async(ProcessorWrapper),
    AsyncCompleted(CompletedAsyncTask),
}

impl ExecutorTask {
    pub fn get_graph(&self) -> Option<Arc<RunningGraph>> {
        match self {
            ExecutorTask::None => None,
            ExecutorTask::Sync(p) => Some(p.graph().clone()),
            ExecutorTask::Async(p) => Some(p.graph().clone()),
            ExecutorTask::AsyncCompleted(p) => Some(p.graph.clone()),
        }
    }
}

pub struct CompletedAsyncTask {
    pub id: NodeIndex,
    pub worker_id: usize,
    pub res: Result<()>,
    pub graph: Arc<RunningGraph>,
    /// `None` for a wake-up through `ExecutorWaker`, or if `async_process` panicked.
    pub processor: Option<ProcessorWrapper>,
}

impl CompletedAsyncTask {
    pub fn create(
        id: NodeIndex,
        worker_id: usize,
        res: Result<()>,
        graph: Arc<RunningGraph>,
        processor: Option<ProcessorWrapper>,
    ) -> Self {
        CompletedAsyncTask {
            id,
            worker_id,
            res,
            graph,
            processor,
        }
    }
}

pub struct ExecutorWorkerContext {
    worker_id: usize,
    task: ExecutorTask,
    workers_condvar: Arc<WorkersCondvar>,
    perf_counters: Option<PerfCounters>,
}

impl ExecutorWorkerContext {
    pub fn create(worker_id: usize, workers_condvar: Arc<WorkersCondvar>) -> Self {
        ExecutorWorkerContext {
            worker_id,
            workers_condvar,
            task: ExecutorTask::None,
            perf_counters: None,
        }
    }

    /// Initialize hardware performance counters for this worker thread.
    /// Silently does nothing if perf events are unavailable (non-Linux, no permissions, etc).
    pub fn init_perf_counters(&mut self, event_groups: &[Vec<PerfEvent>]) {
        self.perf_counters = PerfCounters::try_new(event_groups);
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
        match &self.task {
            ExecutorTask::None => None,
            ExecutorTask::Sync(p) => Some((p.graph().clone(), p.node)),
            ExecutorTask::Async(p) => Some((p.graph().clone(), p.node)),
            ExecutorTask::AsyncCompleted(p) => Some((p.graph.clone(), p.id)),
        }
    }

    /// Runs the task and returns the node to schedule next, if any.
    pub fn execute_task(
        &mut self,
        executor: Option<&Arc<QueriesPipelineExecutor>>,
    ) -> std::result::Result<Option<Reschedule>, Box<NodeErrorType>> {
        match std::mem::replace(&mut self.task, ExecutorTask::None) {
            ExecutorTask::None => Err(Box::new(NodeErrorType::LocalError(ErrorCode::Internal(
                "Execute none task.",
            )))),
            ExecutorTask::Sync(processor) => match self.execute_sync_task(processor) {
                Ok(executed) => Ok(Some(Reschedule::Executed(executed))),
                Err(cause) => Err(Box::new(NodeErrorType::SyncProcessError(cause))),
            },
            ExecutorTask::Async(processor) => {
                if let Some(executor) = executor {
                    self.execute_async_task(
                        processor,
                        executor,
                        executor.global_tasks_queue.clone(),
                    );
                    Ok(None)
                } else {
                    Err(Box::new(NodeErrorType::LocalError(ErrorCode::Internal(
                        "Async task should only be executed on queries executor",
                    ))))
                }
            }
            ExecutorTask::AsyncCompleted(task) => match (task.res, task.processor) {
                (Ok(_), Some(executed)) => Ok(Some(Reschedule::Executed(executed))),
                (Ok(_), None) => Ok(Some(Reschedule::Woken {
                    node: task.id,
                    graph: task.graph,
                })),
                (Err(cause), _) => Err(Box::new(NodeErrorType::AsyncProcessError(cause))),
            },
        }
    }

    fn execute_sync_task(&mut self, mut proc: ProcessorWrapper) -> Result<ProcessorWrapper> {
        let node = proc.node;
        let payload = proc.graph().get_node_tracking_payload(node);
        let guard = ThreadTracker::tracking(payload.clone());
        let begin = SystemTime::now();
        let instant = Instant::now();

        let perf_enabled = payload.perf_enabled;
        if perf_enabled {
            if let Some(counters) = &mut self.perf_counters {
                let _ = counters.reset_and_enable();
            }
        }

        Self::process(&mut proc)?;

        if perf_enabled {
            if let Some(counters) = &mut self.perf_counters {
                if let Ok(values) = counters.disable_and_read() {
                    Profile::record_perf_counters(values);
                }
            }
        }

        let nanos = instant.elapsed().as_nanos();
        // SAFETY: an elapsed time never reaches u128::MAX nanoseconds.
        unsafe { assume(nanos < 18446744073709551615_u128) };
        Profile::record_usize_profile(ProfileStatisticsName::CpuTime, nanos as usize);
        proc.graph()
            .record_process(begin, nanos as usize / 1_000, proc.process_rows);

        if let Err(out_of_limit) = guard.flush() {
            return Err(out_of_limit_error(out_of_limit));
        }

        Ok(proc)
    }

    fn process(proc: &mut ProcessorWrapper) -> Result<()> {
        let node = proc.node;
        let span = LocalSpan::enter_with_local_parent(format!(
            "{}::process",
            proc.graph().node_name(node)
        ))
        .with_property(|| ("graph-node-id", node.index().to_string()));

        match proc.processor().process() {
            Ok(_) => Ok(()),
            Err(err) => {
                let _ = span
                    .with_property(|| ("error", "true"))
                    .with_properties(|| {
                        [
                            ("error.type", err.code().to_string()),
                            ("error.message", err.display_text()),
                        ]
                    });
                log::info!(error = err.to_string(); "Error in process");
                Err(err)
            }
        }
    }

    pub fn execute_async_task(
        &mut self,
        proc: ProcessorWrapper,
        executor: &Arc<QueriesPipelineExecutor>,
        global_queue: Arc<QueriesExecutorTasksQueue>,
    ) {
        let workers_condvar = self.workers_condvar.clone();
        workers_condvar.inc_active_async_worker();
        let query_id = proc.graph().get_query_id().clone();
        let tracking_payload = proc.graph().get_node_tracking_payload(proc.node).clone();
        let _guard = ThreadTracker::tracking(tracking_payload.clone());
        let processor_task = ProcessorAsyncTask::create(
            query_id,
            self.worker_id,
            proc,
            Arc::new(ExecutorTasksQueue::QueriesExecutorTasksQueue(global_queue)),
            workers_condvar,
        );
        executor
            .async_runtime
            .spawn(tracking_payload.tracking(processor_task).in_span(
                Span::enter_with_local_parent(std::any::type_name::<ProcessorAsyncTask>()),
            ));
    }

    pub fn get_workers_condvar(&self) -> &Arc<WorkersCondvar> {
        &self.workers_condvar
    }
}

impl Debug for ExecutorTask {
    fn fmt(&self, f: &mut Formatter) -> core::fmt::Result {
        match self {
            ExecutorTask::None => write!(f, "ExecutorTask::None"),
            ExecutorTask::Sync(p) => write!(
                f,
                "ExecutorTask::Sync {{ id: {}, name: {}}}",
                p.node.index(),
                p.graph().node_name(p.node)
            ),
            ExecutorTask::Async(p) => write!(
                f,
                "ExecutorTask::Async {{ id: {}, name: {}}}",
                p.node.index(),
                p.graph().node_name(p.node)
            ),
            ExecutorTask::AsyncCompleted(_) => write!(f, "ExecutorTask::CompletedAsync"),
        }
    }
}
