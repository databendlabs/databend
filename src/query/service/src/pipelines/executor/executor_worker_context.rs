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

use std::collections::BTreeMap;
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
use log::warn;
use petgraph::prelude::NodeIndex;

use crate::pipelines::executor::PlanNodeMemoryUsage;
use crate::pipelines::executor::ProcessorAsyncTask;
use crate::pipelines::executor::QueriesExecutorTasksQueue;
use crate::pipelines::executor::QueriesPipelineExecutor;
use crate::pipelines::executor::RunningGraph;
use crate::pipelines::executor::WorkersCondvar;
use crate::pipelines::executor::executor_graph::ProcessorWrapper;
use crate::pipelines::executor::processor_async_task::ExecutorTasksQueue;
use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::sessions::SessionManager;

pub(super) const TOP_MEMORY_PLAN_NODE_LIMIT: usize = 10;

pub(super) fn out_of_limit_error(error: impl Debug) -> ErrorCode {
    ErrorCode::MemoryExceedsLimit(format!("{error:?}"))
}

pub(super) fn log_memory_limit_diagnostics<C>(cause: &ErrorCode<C>, message: &str) {
    if cause.code() != ErrorCode::MEMORY_EXCEEDS_LIMIT {
        return;
    }

    let mut top_memory_plan_nodes_by_query =
        SessionManager::instance().get_queries_top_memory_plan_nodes(TOP_MEMORY_PLAN_NODE_LIMIT);
    top_memory_plan_nodes_by_query.extend(
        DataExchangeManager::instance()
            .get_queries_top_memory_plan_nodes(TOP_MEMORY_PLAN_NODE_LIMIT),
    );
    let top_memory_plan_nodes_by_query = format_top_memory_plan_nodes_by_query(
        top_memory_plan_nodes_by_query,
        TOP_MEMORY_PLAN_NODE_LIMIT,
    );

    warn!(top_memory_plan_nodes_by_query = top_memory_plan_nodes_by_query; "{message}");
}

fn format_top_memory_plan_nodes_by_query(
    top_memory_plan_nodes_by_query: Vec<(String, Vec<PlanNodeMemoryUsage>)>,
    limit: usize,
) -> String {
    let mut grouped = BTreeMap::<String, Vec<PlanNodeMemoryUsage>>::new();
    for (query_id, plan_nodes) in top_memory_plan_nodes_by_query {
        grouped.entry(query_id).or_default().extend(plan_nodes);
    }

    let mut query_memory = Vec::new();
    let mut top_plan_nodes = Vec::new();
    for (query_id, mut plan_nodes) in grouped {
        let total_current_bytes = plan_nodes
            .iter()
            .map(|plan_node| plan_node.current_bytes)
            .sum::<usize>();
        query_memory.push(format!("({query_id:?}, {total_current_bytes})"));

        plan_nodes.sort_by(|left, right| {
            right
                .peak_bytes
                .cmp(&left.peak_bytes)
                .then(right.current_bytes.cmp(&left.current_bytes))
                .then(left.identity.cmp(&right.identity))
        });
        plan_nodes.truncate(limit);

        let plan_nodes = plan_nodes
            .into_iter()
            .map(|plan_node| {
                format!(
                    "({:?}, {}, {})",
                    plan_node.identity, plan_node.current_bytes, plan_node.peak_bytes
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        top_plan_nodes.push(format!("query_id: {query_id} [{plan_nodes}]"));
    }

    format!(
        "query_memory: [{}]; top_plan_nodes: {}",
        query_memory.join(", "),
        top_plan_nodes.join("; ")
    )
}

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
    ) -> std::result::Result<Option<(NodeIndex, Arc<RunningGraph>)>, Box<NodeErrorType>> {
        unsafe {
            match std::mem::replace(&mut self.task, ExecutorTask::None) {
                ExecutorTask::None => Err(Box::new(NodeErrorType::LocalError(
                    ErrorCode::Internal("Execute none task."),
                ))),
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
    }

    /// # Safety
    unsafe fn execute_sync_task(
        &mut self,
        proc: ProcessorWrapper,
    ) -> Result<Option<(NodeIndex, Arc<RunningGraph>)>> {
        unsafe {
            let payload = proc.graph.get_node_tracking_payload(proc.processor.id());
            let guard = ThreadTracker::tracking(payload.clone());
            let begin = SystemTime::now();
            let instant = Instant::now();

            let perf_enabled = payload.perf_enabled;
            if perf_enabled {
                if let Some(counters) = &mut self.perf_counters {
                    let _ = counters.reset_and_enable();
                }
            }

            proc.processor.process()?;

            if perf_enabled {
                if let Some(counters) = &mut self.perf_counters {
                    if let Ok(values) = counters.disable_and_read() {
                        Profile::record_perf_counters(values);
                    }
                }
            }

            let nanos = instant.elapsed().as_nanos();
            assume(nanos < 18446744073709551615_u128);
            Profile::record_usize_profile(ProfileStatisticsName::CpuTime, nanos as usize);
            let process_rows = proc.process_rows;
            proc.graph
                .record_process(begin, nanos as usize / 1_000, process_rows);

            if let Err(out_of_limit) = guard.flush() {
                return Err(out_of_limit_error(out_of_limit));
            }

            Ok(Some((proc.processor.id(), proc.graph)))
        }
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
            let tracking_payload = graph.get_node_tracking_payload(node_index).clone();
            let _guard = ThreadTracker::tracking(tracking_payload.clone());
            let processor_task = ProcessorAsyncTask::create(
                query_id,
                wakeup_worker_id,
                proc.processor.clone(),
                Arc::new(ExecutorTasksQueue::QueriesExecutorTasksQueue(global_queue)),
                workers_condvar,
                graph,
                process_future,
            );
            executor.async_runtime.spawn(
                tracking_payload.clone().tracking(processor_task).in_span(
                    Span::enter_with_local_parent(std::any::type_name::<ProcessorAsyncTask>()),
                ),
            );
        }
        Ok(None)
    }

    pub fn get_workers_condvar(&self) -> &Arc<WorkersCondvar> {
        &self.workers_condvar
    }
}

impl Debug for ExecutorTask {
    fn fmt(&self, f: &mut Formatter) -> core::fmt::Result {
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

#[cfg(test)]
mod tests {
    use databend_common_base::runtime::OutOfLimit;
    use databend_common_exception::ErrorCode;

    use super::format_top_memory_plan_nodes_by_query;
    use super::out_of_limit_error;
    use crate::pipelines::executor::PlanNodeMemoryUsage;

    #[test]
    fn test_out_of_limit_error_uses_memory_exceeds_limit() {
        let err = out_of_limit_error(OutOfLimit::new(100, 50));

        assert_eq!(err.code(), ErrorCode::MEMORY_EXCEEDS_LIMIT);
        assert_eq!(err.name(), "MemoryExceedsLimit");
        assert!(err.message().contains("memory usage"));
        assert!(err.message().contains("exceeds limit"));
    }

    #[test]
    fn test_format_top_memory_plan_nodes_by_query_groups_query_id() {
        let output = format_top_memory_plan_nodes_by_query(
            vec![
                ("query-b".to_string(), vec![PlanNodeMemoryUsage {
                    identity: "AggregateFinal [#2]".to_string(),
                    current_bytes: 10,
                    peak_bytes: 100,
                }]),
                ("query-a".to_string(), vec![PlanNodeMemoryUsage {
                    identity: "TableScan [#1]".to_string(),
                    current_bytes: 20,
                    peak_bytes: 80,
                }]),
                ("query-b".to_string(), vec![PlanNodeMemoryUsage {
                    identity: "AggregatePartial [#3]".to_string(),
                    current_bytes: 30,
                    peak_bytes: 120,
                }]),
            ],
            10,
        );

        assert_eq!(
            output,
            "query_memory: [(\"query-a\", 20), (\"query-b\", 40)]; top_plan_nodes: query_id: query-a [(\"TableScan [#1]\", 20, 80)]; query_id: query-b [(\"AggregatePartial [#3]\", 30, 120), (\"AggregateFinal [#2]\", 10, 100)]"
        );
    }
}
