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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::base::tokio;
use databend_common_base::runtime::catch_unwind;
use databend_common_base::runtime::drop_guard;
use databend_common_base::runtime::error_info::NodeErrorType;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::Thread;
use databend_common_base::runtime::ThreadJoinHandle;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrySpawn;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline_core::ExecutionInfo;
use databend_common_pipeline_core::FinishedCallbackChain;
use databend_common_pipeline_core::LockGuard;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_core::PlanProfile;
use fastrace::func_path;
use fastrace::prelude::*;
use futures::future::select;
use futures_util::future::Either;
use log::info;
use log::warn;
use parking_lot::Mutex;
use petgraph::matrix_graph::Zero;

use crate::pipelines::executor::executor_graph::ScheduleQueue;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::ExecutorWorkerContext;
use crate::pipelines::executor::QueryExecutorTasksQueue;
use crate::pipelines::executor::RunningGraph;
use crate::pipelines::executor::WatchNotify;
use crate::pipelines::executor::WorkersCondvar;

pub type InitCallback = Box<dyn FnOnce() -> Result<()> + Send + Sync + 'static>;

pub struct QueryPipelineExecutor {
    threads_num: usize,
    pub(crate) graph: Arc<RunningGraph>,
    workers_condvar: Arc<WorkersCondvar>,
    pub async_runtime: Arc<Runtime>,
    pub global_tasks_queue: Arc<QueryExecutorTasksQueue>,
    on_init_callback: Mutex<Option<InitCallback>>,
    on_finished_chain: Mutex<FinishedCallbackChain>,
    settings: ExecutorSettings,
    finished_notify: Arc<WatchNotify>,
    finished_error: Mutex<Option<ErrorCode>>,
    #[allow(unused)]
    lock_guards: Vec<Arc<LockGuard>>,
}

impl QueryPipelineExecutor {
    pub fn create(
        mut pipeline: Pipeline,
        settings: ExecutorSettings,
    ) -> Result<Arc<QueryPipelineExecutor>> {
        let threads_num = pipeline.get_max_threads();

        if threads_num.is_zero() {
            return Err(ErrorCode::Internal(
                "Pipeline max threads cannot equals zero.",
            ));
        }

        let on_init_callback = pipeline.take_on_init();
        let mut on_finished_chain = pipeline.take_on_finished();
        let lock_guards = pipeline.take_lock_guards();

        match RunningGraph::create(pipeline, 1, settings.query_id.clone(), None) {
            Err(cause) => {
                let info = ExecutionInfo::create(Err(cause.clone()), HashMap::new());
                let _ = on_finished_chain.apply(info);

                Err(cause)
            }
            Ok(running_graph) => Self::try_create(
                running_graph,
                threads_num,
                Mutex::new(Some(on_init_callback)),
                Mutex::new(on_finished_chain),
                settings,
                lock_guards,
            ),
        }
    }

    #[fastrace::trace]
    pub fn from_pipelines(
        mut pipelines: Vec<Pipeline>,
        settings: ExecutorSettings,
    ) -> Result<Arc<QueryPipelineExecutor>> {
        if pipelines.is_empty() {
            return Err(ErrorCode::Internal("Executor Pipelines is empty."));
        }

        let threads_num = pipelines
            .iter()
            .map(|x| x.get_max_threads())
            .max()
            .unwrap_or(0);

        if threads_num.is_zero() {
            return Err(ErrorCode::Internal(
                "Pipeline max threads cannot equals zero.",
            ));
        }

        let on_init_callback = {
            let pipelines_callback = pipelines
                .iter_mut()
                .map(|x| x.take_on_init())
                .collect::<Vec<_>>();

            pipelines_callback.into_iter().reduce(|left, right| {
                Box::new(move || {
                    left()?;
                    right()
                })
            })
        };

        let mut finished_chain = FinishedCallbackChain::create();
        for pipeline in &mut pipelines {
            finished_chain.extend(pipeline.take_on_finished());
        }

        let lock_guards = pipelines
            .iter_mut()
            .flat_map(|x| x.take_lock_guards())
            .collect::<Vec<_>>();

        match RunningGraph::from_pipelines(pipelines, 1, settings.query_id.clone(), None) {
            Err(cause) => {
                let info = ExecutionInfo::create(Err(cause.clone()), HashMap::new());
                let _ignore_res = finished_chain.apply(info);
                Err(cause)
            }
            Ok(running_graph) => Self::try_create(
                running_graph,
                threads_num,
                Mutex::new(on_init_callback),
                Mutex::new(finished_chain),
                settings,
                lock_guards,
            ),
        }
    }

    fn try_create(
        graph: Arc<RunningGraph>,
        threads_num: usize,
        on_init_callback: Mutex<Option<InitCallback>>,
        on_finished_chain: Mutex<FinishedCallbackChain>,
        settings: ExecutorSettings,
        lock_guards: Vec<Arc<LockGuard>>,
    ) -> Result<Arc<QueryPipelineExecutor>> {
        let workers_condvar = WorkersCondvar::create(threads_num);
        let global_tasks_queue = QueryExecutorTasksQueue::create(threads_num);

        Ok(Arc::new(QueryPipelineExecutor {
            graph,
            threads_num,
            workers_condvar,
            global_tasks_queue,
            on_init_callback,
            on_finished_chain,
            async_runtime: GlobalIORuntime::instance(),
            settings,
            finished_error: Mutex::new(None),
            finished_notify: Arc::new(WatchNotify::new()),
            lock_guards,
        }))
    }

    fn on_finished(&self, info: ExecutionInfo) -> Result<()> {
        let mut on_finished_chain = self.on_finished_chain.lock();

        // untracking for on finished
        let tracking_payload = ThreadTracker::new_tracking_payload();
        let _guard = ThreadTracker::tracking(tracking_payload);
        on_finished_chain.apply(info)
    }

    pub fn finish<C>(&self, cause: Option<ErrorCode<C>>) {
        let cause = cause.map(|err| err.with_context("pipeline executor finished"));

        let mut finished_error = self.finished_error.lock();
        if let Some(cause) = cause {
            // We only save the cause of the first error.
            if finished_error.is_none() {
                *finished_error = Some(cause);
            }
        }

        drop(finished_error);
        self.global_tasks_queue.finish(self.workers_condvar.clone());
        self.graph.interrupt_running_nodes();
        self.finished_notify.notify_waiters();
    }

    pub fn is_finished(&self) -> bool {
        self.global_tasks_queue.is_finished()
    }

    #[fastrace::trace]
    pub fn execute(self: &Arc<Self>) -> Result<()> {
        self.init(self.graph.clone())?;

        self.start_executor_daemon()?;

        let mut thread_join_handles = self.execute_threads(self.threads_num);

        while let Some(join_handle) = thread_join_handles.pop() {
            let thread_res = join_handle.join().flatten();

            {
                let finished_error_guard = self.finished_error.lock();
                if let Some(error) = finished_error_guard.as_ref() {
                    let may_error = error.clone();
                    drop(finished_error_guard);

                    let profiling = self.fetch_plans_profile(true);
                    self.on_finished(ExecutionInfo::create(Err(may_error.clone()), profiling))?;
                    return Err(may_error);
                }
            }

            // We will ignore the abort query error, because returned by finished_error if abort query.
            if matches!(&thread_res, Err(error) if error.code() != ErrorCode::ABORTED_QUERY) {
                let may_error = thread_res.unwrap_err();
                let profiling = self.fetch_plans_profile(true);
                self.on_finished(ExecutionInfo::create(Err(may_error.clone()), profiling))?;
                return Err(may_error);
            }
        }

        if let Err(error) = self.graph.assert_finished_graph() {
            let profiling = self.fetch_plans_profile(true);
            self.on_finished(ExecutionInfo::create(Err(error.clone()), profiling))?;
            return Err(error);
        }

        let profiling = self.fetch_plans_profile(true);
        self.on_finished(ExecutionInfo::create(Ok(()), profiling))?;
        Ok(())
    }

    fn init(self: &Arc<Self>, graph: Arc<RunningGraph>) -> Result<()> {
        unsafe {
            // TODO: the on init callback cannot be killed.
            {
                let instant = Instant::now();
                let mut guard = self.on_init_callback.lock();
                if let Some(callback) = guard.take() {
                    drop(guard);

                    // untracking for on finished
                    let tracking_payload = ThreadTracker::new_tracking_payload();
                    if let Err(cause) = Result::flatten(catch_unwind(move || {
                        let _guard = ThreadTracker::tracking(tracking_payload);

                        callback()
                    })) {
                        return Err(cause.add_message_back("(while in query pipeline init)"));
                    }
                }

                info!(
                    "Init pipeline successfully, query_id: {:?}, elapsed: {:?}",
                    self.settings.query_id,
                    instant.elapsed()
                );
            }

            let mut init_schedule_queue = graph.init_schedule_queue(self.threads_num)?;

            let mut wakeup_worker_id = 0;
            while let Some(proc) = init_schedule_queue.async_queue.pop_front() {
                ScheduleQueue::schedule_async_task(
                    proc.clone(),
                    self.settings.query_id.clone(),
                    self,
                    wakeup_worker_id,
                    self.workers_condvar.clone(),
                    self.global_tasks_queue.clone(),
                );
                wakeup_worker_id += 1;

                if wakeup_worker_id == self.threads_num {
                    wakeup_worker_id = 0;
                }
            }

            let sync_queue = std::mem::take(&mut init_schedule_queue.sync_queue);
            self.global_tasks_queue.init_sync_tasks(sync_queue);
            Ok(())
        }
    }

    fn start_executor_daemon(self: &Arc<Self>) -> Result<()> {
        if !self.settings.max_execute_time_in_seconds.is_zero() {
            // NOTE(wake ref): When runtime scheduling is blocked, holding executor strong ref may cause the executor can not stop.
            let this = Arc::downgrade(self);
            let max_execute_time_in_seconds = self.settings.max_execute_time_in_seconds;
            let finished_notify = self.finished_notify.clone();
            self.async_runtime.spawn(async move {
                let finished_future = Box::pin(finished_notify.notified());
                let max_execute_future = Box::pin(tokio::time::sleep(max_execute_time_in_seconds));
                if let Either::Left(_) = select(max_execute_future, finished_future).await {
                    if let Some(executor) = this.upgrade() {
                        executor.finish(Some(ErrorCode::AbortedQuery(
                            "Aborted query, because the execution time exceeds the maximum execution time limit",
                        )));
                    }
                }
            });
        }

        Ok(())
    }

    fn execute_threads(self: &Arc<Self>, threads: usize) -> Vec<ThreadJoinHandle<Result<()>>> {
        let mut thread_join_handles = Vec::with_capacity(threads);

        for thread_num in 0..threads {
            let this = self.clone();
            #[allow(unused_mut)]
            let mut name = format!("PipelineExecutor-{}", thread_num);

            #[cfg(debug_assertions)]
            {
                // We need to pass the thread name in the unit test, because the thread name is the test name
                if matches!(std::env::var("UNIT_TEST"), Ok(var_value) if var_value == "TRUE") {
                    if let Some(cur_thread_name) = std::thread::current().name() {
                        name = cur_thread_name.to_string();
                    }
                }
            }

            let span = Span::enter_with_local_parent(func_path!())
                .with_property(|| ("thread_name", name.clone()));
            thread_join_handles.push(Thread::named_spawn(Some(name), move || unsafe {
                let _g = span.set_local_parent();
                let this_clone = this.clone();
                let try_result = catch_unwind(|| this_clone.execute_single_thread(thread_num));

                // finish the pipeline executor when has error or panic
                if let Err(cause) = try_result.flatten() {
                    this.finish(Some(cause));
                }

                Ok(())
            }));
        }
        thread_join_handles
    }

    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    pub unsafe fn execute_single_thread(self: &Arc<Self>, thread_num: usize) -> Result<()> {
        let workers_condvar = self.workers_condvar.clone();
        let mut context = ExecutorWorkerContext::create(thread_num, workers_condvar);

        while !self.global_tasks_queue.is_finished() {
            // When there are not enough tasks, the thread will be blocked, so we need loop check.
            while !self.global_tasks_queue.is_finished() && !context.has_task() {
                self.global_tasks_queue.steal_task_to_context(&mut context);
            }

            while !self.global_tasks_queue.is_finished() && context.has_task() {
                let task_info = context.get_task_info();
                let execute_res = context.execute_task(None);
                match execute_res {
                    Ok(Some((executed_pid, graph))) => {
                        // Not scheduled graph if pipeline is finished.
                        if !self.global_tasks_queue.is_finished() {
                            // We immediately schedule the processor again.
                            let schedule_queue_res = graph.clone().schedule_queue(executed_pid);
                            match schedule_queue_res {
                                Ok(schedule_queue) => {
                                    schedule_queue.schedule(
                                        &self.global_tasks_queue,
                                        &mut context,
                                        self,
                                    );
                                }
                                Err(cause) => {
                                    graph.record_node_error(
                                        executed_pid,
                                        NodeErrorType::ScheduleEventError(cause.clone()),
                                    );
                                    graph.should_finish(Err(cause.clone()))?;
                                    return Err(cause);
                                }
                            }
                        }
                    }
                    Err(error_type) => {
                        let cause = error_type.get_error_code();
                        if let Some((graph, node_index)) = task_info {
                            graph.record_node_error(node_index, *error_type);
                            graph.should_finish(Err(cause.clone()))?;
                        }
                        return Err(cause);
                    }
                    _ => {}
                };
            }
        }

        Ok(())
    }

    pub fn format_graph_nodes(&self) -> String {
        self.graph.format_graph_nodes()
    }

    pub fn fetch_plans_profile(&self, collect_metrics: bool) -> HashMap<u32, PlanProfile> {
        match collect_metrics {
            true => self
                .graph
                .fetch_profiling(Some(self.settings.executor_node_id.clone())),
            false => self.graph.fetch_profiling(None),
        }
    }
}

impl Drop for QueryPipelineExecutor {
    fn drop(&mut self) {
        drop_guard(move || {
            self.finish::<()>(None);

            let cause = match self.finished_error.lock().as_ref() {
                Some(cause) => cause.clone(),
                None => ErrorCode::Internal("Pipeline illegal state: not successfully shutdown."),
            };

            let mut on_finished_chain = self.on_finished_chain.lock();

            // untracking for on finished
            let tracking_payload = ThreadTracker::new_tracking_payload();
            let _guard = ThreadTracker::tracking(tracking_payload);
            let profiling = self.fetch_plans_profile(true);
            let info = ExecutionInfo::create(Err(cause), profiling);
            if let Err(cause) = on_finished_chain.apply(info) {
                warn!("Pipeline executor shutdown failure, {:?}", cause);
            }
        })
    }
}
