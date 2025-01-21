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
use std::sync::Barrier;
use std::time::Instant;

use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::catch_unwind;
use databend_common_base::runtime::defer;
use databend_common_base::runtime::error_info::NodeErrorType;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::Thread;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrySpawn;
use databend_common_base::JoinHandle;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline_core::ExecutionInfo;
use databend_common_pipeline_core::FinishedCallbackChain;
use databend_common_pipeline_core::LockGuard;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_core::PlanProfile;
use parking_lot::Mutex;
use petgraph::prelude::NodeIndex;
use tokio::sync::watch::Receiver;

use crate::pipelines::executor::executor_graph::ScheduleQueue;
use crate::pipelines::executor::pipeline_executor::InitCallback;
use crate::pipelines::executor::pipeline_executor::NewPipelineExecutor;
use crate::pipelines::executor::pipeline_executor::QueryHandle;
use crate::pipelines::executor::pipeline_executor::QueryTask;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::ExecutorWorkerContext;
use crate::pipelines::executor::QueryExecutorTasksQueue;
use crate::pipelines::executor::RunningGraph;
use crate::pipelines::executor::WorkersCondvar;

pub struct NewQueryPipelineExecutor {
    tx: async_channel::Sender<QueryTask>,
}

impl NewQueryPipelineExecutor {
    pub fn create() -> Arc<dyn NewPipelineExecutor> {
        let (tx, rx) = async_channel::bounded(4);

        GlobalIORuntime::instance().spawn(async move {
            let background = QueryPipelineExecutorBackground::create(rx);
            background.work_loop().await
        });

        Arc::new(NewQueryPipelineExecutor { tx })
    }

    pub fn init(_config: &InnerConfig) -> Result<()> {
        let new_query_pipeline_executor = NewQueryPipelineExecutor::create();
        GlobalInstance::set(new_query_pipeline_executor);
        Ok(())
    }
}

#[async_trait::async_trait]
impl NewPipelineExecutor for NewQueryPipelineExecutor {
    async fn submit(
        &self,
        pipelines: Vec<Pipeline>,
        settings: ExecutorSettings,
    ) -> Result<Arc<dyn QueryHandle>> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let tracking_payload = ThreadTracker::new_tracking_payload();
        let query_task = QueryTask::try_create(pipelines, tx, settings, tracking_payload)?;

        if let Err(_cause) = self.tx.send(query_task).await {
            return Err(ErrorCode::Internal(""));
        }

        match rx.await {
            Ok(submit_res) => submit_res,
            Err(_cause) => Err(ErrorCode::Internal("Broken query task")),
        }
    }
}

pub struct QueryPipelineExecutorBackground {
    rx: async_channel::Receiver<QueryTask>,
}

impl QueryPipelineExecutorBackground {
    pub fn create(rx: async_channel::Receiver<QueryTask>) -> Self {
        QueryPipelineExecutorBackground { rx }
    }

    pub async fn work_loop(&self) {
        while let Ok(msg) = self.rx.recv().await {
            let tracking_payload = msg.tracking_payload.clone();
            let _tracking_payload_guard = ThreadTracker::tracking(tracking_payload);

            Self::recv_query_task(msg);
        }

        log::info!("QueryPipelineExecutor background shutdown.");
    }

    fn recv_query_task(mut msg: QueryTask) {
        let instant = Instant::now();
        let query_id = msg.settings.query_id.clone();

        let _timer_guard = defer(move || {
            log::info!(
                "schedule query({:?}) successfully. elapsed {:?}",
                query_id,
                instant.elapsed()
            );
        });

        let thread_num = msg.max_threads_num;
        let tx = msg.tx.take().unwrap();

        let (finish_tx, finish_rx) = tokio::sync::watch::channel(None);
        let query_handle = QueryPipelineHandle::create(msg, finish_rx);

        if let Err(_send_error) = tx.send(Ok(query_handle.clone())) {
            log::warn!(
                "Ignore query {:?} in executor, because query may killed.",
                &query_handle.settings.query_id
            );
            return;
        }

        let finish_tx = finish_tx.clone();
        let threads_barrier = Arc::new(Barrier::new(thread_num));
        for idx in 0..thread_num {
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

            let finish_tx = finish_tx.clone();
            let threads_barrier = threads_barrier.clone();
            let query_handle = query_handle.clone();
            Thread::named_spawn(Some(name), move || unsafe {
                let _exit_guard = defer({
                    let query_handle = query_handle.clone();

                    move || {
                        if !threads_barrier.wait().is_leader() {
                            return;
                        }

                        if let Err(_cause) = finish_tx.send(Some(query_handle.on_finish())) {
                            log::warn!("");
                        }
                    }
                });

                if let Err(cause) = query_handle.run_query_worker(idx) {
                    // We will ignore the abort query error, because returned by finished_error if abort query.
                    if cause.code() == ErrorCode::ABORTED_QUERY {
                        return;
                    }

                    query_handle.finish(Some(cause.clone()));
                }
            });
        }
    }
}

pub struct QueryPipelineHandle {
    graph: Arc<RunningGraph>,
    #[allow(dead_code)]
    query_holds: Vec<Arc<LockGuard>>,
    workers_condvar: Arc<WorkersCondvar>,
    global_tasks_queue: Arc<QueryExecutorTasksQueue>,
    async_runtime: Arc<Runtime>,
    settings: ExecutorSettings,

    finished_error: Mutex<Option<ErrorCode>>,
    daemon_handle: Mutex<Option<JoinHandle<()>>>,
    on_init_callback: Mutex<Option<InitCallback>>,
    on_finished_callback: Mutex<FinishedCallbackChain>,
    finished_notify: Receiver<Option<Result<()>>>,
}

impl QueryPipelineHandle {
    pub fn create(task: QueryTask, finished_notify: Receiver<Option<Result<()>>>) -> Arc<Self> {
        Arc::new(QueryPipelineHandle {
            finished_notify,
            graph: task.graph,
            query_holds: task.holds,
            workers_condvar: WorkersCondvar::create(task.max_threads_num),

            global_tasks_queue: QueryExecutorTasksQueue::create(task.max_threads_num),
            async_runtime: GlobalIORuntime::instance(),
            settings: task.settings,
            daemon_handle: Mutex::new(None),
            finished_error: Mutex::new(None),
            on_init_callback: Mutex::new(Some(task.on_init_callback)),
            on_finished_callback: Mutex::new(task.on_finished_callback),
        })
    }

    unsafe fn init_schedule(self: &Arc<Self>) -> Result<()> {
        let mut on_init_callback = self.on_init_callback.lock();

        if let Some(on_init_callback) = on_init_callback.take() {
            let instant = Instant::now();
            let query_id = self.settings.query_id.clone();
            let _timer_guard = defer(move || {
                log::info!(
                    "Init pipeline successfully, query_id: {:?}, elapsed: {:?}",
                    query_id,
                    instant.elapsed()
                );
            });

            // untracking for on finished
            let mut tracking_payload = ThreadTracker::new_tracking_payload();
            if let Some(mem_stat) = &tracking_payload.mem_stat {
                tracking_payload.mem_stat = Some(MemStat::create_child(
                    String::from("Pipeline-on-finished"),
                    mem_stat.get_parent_memory_stat(),
                ));
            }

            if let Err(cause) = Result::flatten(catch_unwind(move || {
                let _guard = ThreadTracker::tracking(tracking_payload);

                on_init_callback()
            })) {
                return Err(cause.add_message_back("(while in query pipeline init)"));
            }

            let threads_num = self.settings.max_threads as usize;
            let mut init_schedule_queue = self.graph.init_schedule_queue(threads_num)?;

            let mut wakeup_worker_id = 0;
            while let Some(proc) = init_schedule_queue.async_queue.pop_front() {
                ScheduleQueue::schedule_async_task(
                    proc.clone(),
                    self.settings.query_id.clone(),
                    &self.async_runtime,
                    wakeup_worker_id,
                    self.workers_condvar.clone(),
                    self.global_tasks_queue.clone(),
                );
                wakeup_worker_id += 1;

                if wakeup_worker_id == threads_num {
                    wakeup_worker_id = 0;
                }
            }

            let sync_queue = std::mem::take(&mut init_schedule_queue.sync_queue);
            self.global_tasks_queue.init_sync_tasks(sync_queue);
            self.start_executor_daemon()?;
        }

        Ok(())
    }

    fn start_executor_daemon(self: &Arc<Self>) -> Result<()> {
        if !self.settings.max_execute_time_in_seconds.is_zero() {
            // NOTE(wake ref): When runtime scheduling is blocked, holding executor strong ref may cause the executor can not stop.
            let this = Arc::downgrade(self);
            let max_execute_time_in_seconds = self.settings.max_execute_time_in_seconds;

            self.async_runtime.spawn(async move {
                let _ = tokio::time::sleep(max_execute_time_in_seconds).await;
                if let Some(executor) = this.upgrade() {
                    executor.finish(Some(ErrorCode::AbortedQuery(
                        "Aborted query, because the execution time exceeds the maximum execution time limit",
                    )));
                }
            });
        }

        Ok(())
    }

    pub unsafe fn run_query_worker(self: &Arc<Self>, worker_num: usize) -> Result<()> {
        self.init_schedule()?;

        let mut node_index = NodeIndex::new(0);
        let workers_condvar = self.workers_condvar.clone();
        let mut context = ExecutorWorkerContext::create(worker_num, workers_condvar);

        let execute_result = catch_unwind({
            let node_index = &mut node_index;
            move || {
                while !self.global_tasks_queue.is_finished() {
                    // When there are not enough tasks, the thread will be blocked, so we need loop check.
                    while !self.global_tasks_queue.is_finished() && !context.has_task() {
                        self.global_tasks_queue.steal_task_to_context(&mut context);
                    }

                    while !self.global_tasks_queue.is_finished() && context.has_task() {
                        *node_index = context.get_task_pid();
                        let executed_pid = context.execute_task_new()?;

                        if self.global_tasks_queue.is_finished() {
                            break;
                        }

                        *node_index = executed_pid;
                        let runtime = &self.async_runtime;
                        let schedule_queue = self.graph.schedule_queue(executed_pid)?;
                        schedule_queue.schedule(&self.global_tasks_queue, &mut context, runtime);
                    }
                }

                Ok(())
            }
        });

        if let Err(cause) = execute_result.flatten() {
            let record_error = NodeErrorType::LocalError(cause.clone());
            self.graph.record_node_error(node_index, record_error);
            self.graph.should_finish(Err(cause.clone()))?;
            return Err(cause);
        }

        Ok(())
    }

    fn apply_finished_chain(&self, info: ExecutionInfo) -> Result<()> {
        let mut on_finished_chain = self.on_finished_callback.lock();

        // untracking for on finished
        let mut tracking_payload = ThreadTracker::new_tracking_payload();
        if let Some(mem_stat) = &tracking_payload.mem_stat {
            tracking_payload.mem_stat = Some(MemStat::create_child(
                String::from("Pipeline-on-finished"),
                mem_stat.get_parent_memory_stat(),
            ));
        }

        let _guard = ThreadTracker::tracking(tracking_payload);
        on_finished_chain.apply(info)
    }

    fn on_finish(&self) -> Result<()> {
        {
            let finished_error_guard = self.finished_error.lock();
            if let Some(error) = finished_error_guard.as_ref() {
                let may_error = error.clone();
                drop(finished_error_guard);

                let profiling = self.fetch_profiling(true);
                self.apply_finished_chain(ExecutionInfo::create(
                    Err(may_error.clone()),
                    profiling,
                ))?;
                return Err(may_error);
            }
        }

        if let Err(error) = self.graph.assert_finished_graph() {
            let profiling = self.fetch_profiling(true);

            self.apply_finished_chain(ExecutionInfo::create(Err(error.clone()), profiling))?;
            return Err(error);
        }

        let profiling = self.fetch_profiling(true);
        self.apply_finished_chain(ExecutionInfo::create(Ok(()), profiling))?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl QueryHandle for QueryPipelineHandle {
    async fn wait(&self) -> Result<()> {
        let mut finished_notify = self.finished_notify.clone();

        let x = match finished_notify.wait_for(Option::is_some).await {
            Err(_cause) => Err(ErrorCode::Internal("")),
            Ok(res) => res.as_ref().unwrap().clone(),
        };
        x
    }

    fn is_finished(&self) -> bool {
        self.global_tasks_queue.is_finished()
    }

    fn finish(&self, cause: Option<ErrorCode<()>>) {
        if let Some(cause) = cause {
            let mut finished_error = self.finished_error.lock();

            if finished_error.is_none() {
                *finished_error = Some(cause);
            }
        }

        self.global_tasks_queue.finish(self.workers_condvar.clone());
        self.graph.interrupt_running_nodes();

        if let Some(daemon_handle) = { self.daemon_handle.lock().take() } {
            daemon_handle.abort();
        }
    }

    fn fetch_profiling(&self, collect_metrics: bool) -> HashMap<u32, PlanProfile> {
        match collect_metrics {
            true => self
                .graph
                .fetch_profiling(Some(self.settings.executor_node_id.clone())),
            false => self.graph.fetch_profiling(None),
        }
    }
}
