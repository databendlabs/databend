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

use std::collections::VecDeque;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::base::tokio;
use databend_common_base::runtime::catch_unwind;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::Thread;
use databend_common_base::runtime::ThreadJoinHandle;
use databend_common_base::runtime::TrySpawn;
use databend_common_base::GLOBAL_TASK;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline_core::LockGuard;
use databend_common_pipeline_core::Pipeline;
use futures::future::select;
use futures_util::future::Either;
use log::info;
use log::warn;
use log::LevelFilter;
use minitrace::full_name;
use minitrace::prelude::*;
use parking_lot::Mutex;
use petgraph::matrix_graph::Zero;
use tokio::time;

use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::ExecutorTask;
use crate::pipelines::executor::ExecutorWorkerContext;
use crate::pipelines::executor::QueriesExecutorTasksQueue;
use crate::pipelines::executor::RunningGraph;
use crate::pipelines::executor::WatchNotify;
use crate::pipelines::executor::WorkersCondvar;

pub type InitCallback = Box<dyn FnOnce() -> Result<()> + Send + Sync + 'static>;

pub type FinishedCallback =
    Box<dyn FnOnce(&Result<Vec<Arc<Profile>>, ErrorCode>) -> Result<()> + Send + Sync + 'static>;

pub struct QueriesPipelineExecutor {
    threads_num: usize,
    workers_condvar: Arc<WorkersCondvar>,
    pub async_runtime: Arc<Runtime>,
    pub global_tasks_queue: Arc<QueriesExecutorTasksQueue>,
    on_init_callback: Mutex<Option<InitCallback>>,
    on_finished_callback: Mutex<Option<FinishedCallback>>,
    settings: ExecutorSettings,
    finished_notify: Arc<WatchNotify>,
    finished_error: Mutex<Option<ErrorCode>>,
    #[allow(unused)]
    lock_guards: Vec<LockGuard>,
    pub epoch: AtomicU32,

    // TODO: will remove it after refactoring Executor into a 1:n pattern
    pub graph: Arc<RunningGraph>,
}

impl QueriesPipelineExecutor {
    pub fn create(
        mut pipeline: Pipeline,
        settings: ExecutorSettings,
    ) -> Result<Arc<QueriesPipelineExecutor>> {
        let threads_num = pipeline.get_max_threads();

        if threads_num.is_zero() {
            return Err(ErrorCode::Internal(
                "Pipeline max threads cannot equals zero.",
            ));
        }

        let on_init_callback = pipeline.take_on_init();
        let on_finished_callback = pipeline.take_on_finished();
        let lock_guards = pipeline.take_lock_guards();

        match RunningGraph::create(pipeline, 1) {
            Err(cause) => {
                let _ = on_finished_callback(&Err(cause.clone()));
                Err(cause)
            }
            Ok(running_graph) => Self::try_create(
                running_graph,
                threads_num,
                Mutex::new(Some(on_init_callback)),
                Mutex::new(Some(on_finished_callback)),
                settings,
                lock_guards,
            ),
        }
    }

    #[minitrace::trace]
    pub fn from_pipelines(
        mut pipelines: Vec<Pipeline>,
        settings: ExecutorSettings,
    ) -> Result<Arc<QueriesPipelineExecutor>> {
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

        let on_finished_callback = {
            let pipelines_callback = pipelines
                .iter_mut()
                .map(|x| x.take_on_finished())
                .collect::<Vec<_>>();

            pipelines_callback.into_iter().reduce(|left, right| {
                Box::new(move |arg| {
                    left(arg)?;
                    right(arg)
                })
            })
        };

        let lock_guards = pipelines
            .iter_mut()
            .flat_map(|x| x.take_lock_guards())
            .collect::<Vec<_>>();

        match RunningGraph::from_pipelines(pipelines, 1) {
            Err(cause) => {
                if let Some(on_finished_callback) = on_finished_callback {
                    let _ = on_finished_callback(&Err(cause.clone()));
                }

                Err(cause)
            }
            Ok(running_graph) => Self::try_create(
                running_graph,
                threads_num,
                Mutex::new(on_init_callback),
                Mutex::new(on_finished_callback),
                settings,
                lock_guards,
            ),
        }
    }

    fn try_create(
        graph: Arc<RunningGraph>,
        threads_num: usize,
        on_init_callback: Mutex<Option<InitCallback>>,
        on_finished_callback: Mutex<Option<FinishedCallback>>,
        settings: ExecutorSettings,
        lock_guards: Vec<LockGuard>,
    ) -> Result<Arc<QueriesPipelineExecutor>> {
        let workers_condvar = WorkersCondvar::create(threads_num);
        let global_tasks_queue = QueriesExecutorTasksQueue::create(threads_num);

        Ok(Arc::new(QueriesPipelineExecutor {
            graph,
            threads_num,
            workers_condvar,
            global_tasks_queue,
            on_init_callback,
            on_finished_callback,
            async_runtime: GlobalIORuntime::instance(),
            settings,
            finished_error: Mutex::new(None),
            finished_notify: Arc::new(WatchNotify::new()),
            lock_guards,
            epoch: AtomicU32::new(0),
        }))
    }

    fn on_finished(&self, error: &Result<Vec<Arc<Profile>>, ErrorCode>) -> Result<()> {
        let mut guard = self.on_finished_callback.lock();
        if let Some(on_finished_callback) = guard.take() {
            drop(guard);
            catch_unwind(move || on_finished_callback(error))??;
        }
        Ok(())
    }

    pub fn finish(&self, cause: Option<ErrorCode>) {
        if let Some(cause) = cause {
            let mut finished_error = self.finished_error.lock();

            // We only save the cause of the first error.
            if finished_error.is_none() {
                *finished_error = Some(cause);
            }
        }

        self.global_tasks_queue.finish(self.workers_condvar.clone());
        self.graph.interrupt_running_nodes();
        self.finished_notify.notify_waiters();
    }

    pub fn is_finished(&self) -> bool {
        self.global_tasks_queue.is_finished()
    }

    #[minitrace::trace]
    pub fn execute(self: &Arc<Self>) -> Result<()> {
        // TODO: will remove this in the future
        self.init(self.graph.clone())?;

        self.start_time_limit_daemon()?;

        let mut thread_join_handles = self.execute_threads(self.threads_num);

        while let Some(join_handle) = thread_join_handles.pop() {
            let thread_res = join_handle.join().flatten();

            {
                let finished_error_guard = self.finished_error.lock();
                if let Some(error) = finished_error_guard.as_ref() {
                    let may_error = error.clone();
                    drop(finished_error_guard);

                    self.on_finished(&Err(may_error.clone()))?;
                    return Err(may_error);
                }
            }

            // We will ignore the abort query error, because returned by finished_error if abort query.
            if matches!(&thread_res, Err(error) if error.code() != ErrorCode::ABORTED_QUERY) {
                let may_error = thread_res.unwrap_err();
                self.on_finished(&Err(may_error.clone()))?;
                return Err(may_error);
            }
        }

        if let Err(error) = self.graph.assert_finished_graph() {
            self.on_finished(&Err(error.clone()))?;
            return Err(error);
        }

        self.on_finished(&Ok(self.graph.get_proc_profiles()))?;
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
                    if let Err(cause) = Result::flatten(catch_unwind(callback)) {
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
                let mut tasks = VecDeque::with_capacity(1);
                tasks.push_back(ExecutorTask::Async(proc));
                self.global_tasks_queue
                    .push_tasks(wakeup_worker_id, None, Some(tasks));

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

    /// Used to abort the query when the execution time exceeds the maximum execution time limit
    fn start_time_limit_daemon(self: &Arc<Self>) -> Result<()> {
        if !self.settings.max_execute_time_in_seconds.is_zero() {
            // NOTE(wake ref): When runtime scheduling is blocked, holding executor strong ref may cause the executor can not stop.
            let this = Arc::downgrade(self);
            let max_execute_time_in_seconds = self.settings.max_execute_time_in_seconds;
            let finished_notify = self.finished_notify.clone();
            self.async_runtime.spawn(GLOBAL_TASK, async move {
                let finished_future = Box::pin(finished_notify.notified());
                let max_execute_future = Box::pin(time::sleep(max_execute_time_in_seconds));

                // This waits for either of two futures to complete:
                // 1. The 'finished_future', which gets triggered when an external event signals that the task is finished.
                // 2. The 'max_execute_future', which gets triggered when the maximum execution time as set in 'max_execute_time_in_seconds' elapses.
                // When either future completes, the executor is finished.
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

            let span = Span::enter_with_local_parent(full_name!())
                .with_property(|| ("thread_name", name.clone()));
            thread_join_handles.push(Thread::named_spawn(Some(name), move || unsafe {
                let _g = span.set_local_parent();
                let this_clone = this.clone();
                let try_result = catch_unwind(move || -> Result<()> {
                    match this_clone.execute_single_thread(thread_num) {
                        Ok(_) => Ok(()),
                        Err(cause) => {
                            if log::max_level() == LevelFilter::Trace {
                                Err(cause.add_message_back(format!(
                                    " (while in processor thread {})",
                                    thread_num
                                )))
                            } else {
                                Err(cause)
                            }
                        }
                    }
                });

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
        let mut context = ExecutorWorkerContext::create(
            thread_num,
            workers_condvar,
            self.settings.query_id.clone(),
        );

        while !self.global_tasks_queue.is_finished() {
            // When there are not enough tasks, the thread will be blocked, so we need loop check.
            while !self.global_tasks_queue.is_finished() && !context.has_task() {
                self.global_tasks_queue
                    .steal_task_to_context(&mut context, self);
            }

            while !self.global_tasks_queue.is_finished() && context.has_task() {
                if let Some((executed_pid, graph)) = context.execute_task()? {
                    // Not scheduled graph if pipeline is finished.
                    if !self.global_tasks_queue.is_finished() {
                        // We immediately schedule the processor again.
                        let schedule_queue = graph.schedule_queue(executed_pid)?;
                        schedule_queue.schedule_with_condition(
                            &self.global_tasks_queue,
                            &mut context,
                            self,
                        );
                    }
                }
            }
        }

        Ok(())
    }

    #[inline]
    pub(crate) fn increase_global_epoch(&self) {
        self.epoch.fetch_add(1, Ordering::SeqCst);
    }

    pub fn format_graph_nodes(&self) -> String {
        self.graph.format_graph_nodes()
    }

    pub fn get_profiles(&self) -> Vec<Arc<Profile>> {
        self.graph.get_proc_profiles()
    }
}

impl Drop for QueriesPipelineExecutor {
    fn drop(&mut self) {
        self.finish(None);

        let mut guard = self.on_finished_callback.lock();
        if let Some(on_finished_callback) = guard.take() {
            drop(guard);
            let cause = match self.finished_error.lock().as_ref() {
                Some(cause) => cause.clone(),
                None => ErrorCode::Internal("Pipeline illegal state: not successfully shutdown."),
            };
            if let Err(cause) = catch_unwind(move || on_finished_callback(&Err(cause))).flatten() {
                warn!("Pipeline executor shutdown failure, {:?}", cause);
            }
        }
    }
}
