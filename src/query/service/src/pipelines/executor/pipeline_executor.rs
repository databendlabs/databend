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
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::JoinHandle;

use common_base::base::tokio;
use common_base::base::tokio::sync::Notify;
use common_base::base::Runtime;
use common_base::base::Thread;
use common_base::base::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::future::select;
use futures_util::future::Either;
use tracing::warn;

use crate::pipelines::executor::executor_condvar::WorkersCondvar;
use crate::pipelines::executor::executor_graph::RunningGraph;
use crate::pipelines::executor::executor_tasks::ExecutorTasksQueue;
use crate::pipelines::executor::executor_worker_context::ExecutorWorkerContext;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::pipeline::Pipeline;

pub type FinishedCallback =
    Arc<Box<dyn Fn(&Option<ErrorCode>) -> Result<()> + Send + Sync + 'static>>;

pub struct PipelineExecutor {
    threads_num: usize,
    graph: RunningGraph,
    workers_condvar: Arc<WorkersCondvar>,
    query_need_abort: Arc<AtomicBool>,
    pub async_runtime: Arc<Runtime>,
    pub global_tasks_queue: Arc<ExecutorTasksQueue>,
    on_finished_callback: FinishedCallback,
    settings: ExecutorSettings,
    finished_notify: Notify,
    execute_timeout: Arc<AtomicBool>,
}

impl PipelineExecutor {
    pub fn create(
        async_rt: Arc<Runtime>,
        query_need_abort: Arc<AtomicBool>,
        mut pipeline: Pipeline,
        settings: ExecutorSettings,
    ) -> Result<Arc<PipelineExecutor>> {
        let threads_num = pipeline.get_max_threads();
        let on_finished_callback = pipeline.take_on_finished();

        assert_ne!(threads_num, 0, "Pipeline max threads cannot equals zero.");
        Self::try_create(
            async_rt,
            query_need_abort,
            RunningGraph::create(pipeline)?,
            threads_num,
            on_finished_callback,
            settings,
        )
    }

    pub fn from_pipelines(
        async_rt: Arc<Runtime>,
        query_need_abort: Arc<AtomicBool>,
        mut pipelines: Vec<Pipeline>,
        settings: ExecutorSettings,
    ) -> Result<Arc<PipelineExecutor>> {
        if pipelines.is_empty() {
            return Err(ErrorCode::LogicalError("Executor Pipelines is empty."));
        }

        let threads_num = pipelines
            .iter()
            .map(|x| x.get_max_threads())
            .max()
            .unwrap_or(0);

        let on_finished_callbacks = pipelines
            .iter_mut()
            .map(|x| x.take_on_finished())
            .collect::<Vec<_>>();

        assert_ne!(threads_num, 0, "Pipeline max threads cannot equals zero.");
        Self::try_create(
            async_rt,
            query_need_abort,
            RunningGraph::from_pipelines(pipelines)?,
            threads_num,
            Arc::new(Box::new(move |may_error| {
                for on_finished_callback in &on_finished_callbacks {
                    on_finished_callback(may_error)?;
                }

                Ok(())
            })),
            settings,
        )
    }

    fn try_create(
        async_rt: Arc<Runtime>,
        query_need_abort: Arc<AtomicBool>,
        graph: RunningGraph,
        threads_num: usize,
        on_finished_callback: FinishedCallback,
        settings: ExecutorSettings,
    ) -> Result<Arc<PipelineExecutor>> {
        unsafe {
            let workers_condvar = WorkersCondvar::create(threads_num);
            let global_tasks_queue = ExecutorTasksQueue::create(threads_num);

            let mut init_schedule_queue = graph.init_schedule_queue()?;

            let mut tasks = VecDeque::new();
            while let Some(task) = init_schedule_queue.pop_task() {
                tasks.push_back(task);
            }
            global_tasks_queue.init_tasks(tasks);

            Ok(Arc::new(PipelineExecutor {
                graph,
                threads_num,
                workers_condvar,
                query_need_abort,
                global_tasks_queue,
                on_finished_callback,
                async_runtime: async_rt,
                settings,
                finished_notify: Notify::new(),
                execute_timeout: Arc::new(AtomicBool::new(false)),
            }))
        }
    }

    pub fn finish(&self) -> Result<()> {
        self.global_tasks_queue.finish(self.workers_condvar.clone());
        self.finished_notify.notify_waiters();
        Ok(())
    }

    pub fn is_finished(&self) -> bool {
        self.global_tasks_queue.is_finished()
    }

    pub fn execute(self: &Arc<Self>) -> Result<()> {
        self.start_executor_daemon()?;

        let mut thread_join_handles = self.execute_threads(self.threads_num);

        while let Some(join_handle) = thread_join_handles.pop() {
            // flatten error.
            let join_res = match join_handle.join() {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(cause)) => Err(cause),
                Err(cause) => match cause.downcast_ref::<&'static str>() {
                    None => match cause.downcast_ref::<String>() {
                        None => Err(ErrorCode::PanicError("Sorry, unknown panic message")),
                        Some(message) => Err(ErrorCode::PanicError(message.to_string())),
                    },
                    Some(message) => Err(ErrorCode::PanicError(message.to_string())),
                },
            };

            if let Err(error_code) = join_res {
                let may_error = Some(error_code);
                (self.on_finished_callback)(&may_error)?;
                return Err(may_error.unwrap());
            }
        }

        if let Err(error_code) = self.graph.check_finished() {
            let may_error = Some(error_code);
            (self.on_finished_callback)(&may_error)?;
            return Err(may_error.unwrap());
        }

        (self.on_finished_callback)(&None)?;
        Ok(())
    }

    fn start_executor_daemon(self: &Arc<Self>) -> Result<()> {
        if !self.settings.max_execute_time.is_zero() {
            let this = self.clone();
            self.async_runtime.spawn(async move {
                let max_execute_time = this.settings.max_execute_time;
                let finished_future = Box::pin(this.finished_notify.notified());
                let max_execute_future = Box::pin(tokio::time::sleep(max_execute_time));
                if let Either::Left(_) = select(max_execute_future, finished_future).await {
                    this.execute_timeout.store(true, Ordering::Relaxed);
                    if let Err(cause) = this.finish() {
                        warn!("Cannot finish pipeline executor in max execute time guard. cause: {:?}", cause);
                    }
                }
            });
        }

        Ok(())
    }

    fn execute_threads(self: &Arc<Self>, threads_size: usize) -> Vec<JoinHandle<Result<()>>> {
        let mut thread_join_handles = Vec::with_capacity(threads_size);

        for thread_num in 0..threads_size {
            let this = self.clone();
            #[allow(unused_mut)]
            let mut name = Some(format!("PipelineExecutor-{}", thread_num));

            #[cfg(debug_assertions)]
            {
                // We need to pass the thread name in the unit test, because the thread name is the test name
                if matches!(std::env::var("UNIT_TEST"), Ok(var_value) if var_value == "TRUE") {
                    if let Some(cur_thread_name) = std::thread::current().name() {
                        name = Some(cur_thread_name.to_string());
                    }
                }
            }

            thread_join_handles.push(Thread::named_spawn(name, move || unsafe {
                let this_clone = this.clone();
                let try_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                    move || -> Result<()> {
                        match this_clone.execute_single_thread(thread_num) {
                            Ok(_) => Ok(()),
                            Err(cause) => this_clone.throw_error(thread_num, cause),
                        }
                    },
                ));

                match try_result {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err(cause)) => Err(cause),
                    Err(cause) => {
                        this.finish()?;
                        match cause.downcast_ref::<&'static str>() {
                            None => match cause.downcast_ref::<String>() {
                                None => Err(ErrorCode::PanicError("Sorry, unknown panic message")),
                                Some(message) => Err(ErrorCode::PanicError(message.to_string())),
                            },
                            Some(message) => Err(ErrorCode::PanicError(message.to_string())),
                        }
                    }
                }
            }));
        }
        thread_join_handles
    }

    fn throw_error(self: &Arc<Self>, thread_num: usize, cause: ErrorCode) -> Result<()> {
        // Wake up other threads to finish when throw error
        self.finish()?;

        Err(cause.add_message_back(format!(" (while in processor thread {})", thread_num)))
    }

    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    pub unsafe fn execute_single_thread(&self, thread_num: usize) -> Result<()> {
        let workers_condvar = self.workers_condvar.clone();
        let mut context = ExecutorWorkerContext::create(thread_num, workers_condvar);

        while !self.global_tasks_queue.is_finished() && !self.need_abort() {
            // When there are not enough tasks, the thread will be blocked, so we need loop check.
            while !self.global_tasks_queue.is_finished() && !context.has_task() {
                self.global_tasks_queue.steal_task_to_context(&mut context);
            }

            while !self.global_tasks_queue.is_finished() && !self.need_abort() && context.has_task()
            {
                if let Some(executed_pid) = context.execute_task(self)? {
                    // We immediately schedule the processor again.
                    let schedule_queue = self.graph.schedule_queue(executed_pid)?;
                    schedule_queue.schedule(&self.global_tasks_queue, &mut context);
                }
            }
        }

        if self.execute_timeout.load(Ordering::Relaxed) {
            self.finish()?;
            return Err(ErrorCode::AbortedQuery(
                "Aborted query, because the execution time exceeds the maximum execution time limit",
            ));
        }

        if self.need_abort() {
            self.finish()?;
            return Err(ErrorCode::AbortedQuery(
                "Aborted query, because the server is shutting down or the query was killed",
            ));
        }

        Ok(())
    }

    fn need_abort(&self) -> bool {
        self.query_need_abort.load(Ordering::Relaxed)
    }
}

impl Drop for PipelineExecutor {
    fn drop(&mut self) {
        if let Err(cause) = self.finish() {
            warn!("Catch error when drop pipeline executor {:?}", cause);
        }
    }
}
