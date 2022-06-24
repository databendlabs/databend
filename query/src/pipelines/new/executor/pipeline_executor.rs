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

use common_base::base::Runtime;
use common_base::base::Thread;
use common_exception::ErrorCode;
use common_exception::Result;
use common_tracing::tracing;

use crate::pipelines::new::executor::executor_condvar::WorkersCondvar;
use crate::pipelines::new::executor::executor_graph::RunningGraph;
use crate::pipelines::new::executor::executor_tasks::ExecutorTasksQueue;
use crate::pipelines::new::executor::executor_worker_context::ExecutorWorkerContext;
use crate::pipelines::new::pipeline::NewPipeline;

pub struct PipelineExecutor {
    threads_num: usize,
    graph: RunningGraph,
    workers_condvar: Arc<WorkersCondvar>,
    query_need_abort: Arc<AtomicBool>,
    pub async_runtime: Arc<Runtime>,
    pub global_tasks_queue: Arc<ExecutorTasksQueue>,
}

impl PipelineExecutor {
    pub fn create(
        async_rt: Arc<Runtime>,
        query_need_abort: Arc<AtomicBool>,
        pipeline: NewPipeline,
    ) -> Result<Arc<PipelineExecutor>> {
        let threads_num = pipeline.get_max_threads();
        assert_ne!(threads_num, 0, "Pipeline max threads cannot equals zero.");
        Self::try_create(
            async_rt,
            query_need_abort,
            RunningGraph::create(pipeline)?,
            threads_num,
        )
    }

    pub fn from_pipelines(
        async_rt: Arc<Runtime>,
        query_need_abort: Arc<AtomicBool>,
        pipelines: Vec<NewPipeline>,
    ) -> Result<Arc<PipelineExecutor>> {
        if pipelines.is_empty() {
            return Err(ErrorCode::LogicalError("Executor Pipelines is empty."));
        }

        let threads_num = pipelines
            .iter()
            .map(|x| x.get_max_threads())
            .max()
            .unwrap_or(0);

        assert_ne!(threads_num, 0, "Pipeline max threads cannot equals zero.");
        Self::try_create(
            async_rt,
            query_need_abort,
            RunningGraph::from_pipelines(pipelines)?,
            threads_num,
        )
    }

    fn try_create(
        async_rt: Arc<Runtime>,
        query_need_abort: Arc<AtomicBool>,
        graph: RunningGraph,
        threads_num: usize,
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
                global_tasks_queue,
                async_runtime: async_rt,
                query_need_abort,
            }))
        }
    }

    pub fn finish(&self) -> Result<()> {
        self.global_tasks_queue.finish(self.workers_condvar.clone());
        Ok(())
    }

    pub fn is_finished(&self) -> bool {
        self.global_tasks_queue.is_finished()
    }

    pub fn execute(self: &Arc<Self>) -> Result<()> {
        let mut thread_join_handles = self.execute_threads(self.threads_num);

        while let Some(join_handle) = thread_join_handles.pop() {
            // flatten error.
            match join_handle.join() {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(cause)) => Err(cause),
                Err(cause) => match cause.downcast_ref::<&'static str>() {
                    None => match cause.downcast_ref::<String>() {
                        None => Err(ErrorCode::PanicError("Sorry, unknown panic message")),
                        Some(message) => Err(ErrorCode::PanicError(message.to_string())),
                    },
                    Some(message) => Err(ErrorCode::PanicError(message.to_string())),
                },
            }?;
        }

        Ok(())
    }

    fn execute_threads(self: &Arc<Self>, threads_size: usize) -> Vec<JoinHandle<Result<()>>> {
        let mut thread_join_handles = Vec::with_capacity(threads_size);

        for thread_num in 0..threads_size {
            let this = self.clone();
            let name = format!("PipelineExecutor-{}", thread_num);
            thread_join_handles.push(Thread::named_spawn(Some(name), move || unsafe {
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

        return Err(cause.add_message_back(format!(" (while in processor thread {})", thread_num)));
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
            tracing::warn!("Catch error when drop pipeline executor {:?}", cause);
        }
    }
}
