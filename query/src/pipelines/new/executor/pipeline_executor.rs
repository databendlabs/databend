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
use std::sync::Arc;
use std::thread::JoinHandle;

use common_base::Runtime;
use common_base::Thread;
use common_exception::ErrorCode;
use common_exception::Result;
use common_tracing::tracing;

use crate::pipelines::new::executor::executor_graph::RunningGraph;
use crate::pipelines::new::executor::executor_notify::WorkersNotify;
use crate::pipelines::new::executor::executor_tasks::ExecutorTasksQueue;
use crate::pipelines::new::executor::executor_worker_context::ExecutorWorkerContext;
use crate::pipelines::new::pipeline::NewPipeline;

pub struct PipelineExecutor {
    threads_num: usize,
    graph: RunningGraph,
    workers_notify: Arc<WorkersNotify>,
    pub async_runtime: Arc<Runtime>,
    pub global_tasks_queue: Arc<ExecutorTasksQueue>,
}

impl PipelineExecutor {
    pub fn create(async_rt: Arc<Runtime>, pipeline: NewPipeline) -> Result<Arc<PipelineExecutor>> {
        unsafe {
            let threads_num = pipeline.get_max_threads();
            let workers_notify = WorkersNotify::create(threads_num);
            let global_tasks_queue = ExecutorTasksQueue::create(threads_num);

            let graph = RunningGraph::create(pipeline)?;
            let mut init_schedule_queue = graph.init_schedule_queue()?;

            let mut tasks = VecDeque::new();
            while let Some(task) = init_schedule_queue.pop_task() {
                tasks.push_back(task);
            }

            global_tasks_queue.init_tasks(tasks);
            Ok(Arc::new(PipelineExecutor {
                graph,
                threads_num,
                workers_notify,
                global_tasks_queue,
                async_runtime: async_rt,
            }))
        }
    }

    pub fn finish(&self) -> Result<()> {
        self.global_tasks_queue.finish();
        self.workers_notify.wakeup_all();
        Ok(())
    }

    pub fn execute(self: &Arc<Self>) -> Result<()> {
        let mut threads = self.execute_threads(self.threads_num);

        while let Some(join_handle) = threads.pop() {
            // flatten error.
            match join_handle.join() {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(cause)) => Err(cause),
                Err(cause) => Err(ErrorCode::LogicalError(format!("{:?}", cause))),
            }?;
        }

        Ok(())
    }

    fn execute_threads(self: &Arc<Self>, threads_size: usize) -> Vec<JoinHandle<Result<()>>> {
        let mut threads = Vec::with_capacity(threads_size);

        for thread_num in 0..threads_size {
            let this = self.clone();
            let name = format!("PipelineExecutor-{}", thread_num);
            threads.push(Thread::named_spawn(Some(name), move || unsafe {
                match this.execute_single_thread(thread_num) {
                    Ok(_) => Ok(()),
                    Err(cause) => this.throw_error(thread_num, cause),
                }
            }));
        }

        threads
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
        let workers_notify = self.workers_notify.clone();
        let mut context = ExecutorWorkerContext::create(thread_num, workers_notify);

        while !self.global_tasks_queue.is_finished() {
            // When there are not enough tasks, the thread will be blocked, so we need loop check.
            while !self.global_tasks_queue.is_finished() && !context.has_task() {
                self.global_tasks_queue.steal_task_to_context(&mut context);
            }

            while context.has_task() {
                if let Some(executed_pid) = context.execute_task(self)? {
                    // We immediately schedule the processor again.
                    let schedule_queue = self.graph.schedule_queue(executed_pid)?;
                    schedule_queue.schedule(&self.global_tasks_queue, &mut context);
                }
            }
        }

        Ok(())
    }
}

impl Drop for PipelineExecutor {
    fn drop(&mut self) {
        if let Err(cause) = self.finish() {
            tracing::warn!("Catch error when drop pipeline executor {:?}", cause);
        }
    }
}
