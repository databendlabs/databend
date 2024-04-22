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

use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_base::runtime::catch_unwind;
use databend_common_base::runtime::drop_guard;
use databend_common_base::runtime::error_info::NodeErrorType;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::Thread;
use databend_common_base::runtime::ThreadJoinHandle;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use log::warn;
use log::LevelFilter;
use minitrace::full_name;
use minitrace::prelude::*;
use parking_lot::Mutex;

use crate::pipelines::executor::ExecutorWorkerContext;
use crate::pipelines::executor::QueriesExecutorTasksQueue;
use crate::pipelines::executor::RunningGraph;
use crate::pipelines::executor::WatchNotify;
use crate::pipelines::executor::WorkersCondvar;

pub struct QueriesPipelineExecutor {
    threads_num: usize,
    workers_condvar: Arc<WorkersCondvar>,
    pub async_runtime: Arc<Runtime>,
    pub global_tasks_queue: Arc<QueriesExecutorTasksQueue>,
    finished_notify: Arc<WatchNotify>,
    finished_error: Mutex<Option<ErrorCode>>,

    pub epoch: AtomicU32,
}

impl QueriesPipelineExecutor {
    pub fn create(threads_num: usize) -> Result<Arc<QueriesPipelineExecutor>> {
        let workers_condvar = WorkersCondvar::create(threads_num);
        let global_tasks_queue = QueriesExecutorTasksQueue::create(threads_num);

        Ok(Arc::new(QueriesPipelineExecutor {
            threads_num,
            workers_condvar,
            global_tasks_queue,
            async_runtime: GlobalIORuntime::instance(),
            finished_error: Mutex::new(None),
            finished_notify: Arc::new(WatchNotify::new()),
            epoch: AtomicU32::new(0),
        }))
    }

    #[minitrace::trace]
    pub fn execute(self: &Arc<Self>) -> Result<()> {
        let mut thread_join_handles = self.execute_threads(self.threads_num);

        while let Some(join_handle) = thread_join_handles.pop() {
            let thread_res = join_handle.join().flatten();

            {
                let finished_error_guard = self.finished_error.lock();
                if let Some(error) = finished_error_guard.as_ref() {
                    let may_error = error.clone();
                    drop(finished_error_guard);

                    return Err(may_error);
                }
            }
            if matches!(&thread_res, Err(_error)) {
                let may_error = thread_res.unwrap_err();
                return Err(may_error);
            }
        }

        Ok(())
    }

    pub fn send_graph(self: &Arc<Self>, graph: Arc<RunningGraph>) -> Result<()> {
        unsafe {
            let mut init_schedule_queue = graph.init_schedule_queue(self.threads_num)?;

            let async_queue = std::mem::take(&mut init_schedule_queue.async_queue);
            self.global_tasks_queue
                .init_async_tasks(async_queue, self.workers_condvar.clone());

            let sync_queue = std::mem::take(&mut init_schedule_queue.sync_queue);
            self.global_tasks_queue
                .init_sync_tasks(sync_queue, self.workers_condvar.clone());
            Ok(())
        }
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
        let mut context = ExecutorWorkerContext::create(thread_num, workers_condvar);

        while !self.global_tasks_queue.is_finished() {
            // When there are not enough tasks, the thread will be blocked, so we need loop check.
            while !self.global_tasks_queue.is_finished() && !context.has_task() {
                self.global_tasks_queue
                    .steal_task_to_context(&mut context, self);
            }

            while !self.global_tasks_queue.is_finished() && context.has_task() {
                let task_info = context.get_task_info();
                let execute_res = catch_unwind(|| context.execute_task(Some(self)));
                match execute_res {
                    Ok(res) => {
                        match res {
                            Ok(Some((executed_pid, graph))) => {
                                // Not scheduled graph if pipeline is finished.
                                if !self.global_tasks_queue.is_finished()
                                    && !graph.is_should_finish()
                                {
                                    // We immediately schedule the processor again.
                                    let schedule_queue_res =
                                        graph.clone().schedule_queue(executed_pid);
                                    match schedule_queue_res {
                                        Ok(schedule_queue) => {
                                            schedule_queue.schedule_with_condition(
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
                                            graph.should_finish(Err(cause))?;
                                        }
                                    }
                                }
                                if graph.is_all_nodes_finished() {
                                    graph.should_finish(Ok(()))?;
                                }
                            }
                            Err(node_error) => {
                                let cause = node_error.get_error_code();
                                warn!("Execute task error: {:?}", cause);
                                if let Some((graph, node_index)) = task_info {
                                    graph.record_node_error(node_index, *node_error);
                                    graph.should_finish(Err(cause.clone()))?;
                                }
                            }
                            _ => {}
                        }
                    }
                    Err(panic_error) => {
                        warn!("Execute task error: {:?}", panic_error);
                        if let Some((graph, node_index)) = task_info {
                            graph.record_node_error(
                                node_index,
                                NodeErrorType::SyncProcessError(panic_error.clone()),
                            );
                            graph.should_finish(Err(panic_error.clone()))?;
                        }
                    }
                }
            }
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
        self.finished_notify.notify_waiters();
    }

    pub fn is_finished(&self) -> bool {
        self.global_tasks_queue.is_finished()
    }

    #[inline]
    pub(crate) fn increase_global_epoch(&self) {
        self.epoch.fetch_add(1, Ordering::SeqCst);
    }
}

impl Drop for QueriesPipelineExecutor {
    fn drop(&mut self) {
        drop_guard(move || {
            self.finish(None);
        })
    }
}
