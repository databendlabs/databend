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

use common_exception::Result;

use crate::pipelines::new::executor::executor_graph::RunningGraph;
use crate::pipelines::new::executor::executor_notify::WorkersNotify;
use crate::pipelines::new::executor::executor_tasks::ExecutorTasksQueue;
use crate::pipelines::new::executor::executor_worker_context::ExecutorWorkerContext;
use crate::pipelines::new::pipeline::NewPipeline;

pub struct PipelineExecutor {
    graph: RunningGraph,
    workers_notify: Arc<WorkersNotify>,
    global_tasks_queue: ExecutorTasksQueue,
}

impl PipelineExecutor {
    pub fn create(pipeline: NewPipeline, workers: usize) -> Result<Arc<PipelineExecutor>> {
        unsafe {
            let workers_notify = WorkersNotify::create(workers);
            let global_tasks_queue = ExecutorTasksQueue::create(workers);

            let graph = RunningGraph::create(pipeline)?;
            let mut init_schedule_queue = graph.init_schedule_queue()?;

            // println!("Init queue: {:?}", init_schedule_queue);
            let mut tasks = VecDeque::new();
            while let Some(task) = init_schedule_queue.pop_task() {
                tasks.push_back(task);
            }

            global_tasks_queue.init_tasks(tasks);
            Ok(Arc::new(PipelineExecutor {
                graph,
                workers_notify,
                global_tasks_queue,
            }))
        }
    }

    pub fn finish(&self) {
        self.global_tasks_queue.finish();
        self.workers_notify.wakeup_all();
    }

    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    pub unsafe fn execute(&self, worker_num: usize) -> Result<()> {
        let workers_notify = self.workers_notify.clone();
        let mut context = ExecutorWorkerContext::create(worker_num, workers_notify);

        while !self.global_tasks_queue.is_finished() {
            // When there are not enough tasks, the thread will be blocked, so we need loop check.
            while !self.global_tasks_queue.is_finished() && !context.has_task() {
                // let (sender, receiver) = std::sync::mpsc::channel();
                self.global_tasks_queue.steal_task_to_context(&mut context);
            }

            while context.has_task() {
                let executed_pid = context.execute_task(&self.global_tasks_queue)?;

                // We immediately schedule the processor again.
                let schedule_queue = self.graph.schedule_queue(executed_pid)?;
                // println!("{} queue: {:?}", std::thread::current().name().unwrap(), schedule_queue);
                schedule_queue.schedule(&self.global_tasks_queue, &mut context);
            }
        }

        Ok(())
    }
}
