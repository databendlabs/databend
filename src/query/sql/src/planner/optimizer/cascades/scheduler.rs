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

use databend_common_exception::Result;
use log::debug;

use super::tasks::Task;
use super::CascadesOptimizer;

#[allow(clippy::type_complexity)]
pub struct Scheduler<'a> {
    task_queue: VecDeque<Task>,

    /// A counter to track the number of tasks
    /// that have been scheduled.
    scheduled_task_count: u64,

    /// The maximum number of tasks that can be scheduled.
    /// If the number of scheduled tasks exceeds this limit,
    /// the scheduler will stop scheduling new tasks.
    task_limit: u64,

    /// Task callback functions invoked before a task is executed.
    callback: Option<Box<dyn FnMut(&Task) + 'a>>,
}

impl<'a> Scheduler<'a> {
    pub fn new() -> Self {
        Self {
            task_queue: Default::default(),
            scheduled_task_count: 0,
            task_limit: u64::MAX,
            callback: None,
        }
    }

    /// Set the maximum number of tasks that can be scheduled.
    #[allow(dead_code)]
    pub fn with_task_limit(mut self, task_limit: u64) -> Self {
        self.task_limit = task_limit;
        self
    }

    /// Add a callback function that will be invoked before a task is executed.
    pub fn with_callback(mut self, callback: impl FnMut(&Task) + 'a) -> Self {
        self.callback = Some(Box::new(callback));
        self
    }

    pub fn run(&mut self, optimizer: &mut CascadesOptimizer) -> Result<()> {
        while let Some(mut task) = self.task_queue.pop_front() {
            if self.scheduled_task_count > self.task_limit {
                // Skip explore tasks if the task limit is reached.
                match task {
                    Task::ExploreGroup(t) => {
                        task = Task::ExploreGroup(t.with_termination());
                    }
                    Task::ExploreExpr(t) => {
                        task = Task::ExploreExpr(t.with_termination());
                    }
                    _ => {}
                }
            }

            if task.ref_count() > 0 {
                // The task is still referenced by other tasks, requeue it.
                self.task_queue.push_back(task);
                continue;
            }
            if let Some(callback) = &mut self.callback {
                callback(&task);
            }
            task.execute(optimizer, self)?;

            // Update the counter
            self.scheduled_task_count += 1;
        }

        debug!(
            "CascadesOptimizer: scheduled {} tasks",
            self.scheduled_task_count
        );

        Ok(())
    }

    pub fn add_task(&mut self, task: Task) {
        self.task_queue.push_back(task);
    }

    pub fn scheduled_task_count(&self) -> u64 {
        self.scheduled_task_count
    }
}
