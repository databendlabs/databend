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

use common_exception::Result;

use super::tasks::Task;
use super::CascadesOptimizer;

pub struct Scheduler {
    task_queue: VecDeque<Task>,

    /// A counter to track the number of tasks
    /// that have been scheduled.
    scheduled_task_count: u64,
}

impl Scheduler {
    pub fn new() -> Self {
        Self {
            task_queue: Default::default(),
            scheduled_task_count: 0,
        }
    }

    pub fn run(&mut self, optimizer: &mut CascadesOptimizer) -> Result<()> {
        while let Some(task) = self.task_queue.pop_front() {
            if task.ref_count() > 0 {
                // The task is still referenced by other tasks, requeue it.
                self.task_queue.push_back(task);
                continue;
            }
            task.execute(optimizer, self)?;

            // Update the counter
            self.scheduled_task_count += 1;
        }

        tracing::debug!(
            "CascadesOptimizer: scheduled {} tasks",
            self.scheduled_task_count
        );

        Ok(())
    }

    pub fn add_task(&mut self, task: Task) {
        self.task_queue.push_back(task);
    }
}
