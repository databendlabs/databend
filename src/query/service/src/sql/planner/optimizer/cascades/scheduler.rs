use std::collections::VecDeque;

use common_exception::Result;

use super::tasks::Task;
use super::CascadesOptimizer;

pub struct Scheduler {
    task_queue: VecDeque<Task>,
}

impl Scheduler {
    pub fn new() -> Self {
        Self {
            task_queue: Default::default(),
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
        }

        Ok(())
    }

    pub fn add_task(&mut self, task: Task) {
        self.task_queue.push_back(task);
    }
}
