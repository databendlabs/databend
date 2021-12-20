use common_exception::Result;

use crate::pipelines::new::executor::executor_graph::RunningGraph;
use crate::pipelines::new::executor::executor_tasks::ExecutorTasksQueue;
use crate::pipelines::new::executor::executor_worker_context::ExecutorWorkerContext;

pub struct PipelineExecutor {
    graph: RunningGraph,
    global_tasks_queue: ExecutorTasksQueue,
}

impl PipelineExecutor {
    pub fn create() -> PipelineExecutor {
        // PipelineExecutor {}
        unimplemented!()
    }

    pub fn initialize_executor(&self, workers: usize) -> Result<()> {
        self.graph.initialize_executor()?;
        unimplemented!()
    }

    pub fn execute_with_single_worker(&self, worker_num: usize) -> Result<()> {
        let mut context = ExecutorWorkerContext::create(worker_num);

        while !self.global_tasks_queue.is_finished() {
            // When there are not enough tasks, the thread will be blocked, so we need loop check.
            while !self.global_tasks_queue.is_finished() && !context.has_task() {
                self.global_tasks_queue.steal_task_to_context(&mut context);
            }

            while context.has_task() {
                let executed_pid = context.execute_task(&self.global_tasks_queue)?;

                // We immediately schedule the processor again.
                let schedule_queue = self.graph.schedule_next(executed_pid)?;
                schedule_queue.schedule(&self.global_tasks_queue, &mut context);
            }
        }

        Ok(())
    }
}
