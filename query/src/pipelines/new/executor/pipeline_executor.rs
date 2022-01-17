use std::collections::VecDeque;
use std::sync::Arc;
use common_exception::{Result};

use crate::pipelines::new::executor::executor_graph::RunningGraph;
use crate::pipelines::new::executor::executor_tasks::ExecutorTasksQueue;
use crate::pipelines::new::executor::executor_worker_context::ExecutorWorkerContext;
use crate::pipelines::new::pipeline::NewPipeline;

pub struct PipelineExecutor {
    graph: RunningGraph,
    global_tasks_queue: ExecutorTasksQueue,
}

impl PipelineExecutor {
    pub fn create(pipeline: NewPipeline, workers: usize) -> Result<Arc<PipelineExecutor>> {
        unsafe {
            let mut global_tasks_queue = ExecutorTasksQueue::create(workers);

            let graph = RunningGraph::create(pipeline)?;
            let mut init_schedule_queue = graph.init_schedule_queue()?;

            let mut tasks = VecDeque::new();
            while let Some(task) = init_schedule_queue.pop_task() {
                tasks.push_back(task);
            }

            global_tasks_queue.init_tasks(tasks);
            Ok(Arc::new(PipelineExecutor { graph, global_tasks_queue }))
        }
    }

    pub unsafe fn execute_with_single_worker(&self, worker_num: usize) -> Result<()> {
        let mut context = ExecutorWorkerContext::create(worker_num);

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
                schedule_queue.schedule(&self.global_tasks_queue, &mut context);
            }
        }

        Ok(())
    }
}
