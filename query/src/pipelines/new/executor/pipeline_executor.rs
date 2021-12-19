use common_base::Runtime;
use common_exception::{ErrorCode, Result};
use crate::pipelines::new::executor::exector_graph::RunningGraph;
use crate::pipelines::new::executor::executor_tasks::{GlobalExecutorTasks, ThreadTasksQueue, ThreadExecutorTasks, GlobalTasksQueue};

pub struct PipelineExecutor {
    graph: RunningGraph,
    global_tasks_queue: GlobalTasksQueue,
}

unsafe impl Send for PipelineExecutor {}

unsafe impl Sync for PipelineExecutor {}

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
        let mut local_tasks_queue = ThreadTasksQueue::create(worker_num);

        while !self.global_tasks_queue.is_finished() {

            // When there are not enough tasks, the thread will be blocked, so we need loop check.
            while !self.global_tasks_queue.is_finished() && local_tasks_queue.is_empty() {
                self.global_tasks_queue.steal_task(0, &mut local_tasks_queue);
            }

            while !local_tasks_queue.is_empty() {
                if let Err(cause) = local_tasks_queue.execute_task(&self.graph) {
                    // TODO: stop executor and report ErrorCode
                }

                // TODO: schedule next processor
                // self.graph.schedule_next();
            }
        }

        unimplemented!()
    }
}
