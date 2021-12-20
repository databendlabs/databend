use std::sync::Arc;

use common_base::Thread;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::pipelines::new::executor::pipeline_executor::PipelineExecutor;

pub struct PipelineThreadsExecutor {
    base: Arc<PipelineExecutor>,
    is_started: bool,
}

impl PipelineThreadsExecutor {
    pub fn start(&mut self, workers: usize) -> Result<()> {
        if self.is_started {
            return Err(ErrorCode::PipelineAreadlyStarted(
                "PipelineThreadsExecutor already started.",
            ));
        }

        self.is_started = true;
        self.base.initialize_executor(workers)?;
        for worker_num in 0..workers {
            let worker_executor = self.base.clone();
            Thread::spawn(move || {
                if let Err(cause) = worker_executor.execute_with_single_worker(worker_num) {
                    // TODO:
                    println!("Executor error : {:?}", cause);
                }
            });
        }
        Ok(())
    }
}
