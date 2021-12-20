use std::sync::Arc;

use common_base::Runtime;
use common_base::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::future::BoxFuture;
use futures::future::JoinAll;
use futures::FutureExt;

use crate::pipelines::new::executor::pipeline_executor::PipelineExecutor;

pub struct PipelineRuntimeExecutor {
    base: Arc<PipelineExecutor>,
    is_started: bool,
    workers_join_handler: Option<BoxFuture<'static, ()>>,
}

impl PipelineRuntimeExecutor {
    pub fn start(&mut self, workers: usize, runtime: Runtime) -> Result<()> {
        if self.is_started {
            return Err(ErrorCode::PipelineAreadlyStarted(
                "PipelineThreadsExecutor already started.",
            ));
        }

        self.is_started = true;
        self.base.initialize_executor(workers)?;
        for worker_num in 0..workers {
            let worker_executor = self.base.clone();
            // TODO: wait runtime shutdown.
            runtime.spawn(async move {
                if let Err(cause) = worker_executor.execute_with_single_worker(worker_num) {
                    // TODO: worker
                    println!("Worker {} failure, cause {:?}", worker_num, cause);
                }
            });
        }
        Ok(())
    }

    // pub async fn finish(&mut self) -> Result<()> {
    //     if !self.is_started || self.workers_join_handler.is_none() {
    //         return Err(ErrorCode::PipelineNotStarted("PipelineThreadsExecutor not started."));
    //     }
    //
    //     if let Some(workers_join_handler) = self.workers_join_handler.take() {
    //         // TODO: maybe sync?
    //         workers_join_handler.await;
    //     }
    //
    //     Ok(())
    // }
}
