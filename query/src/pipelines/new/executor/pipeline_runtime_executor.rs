use futures::future::{BoxFuture, JoinAll};
use futures::FutureExt;
use common_base::{Runtime, TrySpawn};
use common_exception::Result;
use common_exception::ErrorCode;

use crate::pipelines::new::executor::pipeline_executor::PipelineExecutor;

pub struct PipelineRuntimeExecutor {
    base: PipelineExecutor,
    is_started: bool,
    workers_join_handler: Option<BoxFuture<'static, ()>>,
}

impl PipelineRuntimeExecutor {
    pub fn start(&mut self, workers: usize, runtime: Runtime) -> Result<()> {
        if self.is_started {
            return Err(ErrorCode::PipelineAreadlyStarted("PipelineThreadsExecutor already started."));
        }

        self.base.initialize_executor(workers)?;
        // let mut join_handler = Vec::with_capacity(workers);
        // for worker_index in 0..workers {
        //     join_handler.push(runtime.spawn(async move {
        //         self.base.schedule(worker_index);
        //         ()
        //     }));
        // }

        // self.workers_join_handler = Some(futures::future::join_all(join_handler).boxed());
        Ok(())
    }

    pub async fn finish(&mut self) -> Result<()> {
        if !self.is_started || self.workers_join_handler.is_none() {
            return Err(ErrorCode::PipelineNotStarted("PipelineThreadsExecutor not started."));
        }

        if let Some(workers_join_handler) = self.workers_join_handler.take() {
            // TODO: maybe sync?
            workers_join_handler.await;
        }

        Ok(())
    }
}
