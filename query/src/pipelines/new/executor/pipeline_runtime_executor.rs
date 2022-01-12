use std::sync::Arc;

use common_base::Runtime;
use common_base::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::future::BoxFuture;
use futures::future::JoinAll;
use futures::FutureExt;

use crate::pipelines::new::executor::pipeline_executor::PipelineExecutor;
use crate::pipelines::new::pipeline::NewPipeline;

pub struct PipelineRuntimeExecutor {
    base: Option<Arc<PipelineExecutor>>,
    pipeline: Option<NewPipeline>,
}

impl PipelineRuntimeExecutor {
    pub fn create(pipeline: NewPipeline) -> Result<PipelineRuntimeExecutor> {
        Ok(PipelineRuntimeExecutor {
            base: None,
            pipeline: Some(pipeline),
        })
    }

    pub fn start(&mut self, workers: usize, runtime: Runtime) -> Result<()> {
        if self.base.is_some() {
            return Err(ErrorCode::AlreadyStarted("PipelineRuntimeExecutor is already started."));
        }

        match self.pipeline.take() {
            None => Err(ErrorCode::LogicalError("Logical error: it's a bug.")),
            Some(pipeline) => self.start_workers(workers, pipeline, runtime),
        }
    }

    fn start_workers(&mut self, workers: usize, pipeline: NewPipeline, rt: Runtime) -> Result<()> {
        let executor = PipelineExecutor::create(pipeline, workers)?;

        self.base = Some(executor.clone());
        for worker_num in 0..workers {
            let worker = executor.clone();
            // TODO: wait runtime shutdown.
            rt.spawn(async move {
                unsafe {
                    if let Err(cause) = worker.execute_with_single_worker(worker_num) {
                        // TODO: worker
                        println!("Worker {} failure, cause {:?}", worker_num, cause);
                    }
                }
            });
        }
        Ok(())
    }
}
