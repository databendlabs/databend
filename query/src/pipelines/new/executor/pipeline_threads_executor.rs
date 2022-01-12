use std::sync::Arc;

use common_base::Thread;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::pipelines::new::executor::pipeline_executor::PipelineExecutor;
use crate::pipelines::new::pipeline::NewPipeline;

pub struct PipelineThreadsExecutor {
    base: Option<Arc<PipelineExecutor>>,

    pipeline: Option<NewPipeline>,
}

impl PipelineThreadsExecutor {
    pub fn create(pipeline: NewPipeline) -> Result<PipelineThreadsExecutor> {
        Ok(PipelineThreadsExecutor {
            base: None,
            pipeline: Some(pipeline),
        })
    }

    pub fn start(&mut self, workers: usize) -> Result<()> {
        if self.base.is_some() {
            return Err(ErrorCode::AlreadyStarted("PipelineThreadsExecutor is already started."));
        }

        match self.pipeline.take() {
            None => Err(ErrorCode::LogicalError("Logical error: it's a bug.")),
            Some(pipeline) => self.start_workers(workers, pipeline),
        }
    }

    fn start_workers(&mut self, workers: usize, pipeline: NewPipeline) -> Result<()> {
        let executor = PipelineExecutor::create(pipeline, workers)?;

        self.base = Some(executor.clone());
        for worker_num in 0..workers {
            let worker = executor.clone();
            Thread::spawn(move || unsafe {
                if let Err(cause) = worker.execute_with_single_worker(worker_num) {
                    // TODO:
                    println!("Executor error : {:?}", cause);
                }
            });
        }

        Ok(())
    }
}
