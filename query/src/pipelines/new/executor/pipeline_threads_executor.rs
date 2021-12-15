use common_exception::ErrorCode;
use common_exception::Result;
use crate::pipelines::new::executor::pipeline_executor::PipelineExecutor;

pub struct PipelineThreadsExecutor {
    base: PipelineExecutor,
    is_started: bool,
}

impl PipelineThreadsExecutor {
    pub fn start(&mut self, workers: usize) -> Result<()> {
        if self.is_started {
            return Err(ErrorCode::PipelineAreadlyStarted("PipelineThreadsExecutor already started."));
        }

        unimplemented!()
    }
}
