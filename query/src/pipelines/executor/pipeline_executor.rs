use common_base::Runtime;
use common_exception::{ErrorCode, Result};

pub struct PipelineExecutor {
    is_started: bool,
}

impl PipelineExecutor {
    pub fn create() -> PipelineExecutor {
        PipelineExecutor { is_started: false }
    }
}

impl PipelineExecutor {
    pub fn start_with(&mut self, workers: usize, runtime: Runtime) -> Result<()> {
        unimplemented!()
    }

    pub fn start(&mut self, workers: usize) -> Result<()> {
        if self.is_started {
            return Err(ErrorCode::PipelineAreadlyStarted("PipelineExecutor already started."));
        }

        unimplemented!()
    }
}
