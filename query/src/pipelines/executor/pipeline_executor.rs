use common_base::Runtime;
use common_exception::{ErrorCode, Result};

pub struct PipelineExecutor {}

impl PipelineExecutor {
    pub fn create() -> PipelineExecutor {
        PipelineExecutor {}
    }

    pub fn initialize_executor(&mut self, workers: usize) -> Result<()> {
        unimplemented!()
    }

    pub fn schedule(&self, worker_num: usize) -> Result<()> {
        unimplemented!()
    }
}
