use common_base::Runtime;
use common_exception::{ErrorCode, Result};
use crate::pipelines::new::executor::graph::RunningGraph;

pub struct PipelineExecutor {
    graph: RunningGraph,
}

impl PipelineExecutor {
    pub fn create() -> PipelineExecutor {
        // PipelineExecutor {}
        unimplemented!()
    }

    pub fn initialize_executor(&mut self, workers: usize) -> Result<()> {
        self.graph.initialize_executor()?;
        unimplemented!()
    }

    pub fn schedule(&self, worker_num: usize) -> Result<()> {
        unimplemented!()
    }
}
