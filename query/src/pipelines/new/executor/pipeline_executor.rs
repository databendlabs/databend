use common_base::Runtime;
use common_exception::{ErrorCode, Result};
use crate::pipelines::new::executor::exector_graph::RunningGraph;

pub struct PipelineExecutor {
    graph: RunningGraph,
}

impl PipelineExecutor {
    pub fn create() -> PipelineExecutor {
        // PipelineExecutor {}
        unimplemented!()
    }

    pub fn initialize_executor(&self, workers: usize) -> Result<()> {
        self.graph.initialize_executor()?;
        unimplemented!()
    }

    pub fn schedule(&self, worker_num: usize) -> Result<()> {
        unimplemented!()
    }
}
