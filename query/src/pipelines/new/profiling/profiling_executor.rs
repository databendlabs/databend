use crate::pipelines::new::ProcessInfo;
use crate::pipelines::new::profiling::profiling_processor::ProcessorProfiling;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ExecutorProfiling {
    graphviz: String,
    processors: Vec<Box<dyn ProcessInfo>>,
}

impl ExecutorProfiling {
    pub fn create(graphviz: String, processors: Vec<Box<dyn ProcessInfo>>) -> ExecutorProfiling {
        ExecutorProfiling { graphviz, processors }
    }
}


