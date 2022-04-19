use crate::pipelines::new::profiling::profiling_processor::ProcessorProfiling;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ExecutorProfiling {
    graphviz: String,
    processors: Vec<ProcessorProfiling>,
}

impl ExecutorProfiling {
    pub fn create(graphviz: String, processors: Vec<ProcessorProfiling>) -> ExecutorProfiling {
        ExecutorProfiling { graphviz, processors }
    }
}


