use crate::pipelines::new::ProcessInfo;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ExecutorProfiling {
    graphviz: String,
    processors: Vec<Box<dyn ProcessInfo>>,
}

impl ExecutorProfiling {
    pub fn create(graphviz: String, processors: Vec<Box<dyn ProcessInfo>>) -> ExecutorProfiling {
        ExecutorProfiling {
            graphviz,
            processors,
        }
    }
}
