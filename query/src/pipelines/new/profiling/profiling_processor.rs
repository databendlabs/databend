use petgraph::stable_graph::NodeIndex;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ProcessorProfiling {
    id: usize,
    sync_elapse_ms: u64,
    async_elapse_ms: u64,
}



