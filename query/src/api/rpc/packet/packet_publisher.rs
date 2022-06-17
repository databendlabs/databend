use std::collections::HashMap;
use std::sync::Arc;

use common_meta_types::NodeInfo;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PrepareChannel {
    pub query_id: String,
    pub executor: String,
    pub request_server: String,
    pub target_nodes_info: HashMap<String, Arc<NodeInfo>>,
    pub target_2_fragments: HashMap<String, Vec<usize>>,
    pub data_endpoints: HashMap<String, Arc<NodeInfo>>,
}

impl PrepareChannel {
    pub fn create(
        query_id: String,
        executor_id: String,
        request_server: String,
        executors: HashMap<String, Arc<NodeInfo>>,
        target_nodes_info: HashMap<String, Arc<NodeInfo>>,
        target_2_fragments: HashMap<String, Vec<usize>>,
    ) -> PrepareChannel {
        PrepareChannel {
            query_id,
            request_server,
            target_nodes_info,
            target_2_fragments,
            executor: executor_id,
            data_endpoints: executors,
        }
    }
}
