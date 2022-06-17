use std::collections::HashMap;
use std::sync::Arc;

use common_meta_types::NodeInfo;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ExecutePacket {
    pub query_id: String,
    pub executor: String,
    // We send nodes info for each node. This is a bad choice
    pub executors_info: HashMap<String, Arc<NodeInfo>>,
}

impl ExecutePacket {
    pub fn create(
        query_id: String,
        executor: String,
        executors_info: HashMap<String, Arc<NodeInfo>>,
    ) -> ExecutePacket {
        ExecutePacket {
            query_id,
            executor,
            executors_info,
        }
    }
}
