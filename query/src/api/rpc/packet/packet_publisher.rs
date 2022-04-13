use std::collections::HashMap;
use std::sync::Arc;
use common_meta_types::NodeInfo;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PublisherPacket {
    pub query_id: String,
    pub executor: String,
    pub executors_info: HashMap<String, Arc<NodeInfo>>,
}

impl PublisherPacket {
    pub fn create(
        query_id: String,
        executor_id: String,
        executors: HashMap<String, Arc<NodeInfo>>,
    ) -> PublisherPacket {
        PublisherPacket {
            query_id,
            executor: executor_id,
            executors_info: executors,
        }
    }
}
