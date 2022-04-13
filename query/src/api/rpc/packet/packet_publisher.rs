use std::collections::HashMap;
use std::sync::Arc;
use common_meta_types::NodeInfo;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PublisherPacket {
    pub query_id: String,
    pub executor: String,
    pub request_server: String,
    pub data_endpoints: HashMap<String, Arc<NodeInfo>>,
}

impl PublisherPacket {
    pub fn create(
        query_id: String,
        executor_id: String,
        request_server: String,
        executors: HashMap<String, Arc<NodeInfo>>,
    ) -> PublisherPacket {
        PublisherPacket {
            query_id,
            request_server,
            executor: executor_id,
            data_endpoints: executors,
        }
    }
}
