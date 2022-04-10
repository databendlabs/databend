use std::collections::HashMap;
use std::sync::Arc;
use common_meta_types::NodeInfo;
use crate::api::rpc::packet::packet_fragment::FragmentPacket;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ExecutorPacket {
    pub query_id: String,
    pub executor: String,
    pub fragments: Vec<FragmentPacket>,
    // We send nodes info for each node. This is a bad choice
    pub executors_info: HashMap<String, Arc<NodeInfo>>,
}

impl ExecutorPacket {
    pub fn create(
        query_id: String,
        executor: String,
        fragments: Vec<FragmentPacket>,
        executors_info: HashMap<String, Arc<NodeInfo>>,
    ) -> ExecutorPacket {
        ExecutorPacket {
            query_id,
            executor,
            fragments,
            executors_info,
        }
    }
}
