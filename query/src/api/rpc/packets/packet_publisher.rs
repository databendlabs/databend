// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;

use common_meta_types::NodeInfo;
use crate::api::rpc::Packet;
use crate::Config;
use common_exception::{ErrorCode, Result};
use crate::api::FlightAction;
use crate::api::rpc::flight_actions::InitNodesChannel;
use crate::api::rpc::packets::packet::create_client;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct InitNodesChannelPacket {
    pub query_id: String,
    pub executor: String,
    pub request_server: String,
    pub target_nodes_info: HashMap<String, Arc<NodeInfo>>,
    pub target_2_fragments: HashMap<String, Vec<usize>>,
    pub data_endpoints: HashMap<String, Arc<NodeInfo>>,
}

impl InitNodesChannelPacket {
    pub fn create(
        query_id: String,
        executor_id: String,
        request_server: String,
        executors: HashMap<String, Arc<NodeInfo>>,
        target_nodes_info: HashMap<String, Arc<NodeInfo>>,
        target_2_fragments: HashMap<String, Vec<usize>>,
    ) -> InitNodesChannelPacket {
        InitNodesChannelPacket {
            query_id,
            request_server,
            target_nodes_info,
            target_2_fragments,
            executor: executor_id,
            data_endpoints: executors,
        }
    }
}

#[async_trait::async_trait]
impl Packet for InitNodesChannelPacket {
    async fn commit(&self, config: &Config, timeout: u64) -> Result<()> {
        if !self.data_endpoints.contains_key(&self.executor) {
            return Err(ErrorCode::LogicalError(format!(
                "Not found {} node in cluster",
                &self.executor
            )));
        }

        let executor_info = &self.data_endpoints[&self.executor];
        let mut conn = create_client(config, &executor_info.flight_address).await?;
        let action = FlightAction::InitNodesChannel(InitNodesChannel { publisher_packet });
        conn.execute_action(action, timeout).await
    }
}
