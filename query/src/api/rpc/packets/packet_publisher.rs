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

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::NodeInfo;

use crate::api::rpc::flight_actions::InitNodesChannel;
use crate::api::rpc::packets::packet::create_client;
use crate::api::rpc::Packet;
use crate::api::FlightAction;
use crate::Config;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConnectionInfo {
    pub target: Arc<NodeInfo>,
    pub fragments: Vec<usize>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct InitNodesChannelPacket {
    pub query_id: String,
    pub executor: Arc<NodeInfo>,
    pub connections_info: Vec<ConnectionInfo>,
}

impl InitNodesChannelPacket {
    pub fn create(query_id: String, executor: Arc<NodeInfo>, connections_info: Vec<ConnectionInfo>) -> InitNodesChannelPacket {
        InitNodesChannelPacket {
            query_id,
            executor,
            connections_info,
        }
    }
}

#[async_trait::async_trait]
impl Packet for InitNodesChannelPacket {
    async fn commit(&self, config: &Config, timeout: u64) -> Result<()> {
        let executor_info = &self.executor;
        let mut conn = create_client(config, &executor_info.flight_address).await?;
        let action = FlightAction::InitNodesChannel(InitNodesChannel {
            init_nodes_channel_packet: self.clone(),
        });
        conn.execute_action(action, timeout).await
    }
}
