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

use crate::flight_actions::FlightAction;
use crate::flight_actions::InitQueryFragmentsPlan;
use crate::packets::packet::create_client;
use crate::packets::packet::Packet;
use crate::packets::FragmentPlanPacket;
use crate::Config;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct QueryFragmentsPlanPacket {
    pub query_id: String,
    pub executor: String,
    pub request_executor: String,
    pub fragments: Vec<FragmentPlanPacket>,
    // We send nodes info for each node. This is a bad choice
    pub executors_info: HashMap<String, Arc<NodeInfo>>,
    pub source_2_fragments: HashMap<String, Vec<usize>>,
}

impl QueryFragmentsPlanPacket {
    pub fn create(
        query_id: String,
        executor: String,
        fragments: Vec<FragmentPlanPacket>,
        executors_info: HashMap<String, Arc<NodeInfo>>,
        source_2_fragments: HashMap<String, Vec<usize>>,
        request_executor: String,
    ) -> QueryFragmentsPlanPacket {
        QueryFragmentsPlanPacket {
            query_id,
            executor,
            fragments,
            executors_info,
            request_executor,
            source_2_fragments,
        }
    }
}

#[async_trait::async_trait]
impl Packet for QueryFragmentsPlanPacket {
    async fn commit(&self, config: &Config, timeout: u64) -> Result<()> {
        if !self.executors_info.contains_key(&self.executor) {
            return Err(ErrorCode::LogicalError(format!(
                "Not found {} node in cluster",
                &self.executor
            )));
        }

        let executor = &self.executors_info[&self.executor];
        let mut conn = create_client(config, &executor.flight_address).await?;
        let action = FlightAction::InitQueryFragmentsPlan(InitQueryFragmentsPlan {
            executor_packet: self.clone(),
        });
        conn.execute_action(action, timeout).await
    }
}
