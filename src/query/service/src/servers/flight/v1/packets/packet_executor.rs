// Copyright 2021 Datafuse Labs
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

use dashmap::DashMap;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_types::NodeInfo;
use databend_common_settings::ChangeValue;

use crate::servers::flight::v1::actions::INIT_QUERY_FRAGMENTS;
use crate::servers::flight::v1::packets::packet::create_client;
use crate::servers::flight::v1::packets::FragmentPlanPacket;
use crate::servers::flight::v1::packets::Packet;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct QueryFragmentsPlanPacket {
    pub query_id: String,
    pub query_kind: QueryKind,
    pub executor: String,
    pub request_executor: String,
    pub fragments: Vec<FragmentPlanPacket>,
    pub changed_settings: Arc<DashMap<String, ChangeValue>>,
    // We send nodes info for each node. This is a bad choice
    pub executors_info: HashMap<String, Arc<NodeInfo>>,
}

impl QueryFragmentsPlanPacket {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        query_id: String,
        query_kind: QueryKind,
        executor: String,
        fragments: Vec<FragmentPlanPacket>,
        executors_info: HashMap<String, Arc<NodeInfo>>,
        changed_settings: Arc<DashMap<String, ChangeValue>>,
        request_executor: String,
    ) -> QueryFragmentsPlanPacket {
        QueryFragmentsPlanPacket {
            query_id,
            query_kind,
            executor,
            fragments,
            executors_info,
            changed_settings,
            request_executor,
        }
    }
}

#[async_trait::async_trait]
impl Packet for QueryFragmentsPlanPacket {
    #[async_backtrace::framed]
    async fn commit(&self, config: &InnerConfig, timeout: u64) -> Result<()> {
        if !self.executors_info.contains_key(&self.executor) {
            return Err(ErrorCode::Internal(format!(
                "Not found {} node in cluster",
                &self.executor
            )));
        }

        let executor = &self.executors_info[&self.executor];
        let mut conn = create_client(config, &executor.flight_address).await?;
        conn.do_action(INIT_QUERY_FRAGMENTS, self.clone(), timeout)
            .await
    }
}
