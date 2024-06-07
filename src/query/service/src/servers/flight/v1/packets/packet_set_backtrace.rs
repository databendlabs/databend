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

use std::sync::Arc;

use databend_common_config::InnerConfig;
use databend_common_exception::Result;
use databend_common_meta_types::NodeInfo;

use crate::servers::flight::v1::actions::SET_BACKTRACE;
use crate::servers::flight::v1::packets::packet::create_client;
use crate::servers::flight::v1::packets::Packet;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct SetBacktracePacket {
    pub switch: bool,
    pub executor: Arc<NodeInfo>,
}

impl SetBacktracePacket {
    pub fn create(switch: bool, executor: Arc<NodeInfo>) -> Self {
        SetBacktracePacket { switch, executor }
    }
}

#[async_trait::async_trait]
impl Packet for SetBacktracePacket {
    #[async_backtrace::framed]
    async fn commit(&self, config: &InnerConfig, timeout: u64) -> Result<()> {
        let executor_info = &self.executor;
        let mut conn = create_client(config, &executor_info.flight_address).await?;

        conn.do_action(SET_BACKTRACE, self.clone(), timeout).await
    }
}
