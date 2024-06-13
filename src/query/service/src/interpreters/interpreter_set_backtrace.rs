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

use databend_common_catalog::table_context::TableContext;
use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use databend_common_sql::plans::SetBacktracePlan;
use databend_common_exception::set_backtrace;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::servers::flight::v1::packets::Packet;
use crate::servers::flight::v1::packets::SetBacktracePacket;
use crate::sessions::QueryContext;

pub struct SetBacktraceInterpreter {
    ctx: Arc<QueryContext>,
    plan: SetBacktracePlan,
    proxy_to_cluster: bool,
}

impl SetBacktraceInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: SetBacktracePlan) -> Result<Self> {
        Ok(SetBacktraceInterpreter {
            ctx,
            plan,
            proxy_to_cluster: true,
        })
    }

    pub fn from_flight(ctx: Arc<QueryContext>, packet: SetBacktracePacket) -> Result<Self> {
        Ok(SetBacktraceInterpreter {
            ctx,
            plan: SetBacktracePlan {
                switch: packet.switch,
            },
            proxy_to_cluster: false,
        })
    }
}

#[async_trait::async_trait]
impl Interpreter for SetBacktraceInterpreter {
    fn name(&self) -> &str {
        "SetBacktraceInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        if self.proxy_to_cluster {
            let settings = self.ctx.get_settings();
            let timeout = settings.get_flight_client_timeout()?;
            let conf = GlobalConfig::instance();
            let cluster = self.ctx.get_cluster();
            for node_info in &cluster.nodes {
                if node_info.id != cluster.local_id {
                    let set_backtrace_packet =
                        SetBacktracePacket::create(self.plan.switch, node_info.clone());
                    set_backtrace_packet.commit(conf.as_ref(), timeout).await?;
                }
            }
        }

        set_backtrace(self.plan.switch);
        Ok(PipelineBuildResult::create())
    }
}
