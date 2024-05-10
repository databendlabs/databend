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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::plans::SetPriorityPlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::servers::flight::v1::packets::Packet;
use crate::servers::flight::v1::packets::SetPriorityPacket;
use crate::sessions::QueryContext;

pub struct SetPriorityInterpreter {
    ctx: Arc<QueryContext>,
    plan: SetPriorityPlan,
    proxy_to_cluster: bool,
}

impl SetPriorityInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: SetPriorityPlan) -> Result<Self> {
        Ok(SetPriorityInterpreter {
            ctx,
            plan,
            proxy_to_cluster: true,
        })
    }

    pub fn from_flight(ctx: Arc<QueryContext>, packet: SetPriorityPacket) -> Result<Self> {
        Ok(SetPriorityInterpreter {
            ctx,
            plan: SetPriorityPlan {
                id: packet.id,
                priority: packet.priority,
            },
            proxy_to_cluster: false,
        })
    }

    #[async_backtrace::framed]
    async fn set_cluster_priority(&self) -> Result<PipelineBuildResult> {
        let settings = self.ctx.get_settings();
        let timeout = settings.get_flight_client_timeout()?;
        let conf = GlobalConfig::instance();
        let cluster = self.ctx.get_cluster();
        for node_info in &cluster.nodes {
            if node_info.id != cluster.local_id {
                let set_priority_packet = SetPriorityPacket::create(
                    self.plan.id.clone(),
                    self.plan.priority,
                    node_info.clone(),
                );

                match set_priority_packet.commit(conf.as_ref(), timeout).await {
                    Ok(_) => {
                        return Ok(PipelineBuildResult::create());
                    }
                    Err(cause) => match cause.code() == ErrorCode::UNKNOWN_SESSION {
                        true => {
                            continue;
                        }
                        false => {
                            return Err(cause);
                        }
                    },
                }
            }
        }
        Err(ErrorCode::UnknownSession(format!(
            "Not found session id {}",
            self.plan.id
        )))
    }
}

#[async_trait::async_trait]
impl Interpreter for SetPriorityInterpreter {
    fn name(&self) -> &str {
        "SetPriorityInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let id = &self.plan.id;
        match self.ctx.get_session_by_id(id) {
            None => match self.proxy_to_cluster {
                true => self.set_cluster_priority().await,
                false => Err(ErrorCode::UnknownSession(format!(
                    "Not found session id {}",
                    id
                ))),
            },
            Some(set_session) => {
                set_session.set_query_priority(self.plan.priority);
                Ok(PipelineBuildResult::create())
            }
        }
    }
}
