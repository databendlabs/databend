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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_sql::plans::KillPlan;
use serde::Deserialize;
use serde::Serialize;

use crate::clusters::ClusterHelper;
use crate::clusters::FlightParams;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::servers::flight::v1::actions::KILL_QUERY;
use crate::servers::flight::v1::actions::KILL_QUERY_V2;
use crate::servers::flight::v1::actions::KillQueryRequest;
use crate::sessions::QueriesQueueManager;
use crate::sessions::QueryContext;
use crate::sessions::TableContextCluster;
use crate::sessions::TableContextSettings;

/// Authorization captured on the requesting node and carried over authenticated Flight RPC.
#[derive(Clone, Serialize, Deserialize)]
pub struct KillQueryAccess {
    pub tenant: String,
    pub user: UserIdentity,
    pub is_super: bool,
}

impl KillQueryAccess {
    pub fn can_kill(&self, target_tenant: &str, target_user: Option<&UserIdentity>) -> bool {
        self.is_super || (self.tenant == target_tenant && target_user == Some(&self.user))
    }
}

pub struct KillInterpreter {
    ctx: Arc<QueryContext>,
    plan: KillPlan,
    proxy_to_warehouse: bool,
    access: Option<KillQueryAccess>,
}

impl KillInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: KillPlan) -> Result<Self> {
        Ok(KillInterpreter {
            ctx,
            plan,
            proxy_to_warehouse: true,
            access: None,
        })
    }

    pub fn from_flight(
        ctx: Arc<QueryContext>,
        plan: KillPlan,
        access: KillQueryAccess,
    ) -> Result<Self> {
        Ok(KillInterpreter {
            ctx,
            plan,
            proxy_to_warehouse: false,
            access: Some(access),
        })
    }

    #[async_backtrace::framed]
    async fn kill_warehouse_query(&self, access: &KillQueryAccess) -> Result<PipelineBuildResult> {
        let settings = self.ctx.get_settings();
        let warehouse = self.ctx.get_warehouse_cluster().await?;

        let flight_params = FlightParams {
            timeout: settings.get_flight_client_timeout()?,
            retry_times: settings.get_flight_max_retry_times()?,
            retry_interval: settings.get_flight_retry_interval()?,
            keep_alive: settings.get_flight_keep_alive_params()?,
            reuse_control_channel: false,
        };

        for node_info in &warehouse.nodes {
            if node_info.id == warehouse.local_id {
                continue;
            }

            let result = if access.is_super {
                // The legacy action is safe only for SUPER requesters, and works on old nodes.
                let message = HashMap::from([(node_info.id.clone(), self.plan.clone())]);
                warehouse
                    .do_action::<_, bool>(KILL_QUERY, message, flight_params)
                    .await
            } else {
                let request = KillQueryRequest {
                    plan: self.plan.clone(),
                    access: access.clone(),
                };
                let message = HashMap::from([(node_info.id.clone(), request)]);
                warehouse
                    .do_action::<_, bool>(KILL_QUERY_V2, message, flight_params)
                    .await
            };

            match result {
                Ok(res) if res.values().any(|found| *found) => {
                    return Ok(PipelineBuildResult::create());
                }
                // During rolling upgrades an old node cannot authorize this request. Never
                // fall back to the legacy action. Continue looking on other (upgraded) nodes.
                Err(cause) if !access.is_super && cause.code() == ErrorCode::UNIMPLEMENTED => {}
                Err(cause) => return Err(cause),
                Ok(_) => {}
            }
        }

        Err(self.unknown_session())
    }

    #[async_backtrace::framed]
    async fn execute_kill(&self, session_id: &str) -> Result<PipelineBuildResult> {
        let access = match &self.access {
            Some(access) => access.clone(),
            None => {
                let session = self.ctx.get_current_session();
                KillQueryAccess {
                    tenant: session.get_current_tenant().tenant_name().to_string(),
                    user: session.get_current_user()?.identity(),
                    is_super: session
                        .has_privilege(&GrantObject::Global, UserPrivilegeType::Super, false)
                        .await?,
                }
            }
        };

        let Some(kill_session) = self.ctx.get_session_by_id(session_id) else {
            return match self.proxy_to_warehouse {
                true => self.kill_warehouse_query(&access).await,
                false => Err(self.unknown_session()),
            };
        };

        let target_user = kill_session
            .get_current_user()
            .ok()
            .map(|user| user.identity());
        if !access.can_kill(
            kill_session.get_current_tenant().tenant_name(),
            target_user.as_ref(),
        ) {
            // Do not reveal whether another user's session exists.
            return Err(self.unknown_session());
        }

        // Queued queries must pass the same authorization as running queries.
        if let Some(query_id) = kill_session.get_current_query_id() {
            if QueriesQueueManager::instance().remove(query_id) {
                return Ok(PipelineBuildResult::create());
            }
        }

        if self.plan.kill_connection {
            kill_session.force_kill_session();
        } else {
            kill_session.force_kill_query(ErrorCode::aborting());
        }
        Ok(PipelineBuildResult::create())
    }

    fn unknown_session(&self) -> ErrorCode {
        let message = match self.plan.id.parse::<u32>() {
            Ok(id) => format!("MySQL connection id {} not found session id", id),
            Err(_) => format!("Not found session id {}", self.plan.id),
        };
        ErrorCode::UnknownSession(message)
    }
}

#[async_trait::async_trait]
impl Interpreter for KillInterpreter {
    fn name(&self) -> &str {
        "KillInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    fn execute2(&self) -> futures::future::BoxFuture<'_, Result<PipelineBuildResult>> {
        Box::pin(async move {
            let id = &self.plan.id;
            // If press Ctrl + C, MySQL Client will create a new session and send query
            // `kill query mysql_connection_id` to server.
            // the type of connection_id is u32, if parse success get session by connection_id,
            // otherwise use the session_id.
            // More info Link to: https://github.com/datafuselabs/databend/discussions/5405.
            match id.parse::<u32>() {
                Ok(mysql_conn_id) => match self.ctx.get_id_by_mysql_conn_id(&Some(mysql_conn_id)) {
                    Some(get) => self.execute_kill(&get).await,
                    None => Err(self.unknown_session()),
                },
                Err(_) => self.execute_kill(id).await,
            }
        })
    }
}
