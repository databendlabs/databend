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

use databend_common_exception::Result;
use databend_common_management::RoleApi;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::UserDefinedConnection;
use databend_common_sql::plans::CreateConnectionPlan;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use log::debug;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct CreateConnectionInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateConnectionPlan,
}

impl CreateConnectionInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateConnectionPlan) -> Result<Self> {
        Ok(Self { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateConnectionInterpreter {
    fn name(&self) -> &str {
        "CreateConnectionInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "create_connection_execute");

        let plan = self.plan.clone();
        let user_mgr = UserApiProvider::instance();
        let conn = UserDefinedConnection::new(
            &plan.name,
            plan.storage_type.clone(),
            plan.storage_params.clone(),
        );

        let tenant = self.ctx.get_tenant();
        let _create_file_format = user_mgr
            .add_connection(&tenant, conn, &plan.create_option)
            .await?;

        // Grant ownership as the current role
        if self
            .ctx
            .get_settings()
            .get_enable_experimental_connection_privilege_check()?
        {
            if let Some(current_role) = self.ctx.get_current_role() {
                let role_api = UserApiProvider::instance().role_api(&tenant);
                role_api
                    .grant_ownership(
                        &OwnershipObject::Connection {
                            name: self.plan.name.clone(),
                        },
                        &current_role.name,
                    )
                    .await?;
                RoleCacheManager::instance().invalidate_cache(&tenant);
            }
        }

        Ok(PipelineBuildResult::create())
    }
}
