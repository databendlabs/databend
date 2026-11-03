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
use databend_common_meta_app::principal::UDFDefinition;
use databend_common_sql::planner::UDFValidator;
use databend_common_sql::plans::CreateUDFPlan;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use log::debug;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContextAuthorization;
use crate::sessions::TableContextQueryIdentity;
use crate::sessions::TableContextTableAccess;

#[derive(Debug)]
pub struct CreateUserUDFScript {
    ctx: Arc<QueryContext>,
    plan: CreateUDFPlan,
}

impl CreateUserUDFScript {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateUDFPlan) -> Result<Self> {
        Ok(CreateUserUDFScript { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateUserUDFScript {
    fn name(&self) -> &str {
        "CreateUserUDFScript"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    fn execute2(&self) -> futures::future::BoxFuture<'_, Result<PipelineBuildResult>> {
        Box::pin(async move {
            debug!("ctx.id" = self.ctx.get_id().as_str(); "create_user_udf_execute");

            let plan = self.plan.clone();
            let tenant = self.ctx.get_tenant();
            let udf = plan.udf;
            if let UDFDefinition::UDFServer(server) = &udf.definition
                && !server.external_access_integrations.is_empty()
            {
                UDFValidator::verify_server(self.ctx.as_ref(), &udf.name, server).await?;
            }
            // The UDF and its ownership are written in one Meta transaction.
            // IF NOT EXISTS on an existing UDF writes neither.
            let owner = self.ctx.get_current_role().map(|role| role.name);
            let written = UserApiProvider::instance()
                .add_udf_with_owner(&tenant, udf, &plan.create_option, owner.as_deref())
                .await?;
            if written {
                RoleCacheManager::instance().invalidate_cache(&tenant);
            }

            Ok(PipelineBuildResult::create())
        })
    }
}
