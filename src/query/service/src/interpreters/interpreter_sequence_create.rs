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

use chrono::Utc;
use databend_common_exception::Result;
use databend_common_management::RoleApi;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::schema::CreateSequenceReq;
use databend_common_sql::plans::CreateSequencePlan;
use databend_common_storages_fuse::TableContext;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct CreateSequenceInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateSequencePlan,
}

impl CreateSequenceInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateSequencePlan) -> Result<Self> {
        Ok(CreateSequenceInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateSequenceInterpreter {
    fn name(&self) -> &str {
        "CreateSequenceInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let req = CreateSequenceReq {
            create_option: self.plan.create_option,
            ident: self.plan.ident.clone(),
            start: self.plan.start,
            increment: self.plan.increment,
            comment: self.plan.comment.clone(),
            create_on: Utc::now(),
            storage_version: 0,
        };
        let catalog = self.ctx.get_default_catalog()?;
        let _reply = catalog.create_sequence(req).await?;

        // Grant ownership as the current role
        if self
            .ctx
            .get_settings()
            .get_enable_experimental_sequence_privilege_check()?
        {
            let tenant = self.plan.ident.tenant();
            let name = self.plan.ident.name().to_string();
            if let Some(current_role) = self.ctx.get_current_role() {
                let role_api = UserApiProvider::instance().role_api(tenant);
                role_api
                    .grant_ownership(&OwnershipObject::Sequence { name }, &current_role.name)
                    .await?;
                RoleCacheManager::instance().invalidate_cache(tenant);
            }
        }

        Ok(PipelineBuildResult::create())
    }
}
