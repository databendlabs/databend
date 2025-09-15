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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_management::RoleApi;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::schema::DropSequenceReq;
use databend_common_meta_app::KeyWithTenant;
use databend_common_sql::plans::DropSequencePlan;
use databend_common_storages_fuse::TableContext;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct DropSequenceInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropSequencePlan,
}

impl DropSequenceInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropSequencePlan) -> Result<Self> {
        Ok(DropSequenceInterpreter { ctx, plan })
    }

    pub async fn req_execute(
        ctx: &dyn TableContext,
        req: DropSequenceReq,
        skip_skip_privilege_check: bool,
    ) -> Result<()> {
        let catalog = ctx.get_default_catalog()?;
        // we should do `drop ownership` after actually drop object, and object maybe not exists.
        // drop the ownership
        if skip_skip_privilege_check
            && ctx
                .get_settings()
                .get_enable_experimental_sequence_privilege_check()?
        {
            let tenant = req.ident.tenant();
            let name = req.ident.name().to_string();
            let role_api = UserApiProvider::instance().role_api(tenant);
            let owner_object = OwnershipObject::Sequence { name };
            role_api.revoke_ownership(&owner_object).await?;
            RoleCacheManager::instance().invalidate_cache(tenant);
        }

        let reply = catalog.drop_sequence(req.clone()).await?;

        if !reply.success && !req.if_exists {
            return Err(ErrorCode::UnknownSequence(format!(
                "unknown sequence {:?}",
                req.ident.name()
            )));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Interpreter for DropSequenceInterpreter {
    fn name(&self) -> &str {
        "DropSequenceInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let req = DropSequenceReq {
            ident: self.plan.ident.clone(),
            if_exists: self.plan.if_exists,
        };
        Self::req_execute(self.ctx.as_ref(), req, false).await?;

        Ok(PipelineBuildResult::create())
    }
}
